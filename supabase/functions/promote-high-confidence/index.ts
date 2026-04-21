// promote-high-confidence/index.ts
// Pipeline step 4 — runs after ai-jurisdiction-check. Two passes:
//
//   Pass 1 — Promote high-confidence agrees:
//     governing_body_agreement = 'agree' + confidence = 'high'
//     → review_status = 'ready'
//
//   Pass 2 — Invalid beach detection on unresolved records:
//     governing_body_agreement = 'unresolved'
//     → Claude asked: is this actually a beach?
//     → Non-beaches marked review_status = 'invalid'
//
// Pipeline order:
//   1. geocode-beaches-staging  (Google + Census + NPS federal + CSP state)
//   2. set-governing-body       (keyword enrichment for remaining)
//   3. ai-jurisdiction-check    (AI assessment + agreement scoring)
//   4. promote-high-confidence  (this function)
//
// POST { state?: string, county?: string, dry_run?: boolean }
// Returns { promoted, invalid_beaches, skipped_low_confidence, skipped_disagree }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const CONCURRENCY = 10;

// ── Is-this-a-beach classifier ────────────────────────────────────────────────
// Rule-based: only flags records where the name itself contains an unambiguous
// non-beach indicator. Does not use LLM knowledge of whether the place exists —
// obscure or unfamiliar beach names are not grounds for invalidation.

const NON_BEACH_PATTERNS: { pattern: RegExp; reason: string }[] = [
  { pattern: /\bdeli\b/i,                          reason: "Name contains 'deli' — a food business." },
  { pattern: /\bpsychic\b/i,                       reason: "Name contains 'psychic' — a business type." },
  { pattern: /\bgarage\b/i,                        reason: "Name contains 'garage' — a business type." },
  { pattern: /&\s*bar\b/i,                         reason: "Name contains '& Bar' — a commercial bar." },
  { pattern: /\brestaurant\b/i,                    reason: "Name contains 'restaurant' — a food business." },
  { pattern: /\bcafe\b/i,                          reason: "Name contains 'cafe' — a food business." },
  { pattern: /\bsurf\s+(school|camp|lesson)/i,     reason: "Name contains surf school/camp — a commercial operation." },
  { pattern: /\b(lessons|rentals|tours|charters)\b/i, reason: "Name contains commercial service term." },
  { pattern: /\bhoa\b/i,                           reason: "Name explicitly contains 'HOA' — private homeowner association." },
  { pattern: /\b(lane|drive|avenue|blvd|street|road|court)\s*$/i, reason: "Name ends with a street suffix — likely an address." },
];

interface BeachCheck { is_beach: boolean; confidence: "high" | "low"; reason: string; }

function isActualBeach(displayName: string): BeachCheck {
  for (const { pattern, reason } of NON_BEACH_PATTERNS) {
    if (pattern.test(displayName)) {
      return { is_beach: false, confidence: "high", reason };
    }
  }
  return { is_beach: true, confidence: "high", reason: "No non-beach indicators in name." };
}

// ── Concurrency limiter ───────────────────────────────────────────────────────

async function pLimit<T>(tasks: (() => Promise<T>)[], limit: number): Promise<T[]> {
  const results: T[] = new Array(tasks.length);
  let index = 0;
  async function worker() {
    while (index < tasks.length) {
      const i = index++;
      results[i] = await tasks[i]();
    }
  }
  await Promise.all(Array.from({ length: limit }, worker));
  return results;
}

// ── Handler ───────────────────────────────────────────────────────────────────

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { state?: string; county?: string; dry_run?: boolean } = {};
  try { body = await req.json(); } catch { /* empty body */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // ── Fetch all unlocked candidates ──────────────────────────────────────────
  let fetchQuery = supabase
    .from("beaches_staging_new")
    .select("id, display_name, governing_body, governing_body_agreement, governing_jurisdiction_ai_confidence")
    .is("review_status", null)
    .not("governing_body_agreement", "is", null);

  if (body.state)  fetchQuery = fetchQuery.eq("state", body.state);
  if (body.county) fetchQuery = fetchQuery.eq("county", body.county);

  const { data: rows, error: fetchError } = await fetchQuery;
  if (fetchError) return json({ error: fetchError.message }, 500);

  const allRows        = rows ?? [];
  const highAgreeRows  = allRows.filter(r => r.governing_body_agreement === "agree" && r.governing_jurisdiction_ai_confidence === "high");
  const lowAgreeRows   = allRows.filter(r => r.governing_body_agreement === "agree" && r.governing_jurisdiction_ai_confidence === "low");
  const disagreeRows   = allRows.filter(r => r.governing_body_agreement === "disagree");
  const unresolvedRows = allRows.filter(r => r.governing_body_agreement === "unresolved");

  if (body.dry_run) {
    return json({
      dry_run:                true,
      would_promote:          highAgreeRows.length,
      unresolved_to_check:    unresolvedRows.length,
      skipped_low_confidence: lowAgreeRows.length,
      skipped_disagree:       disagreeRows.length,
    });
  }

  // ── Pass 1: promote high-confidence agrees ─────────────────────────────────
  let promoted = 0;
  if (highAgreeRows.length > 0) {
    let updateQuery = supabase
      .from("beaches_staging_new")
      .update({
        review_status: "ready",
        review_notes:  "High confidence AI and pipeline agreement on governing jurisdiction.",
      })
      .eq("governing_body_agreement", "agree")
      .eq("governing_jurisdiction_ai_confidence", "high")
      .is("review_status", null);

    if (body.state)  updateQuery = updateQuery.eq("state", body.state);
    if (body.county) updateQuery = updateQuery.eq("county", body.county);

    const { error } = await updateQuery;
    if (error) return json({ error: error.message }, 500);
    promoted = highAgreeRows.length;
  }

  // ── Pass 2: invalid beach detection on unresolved ──────────────────────────
  const writeErrors: string[] = [];
  let invalidCount = 0;

  if (unresolvedRows.length > 0) {
    const results = unresolvedRows.map(row => ({
      id: row.id,
      display_name: row.display_name,
      ...isActualBeach(row.display_name),
    }));

    const writeTasks = results
      .filter(r => !r.is_beach && r.confidence === "high")
      .map(r => async () => {
        const { error } = await supabase
          .from("beaches_staging_new")
          .update({
            review_status: "invalid",
            review_notes:  `Not a beach: ${r.reason}`,
          })
          .eq("id", r.id);
        if (error) writeErrors.push(`id ${r.id}: ${error.message}`);
        else invalidCount++;
      });

    await pLimit(writeTasks, 10);
  }

  return json({
    promoted,
    invalid_beaches:        invalidCount,
    skipped_low_confidence: lowAgreeRows.length,
    skipped_disagree:       disagreeRows.length,
    skipped_unresolved:     unresolvedRows.length - invalidCount,
    errors:                 writeErrors,
  });
});
