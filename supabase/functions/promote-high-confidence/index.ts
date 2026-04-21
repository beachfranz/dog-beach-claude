// promote-high-confidence/index.ts
// Pipeline step 4 — runs after ai-jurisdiction-check.
// Locks beaches where the AI and pipeline strongly agree:
//   governing_body_agreement = 'agree'
//   governing_jurisdiction_ai_confidence = 'high'
//   review_status IS NULL
//
// Sets review_status = 'ready' so subsequent pipeline steps skip them.
//
// Pipeline order:
//   1. geocode-beaches-staging  (Google + Census + NPS federal + CSP state)
//   2. set-governing-body       (keyword enrichment for remaining)
//   3. ai-jurisdiction-check    (AI assessment + agreement scoring)
//   4. promote-high-confidence  (this function — lock high-confidence agrees)
//
// POST { state?: string, county?: string, dry_run?: boolean }
// Returns { promoted, skipped_low_confidence, skipped_disagree, skipped_unresolved }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { state?: string; county?: string; dry_run?: boolean } = {};
  try { body = await req.json(); } catch { /* empty body */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // ── Count candidates by agreement/confidence breakdown ─────────────────────
  let statsQuery = supabase
    .from("beaches_staging_new")
    .select("governing_body_agreement, governing_jurisdiction_ai_confidence")
    .is("review_status", null)
    .not("governing_body_agreement", "is", null);

  if (body.state)  statsQuery = statsQuery.eq("state", body.state);
  if (body.county) statsQuery = statsQuery.eq("county", body.county);

  const { data: statsRows, error: statsError } = await statsQuery;
  if (statsError) return json({ error: statsError.message }, 500);

  const rows = statsRows ?? [];
  const highAgree      = rows.filter(r => r.governing_body_agreement === "agree" && r.governing_jurisdiction_ai_confidence === "high").length;
  const lowAgree       = rows.filter(r => r.governing_body_agreement === "agree" && r.governing_jurisdiction_ai_confidence === "low").length;
  const disagree       = rows.filter(r => r.governing_body_agreement === "disagree").length;
  const unresolved     = rows.filter(r => r.governing_body_agreement === "unresolved").length;

  if (body.dry_run) {
    return json({
      dry_run:                true,
      would_promote:          highAgree,
      skipped_low_confidence: lowAgree,
      skipped_disagree:       disagree,
      skipped_unresolved:     unresolved,
    });
  }

  // ── Promote high-confidence agrees ─────────────────────────────────────────
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

  const { error: updateError } = await updateQuery;
  if (updateError) return json({ error: updateError.message }, 500);

  return json({
    promoted:               highAgree,
    skipped_low_confidence: lowAgree,
    skipped_disagree:       disagree,
    skipped_unresolved:     unresolved,
  });
});
