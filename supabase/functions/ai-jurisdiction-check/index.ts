// ai-jurisdiction-check/index.ts
// Uses Claude to independently assess the governing jurisdiction of each beach,
// then compares against the existing governing_jurisdiction to flag agreements
// and disagreements for human review.
//
// Writes:
//   governing_jurisdiction_ai          — Claude's assessment
//   governing_jurisdiction_ai_confidence — high | low | unknown
//   governing_body_agreement           — agree | disagree | unresolved
//
// Only processes rows where governing_jurisdiction_ai IS NULL.
//
// POST { state?: string, county?: string, limit?: number }
// Returns { processed, agree, disagree, unresolved, errors }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import Anthropic from "https://esm.sh/@anthropic-ai/sdk@0.39.0";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ANTHROPIC_API_KEY    = Deno.env.get("anthropic_api_key")!;

const MODEL         = "claude-haiku-4-5-20251001";
const CONCURRENCY   = 5;
const DEFAULT_LIMIT = 2000;

const client = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

// ── Claude call ───────────────────────────────────────────────────────────────

interface AIResult {
  jurisdiction: string;    // governing city | governing county | governing state | governing federal
  confidence:   string;    // high | low | unknown
}

async function assessJurisdiction(
  displayName: string,
  governingBody: string,
  state: string,
  context?: { city?: string | null; county?: string | null; censusPlace?: string | null },
): Promise<AIResult> {
  const contextLines: string[] = [];
  if (context?.city)        contextLines.push(`City (from geocoding): ${context.city}`);
  if (context?.censusPlace && context.censusPlace !== "UNINCORPORATED") {
    contextLines.push(`Census incorporated place: ${context.censusPlace}`);
  }
  if (context?.county)      contextLines.push(`County: ${context.county}`);
  const contextBlock = contextLines.length
    ? `\n\nLocation context:\n${contextLines.join("\n")}`
    : "";

  const prompt = `Who manages or governs "${displayName}" beach located near ${governingBody}, ${state}?${contextBlock}

Classify the managing/governing authority as exactly one of:
- governing federal  (managed by a US federal agency — National Park Service, US military, Army Corps of Engineers, US Fish & Wildlife, etc.)
- governing state    (managed by the state — state parks, state beaches, state reserves, etc.)
- governing county   (managed by the county — county parks, regional parks, etc.)
- governing city     (managed by a city or municipality)

Respond with a JSON object only, no other text:
{
  "jurisdiction": "<one of the four values above>",
  "confidence": "<high if you are confident, low if uncertain, unknown if you have no reliable information>"
}`;

  try {
    const response = await client.messages.create({
      model:      MODEL,
      max_tokens: 100,
      messages:   [{ role: "user", content: prompt }],
    });

    const text = (response.content[0] as { type: string; text: string }).text.trim();
    const match = text.match(/\{[\s\S]*\}/);
    if (!match) return { jurisdiction: "unknown", confidence: "unknown" };

    const parsed = JSON.parse(match[0]);
    const jurisdiction = [
      "governing federal", "governing state", "governing county", "governing city",
    ].includes(parsed.jurisdiction) ? parsed.jurisdiction : "unknown";
    const confidence = ["high", "low", "unknown"].includes(parsed.confidence)
      ? parsed.confidence
      : "unknown";

    return { jurisdiction, confidence };
  } catch {
    return { jurisdiction: "unknown", confidence: "unknown" };
  }
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

  let body: { state?: string; county?: string; limit?: number; recheck_unresolved?: boolean } = {};
  try { body = await req.json(); } catch { /* empty body */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  let query = supabase
    .from("beaches_staging_new")
    .select("id, display_name, governing_body, governing_jurisdiction, state, city, county, governing_city, census_incorporated_place")
    .is("review_status", null)
    .not("governing_body", "is", null)
    .limit(body.limit ?? DEFAULT_LIMIT);

  if (body.recheck_unresolved) {
    query = query.eq("governing_body_agreement", "unresolved");
  } else {
    query = query.is("governing_jurisdiction_ai", null);
  }

  if (body.state)  query = query.eq("state", body.state);
  if (body.county) query = query.eq("county", body.county);

  const { data: rows, error: fetchError } = await query;
  if (fetchError) return json({ error: fetchError.message }, 500);
  if (!rows?.length) return json({ processed: 0, agree: 0, disagree: 0, unresolved: 0, errors: [] });

  // ── Assess each beach via Claude ────────────────────────────────────────────

  const tasks = rows.map(row => async () => {
    const ai = await assessJurisdiction(
      row.display_name,
      row.governing_body,
      row.state ?? "California",
      {
        city:         row.city ?? row.governing_city,
        county:       row.county,
        censusPlace:  row.census_incorporated_place,
      },
    );

    let agreement: string;
    if (ai.confidence === "unknown" || ai.jurisdiction === "unknown") {
      agreement = "unresolved";
    } else if (ai.jurisdiction === row.governing_jurisdiction) {
      agreement = "agree";
    } else {
      agreement = "disagree";
    }

    return {
      id:                              row.id,
      governing_jurisdiction_ai:       ai.jurisdiction,
      governing_jurisdiction_ai_confidence: ai.confidence,
      governing_body_agreement:        agreement,
    };
  });

  const results = await pLimit(tasks, CONCURRENCY);

  // ── Write results ───────────────────────────────────────────────────────────

  const writeErrors: string[] = [];
  const writeTasks = results.map(({ id, ...fields }) => async () => {
    const { error } = await supabase
      .from("beaches_staging_new")
      .update(fields)
      .eq("id", id);
    if (error) writeErrors.push(`id ${id}: ${error.message}`);
  });

  await pLimit(writeTasks, 10);

  const agree     = results.filter(r => r.governing_body_agreement === "agree").length;
  const disagree  = results.filter(r => r.governing_body_agreement === "disagree").length;
  const unresolved = results.filter(r => r.governing_body_agreement === "unresolved").length;

  return json({
    processed:  rows.length,
    agree,
    disagree,
    unresolved,
    errors:     writeErrors,
  });
});
