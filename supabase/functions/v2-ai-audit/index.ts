// v2-ai-audit/index.ts
// Pipeline stage 10 — AI audit over all final classifications.
//
// Runs Claude against every classified record with full location context
// (city, county, census place, governing body) and asks: given this
// information, does the pipeline's classification make sense?
//
// Writes:
//   governing_jurisdiction_ai            — Claude's independent assessment
//   governing_jurisdiction_ai_confidence — high/low/unknown
//   governing_body_agreement             — agree/disagree/unresolved
//
// Does NOT modify governing_jurisdiction. This is purely audit — any
// downstream consumer may use governing_body_agreement to route records
// into a human review queue.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import Anthropic from "https://esm.sh/@anthropic-ai/sdk@0.39.0";
import { corsHeaders } from "../_shared/cors.ts";
import { stateCodeFromName } from "../_shared/config.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ANTHROPIC_API_KEY    = Deno.env.get("anthropic_api_key")!;

const MODEL         = "claude-haiku-4-5-20251001";
const CONCURRENCY   = 5;
const DEFAULT_LIMIT = 2000;

const client = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

interface AIResult { jurisdiction: string; confidence: string; }

async function assess(
  displayName: string,
  governingBody: string,
  state: string,
  context: { city?: string | null; county?: string | null; censusPlace?: string | null },
): Promise<AIResult> {
  const contextLines: string[] = [];
  if (context.city)        contextLines.push(`City (from geocoding): ${context.city}`);
  if (context.censusPlace && context.censusPlace !== "UNINCORPORATED")
    contextLines.push(`Census incorporated place: ${context.censusPlace}`);
  if (context.county)      contextLines.push(`County: ${context.county}`);
  const contextBlock = contextLines.length
    ? `\n\nLocation context:\n${contextLines.join("\n")}`
    : "";

  const prompt = `Who manages or governs "${displayName}" beach located near ${governingBody}, ${state}?${contextBlock}

Classify the managing/governing authority as exactly one of:
- governing federal  (US federal agency — NPS, military, USFS, BLM, FWS, etc.)
- governing state    (state parks, state beaches, state reserves)
- governing county   (county parks, regional parks)
- governing city     (city or municipality)

Respond with a JSON object only:
{"jurisdiction": "<one of the four values>", "confidence": "<high if confident, low if uncertain, unknown if no reliable info>"}`;

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
    const jurisdiction = ["governing federal", "governing state", "governing county", "governing city"]
      .includes(parsed.jurisdiction) ? parsed.jurisdiction : "unknown";
    const confidence = ["high", "low", "unknown"].includes(parsed.confidence)
      ? parsed.confidence : "unknown";
    return { jurisdiction, confidence };
  } catch {
    return { jurisdiction: "unknown", confidence: "unknown" };
  }
}

async function pLimit<T>(tasks: (() => Promise<T>)[], limit: number): Promise<T[]> {
  const results: T[] = new Array(tasks.length);
  let i = 0;
  async function worker() { while (i < tasks.length) { const n = i++; results[n] = await tasks[n](); } }
  await Promise.all(Array.from({ length: limit }, worker));
  return results;
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { state_code?: string; limit?: number; county?: string } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const stateCode = body.state_code ?? "CA";
  const supabase  = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  let query = supabase
    .from("beaches_staging_new")
    .select("id, display_name, governing_body, governing_jurisdiction, state, city, county, census_incorporated_place")
    .eq("review_status", "ready")
    .is("governing_jurisdiction_ai", null)
    .not("governing_body", "is", null)
    .limit(body.limit ?? DEFAULT_LIMIT);
  if (body.county) query = query.eq("county", body.county);

  const { data: allRows, error } = await query;
  const rows = (allRows ?? []).filter(r => stateCodeFromName(r.state) === stateCode);
  if (error) return json({ error: error.message }, 500);
  if (!rows?.length) return json({ processed: 0, agree: 0, disagree: 0, unresolved: 0 });

  const tasks = rows.map(r => async () => {
    const ai = await assess(
      r.display_name,
      r.governing_body,
      r.state ?? "California",
      { city: r.city, county: r.county, censusPlace: r.census_incorporated_place },
    );
    let agreement: string;
    if (ai.confidence === "unknown" || ai.jurisdiction === "unknown") agreement = "unresolved";
    else if (ai.jurisdiction === r.governing_jurisdiction)            agreement = "agree";
    else                                                               agreement = "disagree";
    return {
      id: r.id,
      governing_jurisdiction_ai:       ai.jurisdiction,
      governing_jurisdiction_ai_confidence: ai.confidence,
      governing_body_agreement:        agreement,
    };
  });
  const results = await pLimit(tasks, CONCURRENCY);

  const writeErrors: string[] = [];
  const writeTasks = results.map(({ id, ...fields }) => async () => {
    const { error } = await supabase.from("beaches_staging_new").update(fields).eq("id", id);
    if (error) writeErrors.push(`id ${id}: ${error.message}`);
  });
  await pLimit(writeTasks, 10);

  const agree      = results.filter(r => r.governing_body_agreement === "agree").length;
  const disagree   = results.filter(r => r.governing_body_agreement === "disagree").length;
  const unresolved = results.filter(r => r.governing_body_agreement === "unresolved").length;

  return json({
    processed: rows.length,
    agree,
    disagree,
    unresolved,
    errors: writeErrors,
  });
});
