// v2-state-parks-dog-policy/index.ts
//
// Phase 7 of POLICY_RESEARCH_MIGRATION (2026-04-25 rewrite).
// Reads from locations_stage (filtered to state-governed beaches whose
// canonical governance came from a state-source: cpad, csp_parks, park_url).
// Writes extracted dog policy to policy_research_extractions with
// origin='v2_dog_policy_v2'. populate_from_research consumes those rows
// and emits beach_enrichment_provenance evidence with source='research'.
//
// POST { dry_run?: boolean, park_filter?: string, limit?: number }
//
// Pre-rewrite version read/wrote beaches_staging_new — see git history.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import Anthropic from "https://esm.sh/@anthropic-ai/sdk@0.39.0";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ANTHROPIC_API_KEY    = Deno.env.get("anthropic_api_key")!;
const TAVILY_API_KEY       = Deno.env.get("TAVILY_API_KEY")!;

const MODEL       = "claude-haiku-4-5-20251001";
const CONCURRENCY = 3;

const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

// Source set this function targets — beaches whose canonical governance
// was assigned by one of these sources (state-park-relevant)
const STATE_SOURCE_SET = ["cpad", "csp_parks", "park_url", "park_url_buffer_attribution"];

// LLM output enum (free-form so prompt stays semantically clear).
// Mapped to the NEW pipeline's CHECK enum at write time.
interface DogPolicy {
  dogs_allowed:             "yes" | "no" | "mixed" | "seasonal" | "unknown";
  dogs_leash_required:      "yes" | "no" | "mixed" | null;
  dogs_allowed_areas:       string | null;
  dogs_prohibited_areas:    string | null;
  dogs_off_leash_area:      string | null;
  dogs_time_restrictions:   string | null;
  dogs_season_restrictions: string | null;
  dogs_policy_notes:        string;
  dogs_policy_source_url:   string | null;
  confidence:               "high" | "low";
}

interface TavilyResult { url: string; title: string; content: string; score: number; }

async function tavilySearch(query: string): Promise<TavilyResult[]> {
  try {
    const resp = await fetch("https://api.tavily.com/search", {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        api_key:       TAVILY_API_KEY,
        query,
        search_depth:  "basic",
        max_results:   5,
      }),
    });
    if (!resp.ok) return [];
    const data = await resp.json();
    return (data?.results ?? []) as TavilyResult[];
  } catch {
    return [];
  }
}

async function extractPolicy(parkName: string, sources: TavilyResult[]): Promise<DogPolicy> {
  const block = sources
    .map((s, i) => `[${i + 1}] ${s.url}\n${s.title}\n${s.content}`)
    .join("\n\n");

  const prompt = `You are extracting the dog policy for a California state park or beach.

Park: ${parkName}

Here are web search results about dog rules at this park:

${block}

Extract the dog policy as a JSON object with these fields:

- dogs_allowed: one of:
    "yes"      — dogs are allowed on the beach itself (the sand / shore)
    "no"       — dogs are not allowed on the beach itself
    "mixed"    — dogs are allowed on some beaches or in some areas of the park but not others
    "seasonal" — dog access is restricted by time of year
    "unknown"  — cannot determine from sources

  Important: for the headline answer, we care about access to the BEACH (sand / shore), not just the campground or parking lot.

- dogs_allowed_areas: short description of where dogs ARE permitted; null if dogs allowed park-wide or dogs_allowed = "no".
- dogs_prohibited_areas: short description of where dogs are NOT permitted; null if whole park prohibited or allowed park-wide.
- dogs_leash_required: "yes", "no", "mixed", or null
- dogs_off_leash_area: description of designated off-leash zones; otherwise null
- dogs_time_restrictions: description if hours limit dog access; otherwise null
- dogs_season_restrictions: description if seasonal limits apply; otherwise null
- dogs_policy_notes: one or two sentences summarizing the policy
- dogs_policy_source_url: the single most authoritative URL from the sources above
- confidence: "high" if sources directly state the rules; "low" if inferring or unclear

Respond with a JSON object only, no other text.`;

  try {
    const response = await anthropic.messages.create({
      model:      MODEL,
      max_tokens: 500,
      messages:   [{ role: "user", content: prompt }],
    });
    const text  = (response.content[0] as { type: string; text: string }).text.trim();
    const match = text.match(/\{[\s\S]*\}/);
    if (!match) return defaultPolicy();
    const parsed = JSON.parse(match[0]);

    const validAllowed = ["yes", "no", "mixed", "seasonal", "unknown"];
    const validConf    = ["high", "low"];
    return {
      dogs_allowed:             validAllowed.includes(parsed.dogs_allowed) ? parsed.dogs_allowed : "unknown",
      dogs_leash_required:      ["yes","no","mixed"].includes(parsed.dogs_leash_required) ? parsed.dogs_leash_required : null,
      dogs_allowed_areas:       parsed.dogs_allowed_areas || null,
      dogs_prohibited_areas:    parsed.dogs_prohibited_areas || null,
      dogs_off_leash_area:      parsed.dogs_off_leash_area || null,
      dogs_time_restrictions:   parsed.dogs_time_restrictions || null,
      dogs_season_restrictions: parsed.dogs_season_restrictions || null,
      dogs_policy_notes:        String(parsed.dogs_policy_notes || "").trim() || "No policy notes available.",
      dogs_policy_source_url:   parsed.dogs_policy_source_url || null,
      confidence:               validConf.includes(parsed.confidence) ? parsed.confidence : "low",
    };
  } catch {
    return defaultPolicy();
  }
}

function defaultPolicy(): DogPolicy {
  return {
    dogs_allowed:             "unknown",
    dogs_leash_required:      null,
    dogs_allowed_areas:       null,
    dogs_prohibited_areas:    null,
    dogs_off_leash_area:      null,
    dogs_time_restrictions:   null,
    dogs_season_restrictions: null,
    dogs_policy_notes:        "Research failed; policy unknown.",
    dogs_policy_source_url:   null,
    confidence:               "low",
  };
}

// ── Mappers from LLM enum → NEW pipeline CHECK enum ──────────────────────

// LLM 'mixed' becomes 'restricted' (NEW enum doesn't have 'mixed' for dogs_allowed)
function mapDogsAllowed(v: string): string | null {
  switch (v) {
    case "yes":      return "yes";
    case "no":       return "no";
    case "seasonal": return "seasonal";
    case "mixed":    return "restricted";
    case "unknown":  return "unknown";
    default:         return null;
  }
}

function mapLeashRequired(v: string | null): string | null {
  switch (v) {
    case "yes":   return "required";
    case "no":    return "off_leash_ok";
    case "mixed": return "mixed";
    default:      return null;
  }
}

// Consolidate freeform area descriptions into a single zone_description
function buildZoneDescription(p: DogPolicy): string | null {
  const parts = [
    p.dogs_allowed_areas    ? `Dogs allowed: ${p.dogs_allowed_areas}` : null,
    p.dogs_prohibited_areas ? `Dogs prohibited: ${p.dogs_prohibited_areas}` : null,
    p.dogs_off_leash_area   ? `Off-leash area: ${p.dogs_off_leash_area}` : null,
  ].filter((s): s is string => s !== null);
  return parts.length === 0 ? null : parts.join(" | ");
}

// Stuff temporal restrictions into policy_notes — structured parsing happens
// downstream via v2-parse-temporal-restrictions (when that's rewritten too).
function buildPolicyNotes(p: DogPolicy): string {
  const parts = [p.dogs_policy_notes];
  if (p.dogs_time_restrictions)   parts.push(`Time restrictions: ${p.dogs_time_restrictions}`);
  if (p.dogs_season_restrictions) parts.push(`Seasonal: ${p.dogs_season_restrictions}`);
  return parts.join(" ");
}

function confidenceToNumeric(c: "high" | "low"): number {
  return c === "high" ? 0.85 : 0.55;
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

  let body: { dry_run?: boolean; park_filter?: string; limit?: number } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // Step 1: fids whose canonical governance is from a state source
  const { data: govRows, error: govErr } = await supabase
    .from("beach_enrichment_provenance")
    .select("fid")
    .eq("field_group", "governance")
    .eq("is_canonical", true)
    .in("source", STATE_SOURCE_SET);
  if (govErr) return json({ error: `gov filter: ${govErr.message}` }, 500);
  const stateGovernedFids = (govRows ?? []).map(r => r.fid);

  // Step 2: closest STATE-level CPAD unit_name per beach.
  // Filter to mng_ag_lev='State' so we don't accidentally pick a city/county
  // park that happens to be the closest polygon — the canonical governance
  // is state-level so the unit we research must also be state-level.
  // Order by candidate_rank ASC; first row per fid wins (lowest rank = closest).
  const { data: candRows, error: candErr } = await supabase
    .from("beach_cpad_candidates")
    .select("fid, unit_name, candidate_rank")
    .in("fid", stateGovernedFids)
    .eq("mng_ag_lev", "State")
    .order("candidate_rank", { ascending: true });
  if (candErr) return json({ error: `cpad cand: ${candErr.message}` }, 500);
  const fidToUnit = new Map<number, string>();
  for (const c of candRows ?? []) {
    if (!fidToUnit.has(c.fid as number)) {
      fidToUnit.set(c.fid as number, c.unit_name as string);
    }
  }

  // Step 3: pull beaches with state governance from locations_stage
  const { data: beaches, error } = await supabase
    .from("locations_stage")
    .select("fid, display_name, governing_body_name, governing_body_type, state_code")
    .eq("governing_body_type", "state")
    .in("fid", stateGovernedFids)
    .not("governing_body_name", "is", null)
    .eq("is_active", true);
  if (error) return json({ error: error.message }, 500);

  // Attach unit_name to each beach. Drop beaches without a CPAD candidate
  // (no unit to research against). Group by unit_name so we research each
  // park once and apply policy to all beaches in it.
  const entityToBeaches = new Map<string, typeof beaches>();
  for (const b of beaches ?? []) {
    const unit = fidToUnit.get(b.fid as number);
    if (!unit) continue;  // no specific park unit known; skip
    if (!entityToBeaches.has(unit)) entityToBeaches.set(unit, []);
    entityToBeaches.get(unit)!.push(b);
  }
  let entities = [...entityToBeaches.keys()];
  if (body.park_filter) entities = entities.filter(p => p.toLowerCase().includes(body.park_filter!.toLowerCase()));
  if (body.limit)       entities = entities.slice(0, body.limit);

  if (entities.length === 0) return json({ entities: 0, beaches: 0 });

  // Research each entity once
  const tasks = entities.map(entity => async () => {
    const query   = `${entity} dogs on beach California`;
    const sources = await tavilySearch(query);
    const policy  = sources.length === 0 ? defaultPolicy() : await extractPolicy(entity, sources);
    return { entity, sources, policy };
  });
  const researched = await pLimit(tasks, CONCURRENCY);

  if (body.dry_run) {
    return json({
      dry_run: true,
      entities: entities.length,
      preview:  researched.slice(0, 10),
    });
  }

  // Write to policy_research_extractions — one row per (fid, primary_source_url)
  const now = new Date().toISOString();
  let rows_written = 0;
  const writeErrors: string[] = [];

  for (const r of researched) {
    const beachesForEntity = entityToBeaches.get(r.entity) ?? [];
    const sourceUrls = r.sources.map(s => s.url);
    const primaryUrl = r.policy.dogs_policy_source_url ?? sourceUrls[0] ?? null;

    for (const b of beachesForEntity) {
      const status =
        r.sources.length === 0      ? "no_sources" :
        r.policy.confidence === "high" ? "success" :
                                      "low_confidence";

      const { error: uErr } = await supabase
        .from("policy_research_extractions")
        .upsert({
          fid:                   b.fid,
          extracted_at:          now,
          extraction_status:     status,
          origin:                "v2_dog_policy_v2",
          research_query:        `${r.entity} dogs on beach California`,
          source_urls:           sourceUrls,
          primary_source_url:    primaryUrl,
          source_count:          r.sources.length,
          extraction_model:      MODEL,
          extraction_confidence: confidenceToNumeric(r.policy.confidence),
          dogs_allowed:          mapDogsAllowed(r.policy.dogs_allowed),
          dogs_leash_required:   mapLeashRequired(r.policy.dogs_leash_required),
          dogs_zone_description: buildZoneDescription(r.policy),
          dogs_policy_notes:     buildPolicyNotes(r.policy),
          // dogs_restricted_hours / dogs_seasonal_rules left null —
          // freeform text gets stuffed into dogs_policy_notes; downstream
          // v2-parse-temporal-restrictions (when rewritten) parses these
          // into structured jsonb.
        }, { onConflict: "fid,primary_source_url,origin" });
      if (uErr) writeErrors.push(`fid=${b.fid} entity="${r.entity}": ${uErr.message}`);
      else      rows_written += 1;
    }
  }

  return json({
    entities:        researched.length,
    beaches:         beaches?.length ?? 0,
    rows_written,
    summary: {
      yes:        researched.filter(r => r.policy.dogs_allowed === "yes").length,
      no:         researched.filter(r => r.policy.dogs_allowed === "no").length,
      mixed:      researched.filter(r => r.policy.dogs_allowed === "mixed").length,
      seasonal:   researched.filter(r => r.policy.dogs_allowed === "seasonal").length,
      unknown:    researched.filter(r => r.policy.dogs_allowed === "unknown").length,
      high_conf:  researched.filter(r => r.policy.confidence === "high").length,
      low_conf:   researched.filter(r => r.policy.confidence === "low").length,
    },
    errors: writeErrors,
  });
});
