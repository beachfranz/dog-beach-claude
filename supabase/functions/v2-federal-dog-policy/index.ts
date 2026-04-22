// v2-federal-dog-policy/index.ts
// Per-federal-unit dog policy research. Same pattern as city/state.
//
// Covers records with governing_body_source in:
//   federal_polygon, cpad_federal, blm_sma_federal
//
// Federal units vary widely:
//   - NPS parks and seashores: typically leashed only in developed areas,
//     varies per-beach (Point Reyes has ~4 dog-friendly beaches, GGNRA has
//     Crissy Field + Fort Funston off-leash areas, etc.). Expect mostly "mixed".
//   - Military bases (Camp Pendleton, NBVC Point Mugu, Vandenberg, etc.):
//     public access often restricted; may report "unknown" or "no".
//   - National Forests (Los Padres, Sierra, Plumas): generally leash-only,
//     more permissive than NPS.
//   - BLM: varies by unit.
//   - National Wildlife Refuges: typically dogs prohibited to protect wildlife.

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

interface DogPolicy {
  dogs_allowed:             "yes" | "no" | "mixed" | "seasonal" | "unknown";
  dogs_leash_required:      boolean | null;
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
        api_key:      TAVILY_API_KEY,
        query,
        search_depth: "basic",
        max_results:  5,
      }),
    });
    if (!resp.ok) return [];
    const data = await resp.json();
    return (data?.results ?? []) as TavilyResult[];
  } catch { return []; }
}

async function extractPolicy(unitName: string, sources: TavilyResult[]): Promise<DogPolicy> {
  const block = sources
    .map((s, i) => `[${i + 1}] ${s.url}\n${s.title}\n${s.content}`)
    .join("\n\n");

  const prompt = `You are extracting the dog policy for a federal land unit in California that contains beaches.

Unit: ${unitName}

Here are web search results about dog rules at this unit:

${block}

Extract the dog policy as a JSON object with these fields:

- dogs_allowed: one of:
    "yes"      — dogs are allowed on the beaches (the sand / shore) generally
    "no"       — dogs are prohibited from all beaches in this unit
    "mixed"    — some beaches allow dogs, others don't (most common for NPS units)
    "seasonal" — dog access restricted by time of year (e.g. snowy plover nesting)
    "unknown"  — cannot determine from sources; also use for military bases where public beach access is restricted

  Important context:
  - NPS parks/seashores typically allow dogs only on leash in developed areas; beach access varies per beach (Point Reyes has a handful of dog-friendly beaches; GGNRA has off-leash areas like Fort Funston and Crissy Field)
  - National Forests (USFS) are generally more permissive, leash required
  - National Wildlife Refuges typically prohibit dogs to protect wildlife
  - Military bases (Camp Pendleton, NBVC, Vandenberg, Pillar Point Space Force) typically have restricted public access — if so, mark "unknown"

- dogs_allowed_areas: short description of specific beaches/areas where dogs ARE permitted (e.g. "Kehoe Beach, Limantour Beach, North Beach, South Beach within 50 yards of parking"). null if dogs allowed unit-wide or dogs_allowed = "no".
- dogs_prohibited_areas: short description of beaches/areas where dogs are NOT permitted. null if dogs_allowed = "no" (everywhere) or allowed unit-wide.
- dogs_leash_required: true, false, or null if unknown
- dogs_off_leash_area: designated off-leash area (e.g. "Fort Funston", "Crissy Field east of Crissy Field Center"); otherwise null
- dogs_time_restrictions: description if hours limit access; otherwise null
- dogs_season_restrictions: description if seasonal limits apply (e.g. "snowy plover nesting March-September"); otherwise null
- dogs_policy_notes: one or two sentences summarizing the policy for a user deciding whether to visit.
- dogs_policy_source_url: the single most authoritative URL (prefer nps.gov, blm.gov, fs.usda.gov, or the agency's official page)
- confidence: "high" if sources directly state the unit's rules; "low" if inferring or unclear

If the sources are unclear or contradictory, mark confidence as "low" and dogs_allowed as "unknown".

Respond with a JSON object only, no other text.`;

  try {
    const response = await anthropic.messages.create({
      model: MODEL, max_tokens: 500,
      messages: [{ role: "user", content: prompt }],
    });
    const text  = (response.content[0] as { type: string; text: string }).text.trim();
    const match = text.match(/\{[\s\S]*\}/);
    if (!match) return defaultPolicy();
    const parsed = JSON.parse(match[0]);

    const validAllowed = ["yes","no","mixed","seasonal","unknown"];
    const validConf    = ["high","low"];
    return {
      dogs_allowed:             validAllowed.includes(parsed.dogs_allowed) ? parsed.dogs_allowed : "unknown",
      dogs_leash_required:      typeof parsed.dogs_leash_required === "boolean" ? parsed.dogs_leash_required : null,
      dogs_allowed_areas:       parsed.dogs_allowed_areas || null,
      dogs_prohibited_areas:    parsed.dogs_prohibited_areas || null,
      dogs_off_leash_area:      parsed.dogs_off_leash_area || null,
      dogs_time_restrictions:   parsed.dogs_time_restrictions || null,
      dogs_season_restrictions: parsed.dogs_season_restrictions || null,
      dogs_policy_notes:        String(parsed.dogs_policy_notes || "").trim() || "No policy notes available.",
      dogs_policy_source_url:   parsed.dogs_policy_source_url || null,
      confidence:               validConf.includes(parsed.confidence) ? parsed.confidence : "low",
    };
  } catch { return defaultPolicy(); }
}

function defaultPolicy(): DogPolicy {
  return {
    dogs_allowed: "unknown", dogs_leash_required: null,
    dogs_allowed_areas: null, dogs_prohibited_areas: null, dogs_off_leash_area: null,
    dogs_time_restrictions: null, dogs_season_restrictions: null,
    dogs_policy_notes: "Research failed; policy unknown.",
    dogs_policy_source_url: null, confidence: "low",
  };
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

  let body: { dry_run?: boolean; unit_filter?: string; limit?: number } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("governing_body")
    .in("governing_body_source", ["federal_polygon","cpad_federal","blm_sma_federal"])
    .eq("review_status", "ready")
    .not("governing_body", "is", null);
  if (error) return json({ error: error.message }, 500);

  const unitSet = new Set<string>();
  for (const r of rows ?? []) unitSet.add(r.governing_body);
  let units = [...unitSet];
  if (body.unit_filter) units = units.filter(u => u.toLowerCase().includes(body.unit_filter!.toLowerCase()));
  if (body.limit)       units = units.slice(0, body.limit);

  if (units.length === 0) return json({ units: 0 });

  const tasks = units.map(unit => async () => {
    const query   = `${unit} dogs on beach California`;
    const sources = await tavilySearch(query);
    const policy  = sources.length === 0 ? defaultPolicy() : await extractPolicy(unit, sources);
    return { unit, sources_count: sources.length, policy };
  });
  const researched = await pLimit(tasks, CONCURRENCY);

  if (body.dry_run) {
    return json({ dry_run: true, units: units.length, preview: researched.slice(0, 10) });
  }

  const now = new Date().toISOString();
  let units_updated = 0;
  const writeErrors: string[] = [];

  for (const r of researched) {
    const { error: uErr } = await supabase
      .from("beaches_staging_new")
      .update({
        dogs_allowed:             r.policy.dogs_allowed,
        dogs_leash_required:      r.policy.dogs_leash_required,
        dogs_allowed_areas:       r.policy.dogs_allowed_areas,
        dogs_prohibited_areas:    r.policy.dogs_prohibited_areas,
        dogs_off_leash_area:      r.policy.dogs_off_leash_area,
        dogs_time_restrictions:   r.policy.dogs_time_restrictions,
        dogs_season_restrictions: r.policy.dogs_season_restrictions,
        dogs_policy_source:       "federal_research",
        dogs_policy_source_url:   r.policy.dogs_policy_source_url,
        dogs_policy_notes:        r.policy.dogs_policy_notes,
        dogs_policy_updated_at:   now,
      })
      .eq("governing_body", r.unit)
      .in("governing_body_source", ["federal_polygon","cpad_federal","blm_sma_federal"]);
    if (uErr) writeErrors.push(`unit "${r.unit}": ${uErr.message}`);
    else      units_updated += 1;
  }

  return json({
    units:         researched.length,
    units_updated,
    summary: {
      yes:       researched.filter(r => r.policy.dogs_allowed === "yes").length,
      no:        researched.filter(r => r.policy.dogs_allowed === "no").length,
      mixed:     researched.filter(r => r.policy.dogs_allowed === "mixed").length,
      seasonal:  researched.filter(r => r.policy.dogs_allowed === "seasonal").length,
      unknown:   researched.filter(r => r.policy.dogs_allowed === "unknown").length,
      high_conf: researched.filter(r => r.policy.confidence === "high").length,
      low_conf:  researched.filter(r => r.policy.confidence === "low").length,
    },
    errors: writeErrors,
  });
});
