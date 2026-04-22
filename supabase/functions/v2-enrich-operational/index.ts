// v2-enrich-operational/index.ts
// Unified operational-data enrichment for any jurisdiction tier. Replaces
// the four per-tier dog-policy functions with one that also captures
// parking, hours, and amenities in the same research pass.
//
// Takes `tier` parameter: 'state' | 'city' | 'county' | 'federal'. Filters
// beaches by governing_jurisdiction accordingly, groups by governing_body,
// and does one Tavily + Claude call per unique body.
//
// Write semantics:
//   - Dog-policy fields: always overwrite (latest research wins)
//   - Parking / hours / amenities: only fill NULL values (preserves CCC
//     amenity data and any prior structured data)
//   - enrichment_source set to "<tier>_research"; enrichment_updated_at
//     always refreshed.
//
// POST { tier: "state"|"city"|"county"|"federal", dry_run?: boolean,
//        body_filter?: string, limit?: number, force_refresh?: boolean }

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

type Tier = "state" | "city" | "county" | "federal";

interface Enrichment {
  // Dog policy
  dogs_allowed:             "yes" | "no" | "mixed" | "seasonal" | "unknown";
  dogs_leash_required:      boolean | null;
  dogs_allowed_areas:       string | null;
  dogs_prohibited_areas:    string | null;
  dogs_off_leash_area:      string | null;
  dogs_time_restrictions:   string | null;
  dogs_season_restrictions: string | null;
  dogs_policy_notes:        string;
  dogs_policy_source_url:   string | null;
  // Parking
  has_parking:              boolean | null;
  parking_type:             string | null;  // lot / street / paid / free / mixed
  parking_notes:            string | null;
  // Hours
  hours_text:               string | null;
  hours_notes:              string | null;
  // Amenities
  has_restrooms:            boolean | null;
  has_showers:              boolean | null;
  has_lifeguards:           boolean | null;
  has_picnic_area:          boolean | null;
  has_food:                 boolean | null;
  has_drinking_water:       boolean | null;
  has_fire_pits:            boolean | null;
  has_disabled_access:      boolean | null;
  // Meta
  confidence:               "high" | "low";
}

interface TavilyResult { url: string; title: string; content: string; score: number; }

async function tavilySearch(query: string): Promise<TavilyResult[]> {
  try {
    const resp = await fetch("https://api.tavily.com/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ api_key: TAVILY_API_KEY, query, search_depth: "basic", max_results: 6 }),
    });
    if (!resp.ok) return [];
    const data = await resp.json();
    return (data?.results ?? []) as TavilyResult[];
  } catch { return []; }
}

function tierContext(tier: Tier): string {
  switch (tier) {
    case "state":
      return "California state parks — typically leashed-only in developed areas, varies per beach. Some state beaches (e.g. Corona del Mar SB, Santa Monica SB, Leucadia SB) are operationally run by the adjacent city/county. State Coastal Conservancy, UC reserves, and CDFW lands also use this tier.";
    case "city":
      return "Municipal California coastal city — typically has a designated 'dog beach' section plus prohibition at other city beaches. City parks departments publish specific rules.";
    case "county":
      return "California county parks & beaches department — varies widely. LA County Beaches & Harbors operates ~20 beaches with formal rules; inland counties may have lake/reservoir beach rules.";
    case "federal":
      return "Federal land unit — NPS (leashed in developed areas, per-beach rules), USFS (generally more permissive, leash required), BLM (varies), military base (restricted public access → unknown), National Wildlife Refuge (typically prohibited).";
  }
}

async function extractEnrichment(body: string, tier: Tier, sources: TavilyResult[]): Promise<Enrichment> {
  const block = sources.map((s, i) => `[${i + 1}] ${s.url}\n${s.title}\n${s.content}`).join("\n\n");
  const context = tierContext(tier);

  const prompt = `You are extracting operational beach data for a California governing body.

Governing body: ${body}
Jurisdiction tier: ${tier}
Context: ${context}

Web search results:

${block}

Respond with a single FLAT JSON object (no nested sections) with exactly these keys. Use null where sources don't provide clear information.

{
  "dogs_allowed": "yes" | "no" | "mixed" | "seasonal" | "unknown",
  "dogs_leash_required": true | false | null,
  "dogs_allowed_areas": string or null (specific beaches where dogs permitted),
  "dogs_prohibited_areas": string or null (specific beaches where dogs prohibited),
  "dogs_off_leash_area": string or null (designated off-leash zone),
  "dogs_time_restrictions": string or null (e.g. "before 9am and after 5pm"),
  "dogs_season_restrictions": string or null (e.g. "snowy plover March-Sept"),
  "dogs_policy_notes": "one or two sentences for a user",
  "dogs_policy_source_url": "most authoritative URL from the sources",
  "has_parking": true | false | null,
  "parking_type": "lot" | "street" | "paid" | "free" | "mixed" | null,
  "parking_notes": string or null,
  "hours_text": string or null (e.g. "sunrise to 10pm" or "24/7"),
  "hours_notes": string or null,
  "has_restrooms": true | false | null,
  "has_showers": true | false | null,
  "has_lifeguards": true | false | null,
  "has_picnic_area": true | false | null,
  "has_food": true | false | null,
  "has_drinking_water": true | false | null,
  "has_fire_pits": true | false | null,
  "has_disabled_access": true | false | null,
  "confidence": "high" | "low"
}

Semantics:
- dogs_allowed: "yes" means dogs on the sand/shore generally; "no" = prohibited from all beaches under this body; "mixed" = some beaches allow, others don't; "seasonal" = time-of-year restriction.
- confidence: "high" if sources directly state rules for this body; "low" if inferring or unclear.

Prefer null over guessing. Output the JSON object only, no markdown fences, no section headers.`;

  try {
    const response = await anthropic.messages.create({
      model: MODEL, max_tokens: 2000,
      messages: [{ role: "user", content: prompt }],
    });
    const text  = (response.content[0] as { type: string; text: string }).text.trim();
    const match = text.match(/\{[\s\S]*\}/);
    if (!match) {
      console.error("no json match in response:", text.slice(0, 200));
      return defaultEnrichment();
    }
    let p: Record<string, unknown>;
    try {
      p = JSON.parse(match[0]);
    } catch (e) {
      console.error("json parse failed:", e, "raw:", text.slice(0, 500));
      return defaultEnrichment();
    }

    const allow  = ["yes","no","mixed","seasonal","unknown"];
    const conf   = ["high","low"];
    const boolOrNull = (v: unknown) => typeof v === "boolean" ? v : null;
    const strOrNull  = (v: unknown) => v == null || v === "" ? null : String(v);

    return {
      dogs_allowed:             allow.includes(p.dogs_allowed) ? p.dogs_allowed : "unknown",
      dogs_leash_required:      boolOrNull(p.dogs_leash_required),
      dogs_allowed_areas:       strOrNull(p.dogs_allowed_areas),
      dogs_prohibited_areas:    strOrNull(p.dogs_prohibited_areas),
      dogs_off_leash_area:      strOrNull(p.dogs_off_leash_area),
      dogs_time_restrictions:   strOrNull(p.dogs_time_restrictions),
      dogs_season_restrictions: strOrNull(p.dogs_season_restrictions),
      dogs_policy_notes:        strOrNull(p.dogs_policy_notes) ?? "No policy notes available.",
      dogs_policy_source_url:   strOrNull(p.dogs_policy_source_url),
      has_parking:              boolOrNull(p.has_parking),
      parking_type:             strOrNull(p.parking_type),
      parking_notes:            strOrNull(p.parking_notes),
      hours_text:               strOrNull(p.hours_text),
      hours_notes:              strOrNull(p.hours_notes),
      has_restrooms:            boolOrNull(p.has_restrooms),
      has_showers:              boolOrNull(p.has_showers),
      has_lifeguards:           boolOrNull(p.has_lifeguards),
      has_picnic_area:          boolOrNull(p.has_picnic_area),
      has_food:                 boolOrNull(p.has_food),
      has_drinking_water:       boolOrNull(p.has_drinking_water),
      has_fire_pits:            boolOrNull(p.has_fire_pits),
      has_disabled_access:      boolOrNull(p.has_disabled_access),
      confidence:               conf.includes(p.confidence) ? p.confidence : "low",
    };
  } catch {
    return defaultEnrichment();
  }
}

function defaultEnrichment(): Enrichment {
  return {
    dogs_allowed: "unknown", dogs_leash_required: null,
    dogs_allowed_areas: null, dogs_prohibited_areas: null, dogs_off_leash_area: null,
    dogs_time_restrictions: null, dogs_season_restrictions: null,
    dogs_policy_notes: "Research failed; policy unknown.",
    dogs_policy_source_url: null,
    has_parking: null, parking_type: null, parking_notes: null,
    hours_text: null, hours_notes: null,
    has_restrooms: null, has_showers: null, has_lifeguards: null,
    has_picnic_area: null, has_food: null, has_drinking_water: null,
    has_fire_pits: null, has_disabled_access: null,
    confidence: "low",
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
  const json = (b: unknown, s = 200) => new Response(JSON.stringify(b), { status: s, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { tier?: Tier; dry_run?: boolean; body_filter?: string; limit?: number } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const tier = body.tier;
  if (!tier || !["state","city","county","federal"].includes(tier))
    return json({ error: "tier must be one of state|city|county|federal" }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("governing_body")
    .eq("governing_jurisdiction", `governing ${tier}`)
    .eq("review_status", "ready")
    .not("governing_body", "is", null);
  if (error) return json({ error: error.message }, 500);

  const bodySet = new Set<string>();
  for (const r of rows ?? []) bodySet.add(r.governing_body);
  let bodies = [...bodySet];
  if (body.body_filter) bodies = bodies.filter(b => b.toLowerCase().includes(body.body_filter!.toLowerCase()));
  if (body.limit)       bodies = bodies.slice(0, body.limit);

  if (bodies.length === 0) return json({ tier, bodies: 0 });

  const tasks = bodies.map(b => async () => {
    const query   = `${b} California beach dog policy parking hours amenities`;
    const sources = await tavilySearch(query);
    const enrich  = sources.length === 0 ? defaultEnrichment() : await extractEnrichment(b, tier, sources);
    return { body: b, sources_count: sources.length, enrich };
  });
  const researched = await pLimit(tasks, CONCURRENCY);

  if (body.dry_run) {
    return json({ dry_run: true, tier, bodies: bodies.length, preview: researched.slice(0, 5) });
  }

  const now = new Date().toISOString();
  let bodies_updated = 0;
  const writeErrors: string[] = [];

  for (const r of researched) {
    // Dog-policy fields always overwrite
    const dogFields: Record<string, unknown> = {
      dogs_allowed:             r.enrich.dogs_allowed,
      dogs_leash_required:      r.enrich.dogs_leash_required,
      dogs_allowed_areas:       r.enrich.dogs_allowed_areas,
      dogs_prohibited_areas:    r.enrich.dogs_prohibited_areas,
      dogs_off_leash_area:      r.enrich.dogs_off_leash_area,
      dogs_time_restrictions:   r.enrich.dogs_time_restrictions,
      dogs_season_restrictions: r.enrich.dogs_season_restrictions,
      dogs_policy_source:       `${tier}_research`,
      dogs_policy_source_url:   r.enrich.dogs_policy_source_url,
      dogs_policy_notes:        r.enrich.dogs_policy_notes,
      dogs_policy_updated_at:   now,
      enrichment_source:        `${tier}_research`,
      enrichment_updated_at:    now,
      enrichment_confidence:    r.enrich.confidence,
    };

    // Write dog fields to all beaches under this body
    const { error: uErr } = await supabase
      .from("beaches_staging_new")
      .update(dogFields)
      .eq("governing_body", r.body)
      .eq("governing_jurisdiction", `governing ${tier}`)
      .eq("review_status", "ready");
    if (uErr) { writeErrors.push(`body "${r.body}" dog: ${uErr.message}`); continue; }
    bodies_updated++;

    // Amenity / parking / hours fields — fill NULLs only, per-row (can't do
    // "coalesce on update" at the table level through the PostgREST API)
    const amenityFields: Record<string, unknown> = {
      has_parking:         r.enrich.has_parking,
      parking_type:        r.enrich.parking_type,
      parking_notes:       r.enrich.parking_notes,
      hours_text:          r.enrich.hours_text,
      hours_notes:         r.enrich.hours_notes,
      has_restrooms:       r.enrich.has_restrooms,
      has_showers:         r.enrich.has_showers,
      has_lifeguards:      r.enrich.has_lifeguards,
      has_picnic_area:     r.enrich.has_picnic_area,
      has_food:            r.enrich.has_food,
      has_drinking_water:  r.enrich.has_drinking_water,
      has_fire_pits:       r.enrich.has_fire_pits,
      has_disabled_access: r.enrich.has_disabled_access,
    };

    // Fetch existing rows so we can skip non-null fields
    const { data: existing, error: fErr } = await supabase
      .from("beaches_staging_new")
      .select("id, has_parking, parking_type, parking_notes, hours_text, hours_notes, has_restrooms, has_showers, has_lifeguards, has_picnic_area, has_food, has_drinking_water, has_fire_pits, has_disabled_access")
      .eq("governing_body", r.body)
      .eq("governing_jurisdiction", `governing ${tier}`)
      .eq("review_status", "ready");
    if (fErr) { writeErrors.push(`body "${r.body}" fetch: ${fErr.message}`); continue; }

    for (const e of existing ?? []) {
      const patch: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(amenityFields)) {
        if (v !== null && (e as Record<string, unknown>)[k] === null) patch[k] = v;
      }
      if (Object.keys(patch).length === 0) continue;
      const { error: pErr } = await supabase
        .from("beaches_staging_new").update(patch).eq("id", e.id);
      if (pErr) writeErrors.push(`id ${e.id}: ${pErr.message}`);
    }
  }

  return json({
    tier,
    bodies:         researched.length,
    bodies_updated,
    summary: {
      dogs_yes:       researched.filter(r => r.enrich.dogs_allowed === "yes").length,
      dogs_no:        researched.filter(r => r.enrich.dogs_allowed === "no").length,
      dogs_mixed:     researched.filter(r => r.enrich.dogs_allowed === "mixed").length,
      dogs_seasonal:  researched.filter(r => r.enrich.dogs_allowed === "seasonal").length,
      dogs_unknown:   researched.filter(r => r.enrich.dogs_allowed === "unknown").length,
      parking_yes:    researched.filter(r => r.enrich.has_parking === true).length,
      restrooms_yes:  researched.filter(r => r.enrich.has_restrooms === true).length,
      lifeguards_yes: researched.filter(r => r.enrich.has_lifeguards === true).length,
      hours_known:    researched.filter(r => r.enrich.hours_text !== null).length,
      high_conf:      researched.filter(r => r.enrich.confidence === "high").length,
    },
    errors: writeErrors.slice(0, 20),
  });
});
