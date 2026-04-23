// v2-county-dog-policy/index.ts
// Per-county / per-county-park dog policy research.
//
// Covers records with:
//   governing_jurisdiction = 'governing county'
//   governing_body_source in (county_polygon, county_default,
//                              county_name_rescue, state_operator_override)
//
// Counties vary: LA County Beaches & Harbors has formal dog rules for 20+
// beaches it operates; smaller coastal counties usually have a county-wide
// leash law plus dog-specific beaches within some parks. Inland counties
// with lake/reservoir beaches often have county-park-specific rules.

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

async function extractPolicy(body: string, sources: TavilyResult[]): Promise<DogPolicy> {
  const block = sources
    .map((s, i) => `[${i + 1}] ${s.url}\n${s.title}\n${s.content}`)
    .join("\n\n");

  const prompt = `You are extracting the dog policy for county-managed beaches in California.

Governing body: ${body}

Here are web search results about dog rules for beaches managed by this county:

${block}

Extract the dog policy as a JSON object with these fields:

- dogs_allowed: one of:
    "yes"      — dogs allowed on the county's beaches (the sand / shore) generally
    "no"       — dogs prohibited from county beaches
    "mixed"    — some county beaches allow dogs, others don't (very common — e.g. LA County Beaches & Harbors allows dogs at Dockweiler RV Park portion only)
    "seasonal" — dog access restricted by time of year (e.g. snowy plover nesting)
    "unknown"  — cannot determine from sources

  Important context:
  - Coastal county beaches (LA, Santa Cruz, Sonoma, Marin, Monterey etc.) typically have a leash requirement plus per-beach rules
  - Inland county beaches (Nevada, Calaveras, Placer — Lake Tahoe area) are often at county-managed reservoirs/lakes and may have different rules
  - County parks departments usually publish beach-specific dog policies; LA County Beaches & Harbors is particularly detailed
  - Default CA state law: dogs must be on a leash no longer than 6 feet in public spaces

- dogs_allowed_areas: short description of specific beaches/areas where dogs ARE permitted. null if dogs_allowed = "no" or allowed county-wide.
- dogs_prohibited_areas: short description of specific beaches where dogs are NOT permitted. null if prohibited county-wide or allowed county-wide.
- dogs_leash_required: "yes", "no", "mixed", or null if unknown (mixed = rules vary by area or time)
- dogs_off_leash_area: designated off-leash area if any; otherwise null
- dogs_time_restrictions: description if hours limit access; otherwise null
- dogs_season_restrictions: description if seasonal limits apply; otherwise null
- dogs_policy_notes: one or two sentences summarizing the policy for a user.
- dogs_policy_source_url: the single most authoritative URL (prefer the county parks department's official page)
- confidence: "high" if sources directly state the county's rules; "low" if inferring or unclear

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

  let body: { dry_run?: boolean; body_filter?: string; limit?: number } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("governing_body")
    .in("governing_body_source", ["county_polygon","county_default","county_name_rescue","state_operator_override"])
    .eq("governing_jurisdiction", "governing county")
    .eq("review_status", "ready")
    .not("governing_body", "is", null);
  if (error) return json({ error: error.message }, 500);

  const bodySet = new Set<string>();
  for (const r of rows ?? []) bodySet.add(r.governing_body);
  let bodies = [...bodySet];
  if (body.body_filter) bodies = bodies.filter(b => b.toLowerCase().includes(body.body_filter!.toLowerCase()));
  if (body.limit)       bodies = bodies.slice(0, body.limit);

  if (bodies.length === 0) return json({ bodies: 0 });

  const tasks = bodies.map(b => async () => {
    const query   = `${b} dog policy beach California`;
    const sources = await tavilySearch(query);
    const policy  = sources.length === 0 ? defaultPolicy() : await extractPolicy(b, sources);
    return { body: b, sources_count: sources.length, policy };
  });
  const researched = await pLimit(tasks, CONCURRENCY);

  if (body.dry_run) {
    return json({ dry_run: true, bodies: bodies.length, preview: researched.slice(0, 10) });
  }

  const now = new Date().toISOString();
  let bodies_updated = 0;
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
        dogs_policy_source:       "county_research",
        dogs_policy_source_url:   r.policy.dogs_policy_source_url,
        dogs_policy_notes:        r.policy.dogs_policy_notes,
        dogs_policy_updated_at:   now,
      })
      .eq("governing_body", r.body)
      .eq("governing_jurisdiction", "governing county")
      .in("governing_body_source", ["county_polygon","county_default","county_name_rescue","state_operator_override"]);
    if (uErr) writeErrors.push(`body "${r.body}": ${uErr.message}`);
    else      bodies_updated += 1;
  }

  return json({
    bodies:         researched.length,
    bodies_updated,
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
