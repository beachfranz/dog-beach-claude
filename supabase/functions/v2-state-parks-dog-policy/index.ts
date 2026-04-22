// v2-state-parks-dog-policy/index.ts
// Phase 2 tier 2 — per-state-park dog policy research.
//
// For each unique state park unit in our dataset, does ONE pass of:
//   1. Tavily web search   "<park name> dogs beach California state park"
//   2. Claude Haiku extraction from top results
// and applies the resulting policy to all beaches in that park.
//
// Covers records with governing_body_source in:
//   state_polygon, state_name_rescue
// (not cpad_state — those are UC reserves / Tahoe Conservancy / CDFW with
// distinct policies; handled in a separate stage if needed.)
//
// Writes to: dogs_allowed, dogs_leash_required, dogs_off_leash_area,
// dogs_time_restrictions, dogs_season_restrictions, dogs_policy_source,
// dogs_policy_source_url, dogs_policy_notes, dogs_policy_updated_at.
//
// POST { dry_run?: boolean, park_filter?: string, limit?: number }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import Anthropic from "https://esm.sh/@anthropic-ai/sdk@0.39.0";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ANTHROPIC_API_KEY    = Deno.env.get("anthropic_api_key")!;
const TAVILY_API_KEY       = Deno.env.get("TAVILY_API_KEY")!;

const MODEL       = "claude-haiku-4-5-20251001";
const CONCURRENCY = 3;  // tavily free tier is rate-limited

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

  Important: for the headline answer, we care about access to the BEACH (sand / shore), not just the campground or parking lot. If dogs are welcome in the parking lot and campground but not on the sand, that is "no" (unless separate beaches within the park do allow dogs, in which case "mixed").

- dogs_allowed_areas: short description of where dogs ARE permitted (e.g. "Blind Beach, campgrounds, paved trails"). null if dogs allowed park-wide or if dogs_allowed = "no".
- dogs_prohibited_areas: short description of where dogs are NOT permitted (e.g. "Bodega Dunes, Russian River SMCA, nature trails, main beach"). null if dogs_allowed = "no" (the whole park is prohibited) or if dogs allowed park-wide.
- dogs_leash_required: true, false, or null if unknown
- dogs_off_leash_area: description if there's a designated off-leash zone; otherwise null
- dogs_time_restrictions: description if hours limit dog access (e.g. "before 9am and after 5pm"); otherwise null
- dogs_season_restrictions: description if seasonal limits apply (e.g. "closed to dogs March-September for snowy plover nesting"); otherwise null
- dogs_policy_notes: one or two sentences summarizing the policy for a user deciding whether to visit.
- dogs_policy_source_url: the single most authoritative URL from the sources above (prefer parks.ca.gov or the park's official page)
- confidence: "high" if the sources directly state the park's rules; "low" if inferring from general state park policy or unclear sources

If the sources are unclear or contradictory, mark confidence as "low" and dogs_allowed as "unknown".

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

  // Fetch distinct state parks
  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("governing_body")
    .in("governing_body_source", ["state_polygon", "state_name_rescue"])
    .eq("review_status", "ready")
    .not("governing_body", "is", null);
  if (error) return json({ error: error.message }, 500);

  const parkSet = new Set<string>();
  for (const r of rows ?? []) parkSet.add(r.governing_body);
  let parks = [...parkSet];
  if (body.park_filter) parks = parks.filter(p => p.toLowerCase().includes(body.park_filter!.toLowerCase()));
  if (body.limit)       parks = parks.slice(0, body.limit);

  if (parks.length === 0) return json({ parks: 0 });

  // Research each park once
  const tasks = parks.map(park => async () => {
    const query   = `${park} dogs on beach California`;
    const sources = await tavilySearch(query);
    const policy  = sources.length === 0 ? defaultPolicy() : await extractPolicy(park, sources);
    return { park, sources_count: sources.length, policy };
  });
  const researched = await pLimit(tasks, CONCURRENCY);

  if (body.dry_run) {
    return json({
      dry_run: true,
      parks:   parks.length,
      preview: researched.slice(0, 10),
    });
  }

  // Write policy to all beaches for each researched park
  const now = new Date().toISOString();
  let beaches_updated = 0;
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
        dogs_policy_source:       "state_parks_research",
        dogs_policy_source_url:   r.policy.dogs_policy_source_url,
        dogs_policy_notes:        r.policy.dogs_policy_notes,
        dogs_policy_updated_at:   now,
      })
      .eq("governing_body", r.park)
      .in("governing_body_source", ["state_polygon", "state_name_rescue"]);
    if (uErr) writeErrors.push(`park "${r.park}": ${uErr.message}`);
    else      beaches_updated += 1;   // approximate; eq() updates all matches
  }

  return json({
    parks:          researched.length,
    beaches_updated,
    summary: {
      yes:        researched.filter(r => r.policy.dogs_allowed === "yes").length,
      no:         researched.filter(r => r.policy.dogs_allowed === "no").length,
      mixed:      researched.filter(r => r.policy.dogs_allowed === "mixed").length,
      seasonal:   researched.filter(r => r.policy.dogs_allowed === "seasonal").length,
      unknown:    researched.filter(r => r.policy.dogs_allowed === "unknown").length,
      high_conf:  researched.filter(r => r.policy.confidence === "high").length,
      low_conf:   researched.filter(r => r.policy.confidence === "low").length,
    },
    errors:         writeErrors,
  });
});
