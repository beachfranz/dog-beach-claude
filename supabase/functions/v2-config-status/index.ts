// v2-config-status/index.ts
// Diagnostic endpoint. Returns a snapshot of the multi-state config tables
// for a quick sanity check at the start of a session or before adding a
// new state.
//
// Report structure:
//   - pipeline_sources summary: counts by state_code and source_key, flags
//     missing source_keys (referenced by known stages but not in the table)
//   - state_config: per-state flags
//   - park_operators: count per state
//   - private_land_zones: count per state
//   - sma_code_mappings: count per agency_type
//   - research_prompts: count per (state, tier); flags missing (state, tier)
//     combos for any enabled state
//   - beaches_staging_new: counts per state + per (state, review_status)
//
// POST {}  returns JSON.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// Source keys that the pipeline stages reference. Used to check for missing
// source_key rows (a stage that asks for a source that isn't configured).
const REQUIRED_SOURCE_KEYS: { key: string; scope: "national_ok" | "per_state" }[] = [
  { key: "federal_polygon",           scope: "national_ok" },
  { key: "state_park_polygon",        scope: "per_state"   },
  { key: "city_polygon",              scope: "national_ok" },
  { key: "cpad_polygon",              scope: "national_ok" },
  { key: "coastal_access_points",     scope: "per_state"   },  // gated by state_config.has_coastal_access_source
  { key: "blm_sma",                   scope: "per_state"   },  // fallback to blm_sma_national
  { key: "blm_sma_national",          scope: "national_ok" },
  { key: "noaa_tide_stations",        scope: "national_ok" },
  { key: "geocoder_google",           scope: "national_ok" },
  { key: "geocoder_census_incorporated", scope: "national_ok" },
];

const TIERS = ["federal", "state", "city", "county"] as const;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (b: unknown, s = 200) => new Response(JSON.stringify(b, null, 2), { status: s, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // ── pipeline_sources ──────────────────────────────────────────────────
  const { data: sources } = await supabase
    .from("pipeline_sources")
    .select("source_key, state_code, url, priority, active, notes")
    .order("source_key").order("state_code", { ascending: true, nullsFirst: true });

  const sourcesByKey: Record<string, Array<{ state_code: string | null; url: string; priority: number; active: boolean }>> = {};
  for (const s of sources ?? []) {
    (sourcesByKey[s.source_key] ??= []).push({
      state_code: s.state_code, url: s.url.slice(0, 100), priority: s.priority, active: s.active,
    });
  }

  // ── state_config ──────────────────────────────────────────────────────
  const { data: states } = await supabase
    .from("state_config")
    .select("state_code, state_name, enabled, coastal_default_tier, coastal_default_body, has_coastal_access_source, excluded_federal_units")
    .order("state_code");

  const enabledStates = (states ?? []).filter(s => s.enabled).map(s => s.state_code);

  // ── park_operators ────────────────────────────────────────────────────
  const { data: operators } = await supabase.from("park_operators").select("state_code");
  const operatorsByState: Record<string, number> = {};
  for (const o of operators ?? []) operatorsByState[o.state_code] = (operatorsByState[o.state_code] ?? 0) + 1;

  // ── private_land_zones ────────────────────────────────────────────────
  const { data: zones } = await supabase.from("private_land_zones").select("state_code, active");
  const zonesByState: Record<string, number> = {};
  for (const z of zones ?? []) if (z.active) zonesByState[z.state_code] = (zonesByState[z.state_code] ?? 0) + 1;

  // ── sma_code_mappings ─────────────────────────────────────────────────
  const { data: smaRows } = await supabase.from("sma_code_mappings").select("agency_type");
  const smaByType: Record<string, number> = {};
  for (const s of smaRows ?? []) smaByType[s.agency_type] = (smaByType[s.agency_type] ?? 0) + 1;

  // ── research_prompts ──────────────────────────────────────────────────
  const { data: prompts } = await supabase
    .from("research_prompts").select("state_code, tier, active");
  const promptCoverage: Record<string, { federal: boolean; state: boolean; city: boolean; county: boolean }> = {};
  for (const p of prompts ?? []) {
    if (!p.active) continue;
    (promptCoverage[p.state_code] ??= { federal: false, state: false, city: false, county: false })[p.tier as typeof TIERS[number]] = true;
  }

  // ── beaches_staging_new ───────────────────────────────────────────────
  const { data: beaches } = await supabase
    .from("beaches_staging_new").select("state, review_status");
  const beachByState: Record<string, { total: number; ready: number; invalid: number; duplicate: number; unclassified: number }> = {};
  for (const b of beaches ?? []) {
    const s = b.state ?? "null";
    const e = (beachByState[s] ??= { total: 0, ready: 0, invalid: 0, duplicate: 0, unclassified: 0 });
    e.total++;
    if (b.review_status === "ready")          e.ready++;
    else if (b.review_status === "invalid")   e.invalid++;
    else if (b.review_status === "duplicate") e.duplicate++;
    else                                       e.unclassified++;
  }

  // ── consistency checks ────────────────────────────────────────────────
  const issues: string[] = [];

  // Some source_keys are gated by a state_config flag (e.g. coastal_access_points
  // is only required when that state has has_coastal_access_source=true) or
  // have an alternate source_key as fallback (blm_sma → blm_sma_national).
  const GATED_BY_FLAG: Record<string, keyof typeof states[number] | ""> = {
    coastal_access_points: "has_coastal_access_source",
  };
  const FALLBACK_KEY: Record<string, string> = {
    blm_sma: "blm_sma_national",
  };

  // Required source_keys: which are missing?
  for (const { key, scope } of REQUIRED_SOURCE_KEYS) {
    const rows = sourcesByKey[key] ?? [];
    if (rows.length === 0 && !FALLBACK_KEY[key]) {
      issues.push(`MISSING pipeline_sources row for source_key='${key}' (scope: ${scope})`);
    } else if (scope === "per_state") {
      for (const stateRow of states ?? []) {
        if (!stateRow.enabled) continue;
        // Skip this check if the state has the gating flag set to false
        const gateFlag = GATED_BY_FLAG[key];
        if (gateFlag && !(stateRow as any)[gateFlag]) continue;

        const state = stateRow.state_code;
        const hasState = rows.some(r => r.state_code === state);
        const hasNational = rows.some(r => r.state_code === null);
        const fallbackKey = FALLBACK_KEY[key];
        const hasFallback = fallbackKey && (sourcesByKey[fallbackKey] ?? []).some(r => r.state_code === null || r.state_code === state);
        if (!hasState && !hasNational && !hasFallback) {
          issues.push(`source_key='${key}' has no row for enabled state '${state}' and no national fallback` + (fallbackKey ? ` or '${fallbackKey}'` : ""));
        }
      }
    }
  }

  // For each enabled state: check research_prompts has all 4 tiers
  for (const state of enabledStates) {
    const cov = promptCoverage[state];
    if (!cov) {
      issues.push(`state '${state}' is enabled but has NO research_prompts rows`);
    } else {
      for (const tier of TIERS) {
        if (!cov[tier]) issues.push(`state '${state}' missing research_prompts row for tier='${tier}'`);
      }
    }
  }

  // has_coastal_access_source=true but no coastal_access_points row for state
  for (const s of states ?? []) {
    if (s.has_coastal_access_source) {
      const has = (sourcesByKey["coastal_access_points"] ?? []).some(r => r.state_code === s.state_code);
      if (!has) issues.push(`state '${s.state_code}' has has_coastal_access_source=true but no pipeline_sources row for coastal_access_points`);
    }
  }

  return json({
    issues_count: issues.length,
    issues,
    summary: {
      pipeline_sources: Object.keys(sourcesByKey).length,
      state_config:     (states ?? []).length,
      enabled_states:   enabledStates,
      park_operators:   operatorsByState,
      private_land_zones: zonesByState,
      sma_code_mappings_by_type: smaByType,
      research_prompts_coverage: promptCoverage,
      beaches_by_state: beachByState,
    },
    details: {
      pipeline_sources: sourcesByKey,
      state_config:     states,
    },
  });
});
