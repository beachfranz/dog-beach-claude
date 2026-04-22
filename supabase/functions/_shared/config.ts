// _shared/config.ts
// Helpers for the multi-state config tables (pipeline_sources, state_config,
// park_operators, private_land_zones, sma_code_mappings, research_prompts).
//
// Principle: edge functions read configuration via these helpers instead of
// hardcoding URLs / field names / state-specific values.

// deno-lint-ignore-file no-explicit-any

export interface PipelineSource {
  source_key:     string;
  state_code:     string | null;
  kind:           "polygon" | "point" | "polyline" | "rest_json";
  url:            string;
  query_defaults: Record<string, unknown>;
  field_map:      Record<string, string>;
  priority:       number;
  notes:          string | null;
}

export interface StateConfig {
  state_code:                string;
  state_name:                string;
  enabled:                   boolean;
  coastal_default_tier:      "state" | "county";
  coastal_default_body:      string | null;
  has_coastal_access_source: boolean;
  research_context_notes:    string | null;
  excluded_federal_units:    string[];
}

// Map full state name (as returned by Google geocoder / stored in
// beaches_staging_new.state) to its 2-letter code used throughout the config
// tables. Expand as we add states.
const STATE_NAME_TO_CODE: Record<string, string> = {
  "California": "CA",
  "Oregon":     "OR",
  "Washington": "WA",
  "Hawaii":     "HI",
};

export function stateCodeFromName(name: string | null | undefined): string | null {
  if (!name) return null;
  return STATE_NAME_TO_CODE[name] ?? null;
}

/**
 * Return the best pipeline_source for (source_key, state_code).
 * Prefers state-specific over national. Lower `priority` wins within scope.
 */
export async function getSource(
  supabase: any,
  source_key: string,
  state_code?: string | null,
): Promise<PipelineSource | null> {
  if (state_code) {
    const { data } = await supabase
      .from("pipeline_sources")
      .select("*")
      .eq("source_key", source_key)
      .eq("state_code", state_code)
      .eq("active", true)
      .order("priority", { ascending: true })
      .limit(1)
      .maybeSingle();
    if (data) return data as PipelineSource;
  }
  const { data } = await supabase
    .from("pipeline_sources")
    .select("*")
    .eq("source_key", source_key)
    .is("state_code", null)
    .eq("active", true)
    .order("priority", { ascending: true })
    .limit(1)
    .maybeSingle();
  return (data as PipelineSource | null) ?? null;
}

/**
 * Strict version of getSource: throws a descriptive error if no matching row
 * exists. Use this at the top of an edge function where a source is required
 * to run; use getSource() in contexts where a missing source should be
 * gracefully skipped.
 */
export async function requireSource(
  supabase: any,
  source_key: string,
  state_code?: string | null,
): Promise<PipelineSource> {
  const s = await getSource(supabase, source_key, state_code);
  if (!s) {
    throw new Error(
      `Config error: no active pipeline_sources row for source_key='${source_key}'` +
      (state_code ? ` and state_code='${state_code}' (or national fallback).` : '.') +
      ' Check pipeline_sources table.'
    );
  }
  return s;
}

export async function getStateConfig(
  supabase: any,
  state_code: string,
): Promise<StateConfig | null> {
  const { data } = await supabase
    .from("state_config")
    .select("*")
    .eq("state_code", state_code)
    .maybeSingle();
  if (!data) return null;
  const cfg = data as Record<string, unknown>;
  return {
    state_code:                String(cfg.state_code),
    state_name:                String(cfg.state_name),
    enabled:                   Boolean(cfg.enabled),
    coastal_default_tier:      (cfg.coastal_default_tier as "state" | "county"),
    coastal_default_body:      (cfg.coastal_default_body as string | null) ?? null,
    has_coastal_access_source: Boolean(cfg.has_coastal_access_source),
    research_context_notes:    (cfg.research_context_notes as string | null) ?? null,
    excluded_federal_units:    Array.isArray(cfg.excluded_federal_units)
                                 ? cfg.excluded_federal_units as string[]
                                 : [],
  };
}

/**
 * Strict version of getStateConfig: throws if the state has no config row.
 */
export async function requireStateConfig(
  supabase: any,
  state_code: string,
): Promise<StateConfig> {
  const cfg = await getStateConfig(supabase, state_code);
  if (!cfg) {
    throw new Error(
      `Config error: no state_config row for state_code='${state_code}'. ` +
      `Insert a row before running the pipeline for this state.`
    );
  }
  return cfg;
}

/**
 * Build an ArcGIS FeatureServer/MapServer query URL from a source's URL +
 * query_defaults + caller-provided params. The result is a complete URL
 * suitable for fetch().
 */
export function buildArcgisQueryUrl(
  source: PipelineSource,
  params: Record<string, string>,
): string {
  const u = new URL(source.url);
  // Merge query_defaults first, then caller params (caller wins)
  for (const [k, v] of Object.entries(source.query_defaults ?? {})) {
    u.searchParams.set(k, String(v));
  }
  for (const [k, v] of Object.entries(params)) {
    u.searchParams.set(k, String(v));
  }
  u.searchParams.set("f", "json");
  return u.toString();
}

// Track warnings per-request so we don't spam logs when extractField is
// called repeatedly in a loop over many features. Each (source, logical_name)
// pair warns once per edge-function invocation.
const WARNED_MISSING_FIELDS = new Set<string>();

/**
 * Extract a field from an ArcGIS feature's attributes using the source's
 * field_map.
 *
 * Returns null in two cases, distinguished by log behaviour:
 *   (a) field_map[logicalName] is undefined — CONFIG ERROR, a logical name
 *       the caller expects isn't mapped at all. Logs a warning (once per
 *       source+name per invocation) to surface typos in pipeline_sources.field_map.
 *   (b) field_map[logicalName] is defined but the actual attribute on the
 *       feature is null/empty — normal data variability, silent null.
 */
export function extractField(
  source: PipelineSource,
  attrs: Record<string, unknown>,
  logicalName: string,
): string | null {
  const physicalKey = source.field_map?.[logicalName];
  if (!physicalKey) {
    const warnKey = `${source.source_key}:${source.state_code ?? "null"}:${logicalName}`;
    if (!WARNED_MISSING_FIELDS.has(warnKey)) {
      WARNED_MISSING_FIELDS.add(warnKey);
      console.warn(
        `[config] extractField: logical name '${logicalName}' not found in field_map for ` +
        `source_key='${source.source_key}' state_code='${source.state_code ?? "null"}'. ` +
        `Check pipeline_sources.field_map.`
      );
    }
    return null;
  }
  const v = attrs[physicalKey];
  if (v === null || v === undefined || v === "") return null;
  return String(v);
}
