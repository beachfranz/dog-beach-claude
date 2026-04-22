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

/**
 * Extract a field from an ArcGIS feature's attributes using the source's
 * field_map. Returns null if the mapped key is missing or the attribute
 * itself is null/empty.
 */
export function extractField(
  source: PipelineSource,
  attrs: Record<string, unknown>,
  logicalName: string,
): string | null {
  const physicalKey = source.field_map?.[logicalName];
  if (!physicalKey) return null;
  const v = attrs[physicalKey];
  if (v === null || v === undefined || v === "") return null;
  return String(v);
}
