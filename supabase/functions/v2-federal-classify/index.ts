// v2-federal-classify/index.ts
// Pipeline stage 5 — primary federal classifier via point-in-polygon.
//
// Config-driven (Phase 3 refactor):
//   - Source URL + field_map read from pipeline_sources where
//     source_key='federal_polygon'.
//   - Mixed-management unit exclusions read from state_config.excluded_federal_units
//     per state.
//
// Accepts `state_code` in POST body (default 'CA'). Rows are filtered to
// that state so one pipeline run = one state.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import {
  buildArcgisQueryUrl,
  extractField,
  requireSource,
  getStateConfig,
  PipelineSource,
  stateCodeFromName,
} from "../_shared/config.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const CONCURRENCY   = 10;
const DEFAULT_LIMIT = 2000;

async function findFederalUnit(
  lat: number, lon: number, source: PipelineSource,
): Promise<{ agency: string; unit: string } | null> {
  const url = buildArcgisQueryUrl(source, {
    geometry:       `${lon},${lat}`,
    geometryType:   "esriGeometryPoint",
    spatialRel:     "esriSpatialRelIntersects",
    inSR:           "4326",
    returnGeometry: "false",
  });
  try {
    const resp = await fetch(url);
    if (!resp.ok) return null;
    const data = await resp.json();
    const features = data?.features ?? [];
    if (features.length === 0) return null;
    const attrs = features[0].attributes ?? {};
    return {
      agency: extractField(source, attrs, "agency") ?? "",
      unit:   extractField(source, attrs, "unit")   ?? "",
    };
  } catch { return null; }
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

  let body: { state_code?: string; limit?: number; dry_run?: boolean } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const stateCode = body.state_code ?? "CA";
  const supabase  = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  let source: PipelineSource;
  try { source = await requireSource(supabase, "federal_polygon", stateCode); }
  catch (e) { return json({ error: (e as Error).message }, 500); }

  const stateCfg = await getStateConfig(supabase, stateCode);
  const excludedUnits = new Set(stateCfg?.excluded_federal_units ?? []);

  // Filter beaches_staging_new to rows whose state (full name from geocoder)
  // matches the requested state_code.
  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("id, display_name, latitude, longitude, state")
    .is("review_status", null)
    .not("latitude", "is", null)
    .not("longitude", "is", null)
    .limit(body.limit ?? DEFAULT_LIMIT);
  if (error) return json({ error: error.message }, 500);
  const filtered = (rows ?? []).filter(r => stateCodeFromName(r.state) === stateCode);
  if (!filtered.length) return json({ state_code: stateCode, processed: 0, matched: 0, updated: 0 });

  const tasks = filtered.map(r => async () => ({
    id:           r.id,
    display_name: r.display_name,
    hit:          await findFederalUnit(r.latitude, r.longitude, source),
  }));
  const results = await pLimit(tasks, CONCURRENCY);

  const matches = results.filter(r => r.hit && !excludedUnits.has(r.hit.unit));
  const skipped = results.filter(r => r.hit &&  excludedUnits.has(r.hit.unit));

  if (body.dry_run) {
    return json({
      dry_run:                  true,
      state_code:               stateCode,
      source:                   { url: source.url, priority: source.priority, state_code: source.state_code },
      excluded_units:           [...excludedUnits],
      processed:                filtered.length,
      matched:                  matches.length,
      skipped_mixed_management: skipped.length,
      preview:                  matches.slice(0, 50).map(m => ({
        display_name: m.display_name,
        agency:       m.hit!.agency,
        unit:         m.hit!.unit,
      })),
    });
  }

  let updated = 0;
  const writeErrors: string[] = [];
  for (const m of matches) {
    const { error } = await supabase
      .from("beaches_staging_new")
      .update({
        governing_jurisdiction: "governing federal",
        governing_body:         m.hit!.unit,
        governing_body_source:  "federal_polygon",
        governing_body_notes:   `Beach falls within ${m.hit!.unit} (${m.hit!.agency}) boundary polygon.`,
        review_status:          "ready",
        review_notes:           `Confirmed federal via ${m.hit!.agency} boundary polygon match.`,
      })
      .eq("id", m.id);
    if (error) writeErrors.push(`id ${m.id}: ${error.message}`);
    else updated++;
  }

  return json({
    state_code:               stateCode,
    processed:                filtered.length,
    matched:                  matches.length,
    updated,
    skipped_mixed_management: skipped.length,
    errors:                   writeErrors,
  });
});
