// v2-city-classify/index.ts
// Pipeline stage 8 — city classifier. Config-driven: reads source_key='city_polygon'
// (Census TIGER/Line Places, national). Filters to incorporated places via
// LSADC=25 check.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import { buildArcgisQueryUrl, extractField, requireSource, PipelineSource, stateCodeFromName } from "../_shared/config.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const CONCURRENCY = 10;
const BUFFER_DEG  = 0.001;   // ~100m

function envelopeAround(lat: number, lon: number, delta: number): string {
  return JSON.stringify({
    xmin: lon - delta, ymin: lat - delta,
    xmax: lon + delta, ymax: lat + delta,
    spatialReference: { wkid: 4326 },
  });
}

async function findCity(lat: number, lon: number, buffer: boolean, source: PipelineSource): Promise<{ name: string; geoid: string } | null> {
  const extra: Record<string, string> = buffer
    ? { geometry: envelopeAround(lat, lon, BUFFER_DEG), geometryType: "esriGeometryEnvelope" }
    : { geometry: `${lon},${lat}`, geometryType: "esriGeometryPoint" };
  const url = buildArcgisQueryUrl(source, {
    ...extra,
    spatialRel:     "esriSpatialRelIntersects",
    inSR:           "4326",
    returnGeometry: "false",
  });
  try {
    const resp = await fetch(url);
    if (!resp.ok) return null;
    const data = await resp.json();
    const features = data?.features ?? [];
    // Filter to incorporated places (LSADC=25). The lsadc field-map entry tells us
    // which attribute to inspect.
    const lsadcKey = source.field_map?.lsadc ?? "LSADC";
    const hit = features.find((f: { attributes: Record<string, unknown> }) =>
      String(f.attributes[lsadcKey] ?? "") === "25",
    );
    if (!hit) return null;
    const attrs = hit.attributes;
    return {
      name:  extractField(source, attrs, "name")  ?? "",
      geoid: extractField(source, attrs, "geoid") ?? "",
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

  let body: { state_code?: string; limit?: number; dry_run?: boolean; use_buffer?: boolean } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const stateCode = body.state_code ?? "CA";
  const useBuffer = body.use_buffer !== false;
  const supabase  = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  let source: PipelineSource;
  try { source = await requireSource(supabase, "city_polygon", stateCode); }
  catch (e) { return json({ error: (e as Error).message }, 500); }

  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("id, display_name, latitude, longitude, state")
    .is("review_status", null)
    .not("latitude", "is", null)
    .not("longitude", "is", null)
    .limit(body.limit ?? 5000);
  if (error) return json({ error: error.message }, 500);
  const filtered = (rows ?? []).filter(r => stateCodeFromName(r.state) === stateCode);
  if (!filtered.length) return json({ state_code: stateCode, processed: 0, matched: 0, updated: 0 });

  const tasks = filtered.map(r => async () => {
    let hit = await findCity(r.latitude, r.longitude, false, source);
    let matchedVia: "exact" | "buffer" | null = hit ? "exact" : null;
    if (!hit && useBuffer) {
      hit = await findCity(r.latitude, r.longitude, true, source);
      if (hit) matchedVia = "buffer";
    }
    return { id: r.id, display_name: r.display_name, hit, matchedVia };
  });
  const results = await pLimit(tasks, CONCURRENCY);
  const matches = results.filter(r => r.hit);

  if (body.dry_run) {
    return json({
      dry_run: true, state_code: stateCode,
      processed: filtered.length,
      matched:  matches.length,
      exact:    matches.filter(m => m.matchedVia === "exact").length,
      buffered: matches.filter(m => m.matchedVia === "buffer").length,
      preview:  matches.slice(0, 50).map(m => ({
        display_name: m.display_name,
        city:         m.hit!.name,
        geoid:        m.hit!.geoid,
        via:          m.matchedVia,
      })),
    });
  }

  let updated = 0;
  const writeErrors: string[] = [];
  for (const m of matches) {
    const note = m.matchedVia === "buffer"
      ? `Beach lies within ~100m of ${m.hit!.name} city boundary (Census TIGER).`
      : `Beach falls within ${m.hit!.name} city boundary (Census TIGER).`;
    const { error } = await supabase
      .from("beaches_staging_new")
      .update({
        governing_jurisdiction: "governing city",
        governing_body:         `City of ${m.hit!.name}`,
        governing_body_source:  m.matchedVia === "buffer" ? "city_polygon_buffer" : "city_polygon",
        governing_body_notes:   note,
        review_status:          "ready",
        review_notes:           "Confirmed city via Census TIGER Places polygon match.",
      })
      .eq("id", m.id);
    if (error) writeErrors.push(`id ${m.id}: ${error.message}`);
    else updated++;
  }

  return json({ state_code: stateCode, processed: filtered.length, matched: matches.length, updated, errors: writeErrors });
});
