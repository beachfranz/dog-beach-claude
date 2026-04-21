// v2-federal-classify/index.ts
// Pipeline stage 5 — primary federal classifier via point-in-polygon against
// Esri Living Atlas USA Federal Lands (NPS, USFS, BLM, DOD, FWS, USBR).
//
// Federal takes priority over state/city. If hit, governing federal + ready.
// Excludes mixed-management NRAs (Santa Monica Mountains NRA) — polygon
// membership in those units does not imply federal management of the beach.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const FEDERAL_URL   = "https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/USA_Federal_Lands/FeatureServer/0/query";
const CONCURRENCY   = 10;
const DEFAULT_LIMIT = 2000;

const MIXED_MANAGEMENT_UNITS = new Set([
  "Santa Monica Mountains National Recreation Area",
]);

async function findFederalUnit(lat: number, lon: number): Promise<{ agency: string; unit: string } | null> {
  const params = new URLSearchParams({
    geometry:       `${lon},${lat}`,
    geometryType:   "esriGeometryPoint",
    spatialRel:     "esriSpatialRelIntersects",
    inSR:           "4326",
    outFields:      "Agency,unit_name",
    returnGeometry: "false",
    f:              "json",
  });
  try {
    const resp = await fetch(`${FEDERAL_URL}?${params}`);
    if (!resp.ok) return null;
    const data = await resp.json();
    const features = data?.features ?? [];
    if (features.length === 0) return null;
    return {
      agency: String(features[0].attributes.Agency ?? ""),
      unit:   String(features[0].attributes.unit_name ?? ""),
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

  let body: { limit?: number; dry_run?: boolean } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("id, display_name, latitude, longitude")
    .is("review_status", null)
    .not("latitude", "is", null)
    .not("longitude", "is", null)
    .limit(body.limit ?? DEFAULT_LIMIT);
  if (error) return json({ error: error.message }, 500);
  if (!rows?.length) return json({ processed: 0, matched: 0, updated: 0 });

  const tasks = rows.map(r => async () => ({
    id:           r.id,
    display_name: r.display_name,
    hit:          await findFederalUnit(r.latitude, r.longitude),
  }));
  const results = await pLimit(tasks, CONCURRENCY);

  const matches = results.filter(r => r.hit && !MIXED_MANAGEMENT_UNITS.has(r.hit.unit));
  const skipped = results.filter(r => r.hit && MIXED_MANAGEMENT_UNITS.has(r.hit.unit));

  if (body.dry_run) {
    return json({
      dry_run:   true,
      processed: rows.length,
      matched:   matches.length,
      skipped_mixed_management: skipped.length,
      preview:   matches.slice(0, 50).map(m => ({
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
    processed: rows.length,
    matched:   matches.length,
    updated,
    skipped_mixed_management: skipped.length,
    errors: writeErrors,
  });
});
