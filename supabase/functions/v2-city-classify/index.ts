// v2-city-classify/index.ts
// Pipeline stage 8 — city classifier via Census TIGER/Line Places polygons.
//
// Incorporated places only (LSADC = 25). If a beach's lat/lon is inside a
// Census incorporated place, it's governing city.
//
// Also attempts a short buffer (100m) for beaches that sit just outside the
// polygon — common for coastal beaches on state tidelands adjacent to a
// city that operationally manages them. Buffer is done by querying the
// same point with a small geometry ring expansion.
//
// Runs AFTER federal and state classifiers (those take priority).

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const TIGER_URL =
  "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Places_CouSub_ConCity_SubMCD/MapServer/4/query";
const CONCURRENCY = 10;
const BUFFER_DEG  = 0.001;   // ~100m at California latitude, for envelope expansion

// Build a small envelope around the point for the buffered lookup. ArcGIS
// supports envelope geometry and esriSpatialRelIntersects returns any polygon
// the envelope touches — effectively matching with a ~100m buffer.
function envelopeAround(lat: number, lon: number, delta: number): string {
  const xmin = lon - delta, ymin = lat - delta;
  const xmax = lon + delta, ymax = lat + delta;
  return JSON.stringify({ xmin, ymin, xmax, ymax, spatialReference: { wkid: 4326 } });
}

async function findCity(lat: number, lon: number, buffer: boolean): Promise<{ name: string; geoid: string } | null> {
  const params = new URLSearchParams({
    outFields:      "BASENAME,NAME,LSADC,GEOID",
    returnGeometry: "false",
    f:              "json",
    spatialRel:     "esriSpatialRelIntersects",
    inSR:           "4326",
  });
  if (buffer) {
    params.set("geometry", envelopeAround(lat, lon, BUFFER_DEG));
    params.set("geometryType", "esriGeometryEnvelope");
  } else {
    params.set("geometry", `${lon},${lat}`);
    params.set("geometryType", "esriGeometryPoint");
  }
  try {
    const resp = await fetch(`${TIGER_URL}?${params}`);
    if (!resp.ok) return null;
    const data = await resp.json();
    const features = data?.features ?? [];
    // Filter to incorporated places only (LSADC = 25)
    const hit = features.find((f: { attributes: Record<string, unknown> }) =>
      String(f.attributes.LSADC ?? "") === "25"
    );
    if (!hit) return null;
    return {
      name:  String(hit.attributes.BASENAME ?? ""),
      geoid: String(hit.attributes.GEOID ?? ""),
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

  let body: { limit?: number; dry_run?: boolean; use_buffer?: boolean } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const useBuffer = body.use_buffer !== false;  // default: true
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("id, display_name, latitude, longitude")
    .is("review_status", null)
    .not("latitude", "is", null)
    .not("longitude", "is", null)
    .limit(body.limit ?? 5000);
  if (error) return json({ error: error.message }, 500);
  if (!rows?.length) return json({ processed: 0, matched: 0, updated: 0 });

  // Two-pass: exact first, then buffered for the misses.
  const tasks = rows.map(r => async () => {
    let hit = await findCity(r.latitude, r.longitude, false);
    let matchedVia: "exact" | "buffer" | null = hit ? "exact" : null;
    if (!hit && useBuffer) {
      hit = await findCity(r.latitude, r.longitude, true);
      if (hit) matchedVia = "buffer";
    }
    return { id: r.id, display_name: r.display_name, hit, matchedVia };
  });
  const results = await pLimit(tasks, CONCURRENCY);
  const matches = results.filter(r => r.hit);

  if (body.dry_run) {
    return json({
      dry_run:   true,
      processed: rows.length,
      matched:   matches.length,
      exact:     matches.filter(m => m.matchedVia === "exact").length,
      buffered:  matches.filter(m => m.matchedVia === "buffer").length,
      preview:   matches.slice(0, 50).map(m => ({
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

  return json({
    processed: rows.length,
    matched:   matches.length,
    updated,
    errors: writeErrors,
  });
});
