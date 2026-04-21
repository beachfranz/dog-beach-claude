// csp-polygon-check/index.ts
// Point-in-polygon match against California State Park boundary polygons.
//
// Fills the gap where our CSP ArcGIS entry points (csp_places) only list
// formal park entry points, not the full park extent. Beaches like those
// along the Big Sur coast sit inside Pfeiffer-Burns, Garrapata, Andrew Molera,
// or Julia Pfeiffer Burns State Park boundaries but do not appear in the
// entry points feed.
//
// Queries the CA State Parks ParkBoundaries FeatureServer live — no cache —
// once per beach. ~460 park polygons total.
//
// Only processes rows where:
//   review_status IS NULL
//   governing_jurisdiction != 'governing state' (already state, nothing to fix)
//   governing_jurisdiction != 'governing federal' (federal takes priority)
//
// Confirmed matches set:
//   governing_jurisdiction = 'governing state'
//   governing_body         = <park name>
//   governing_body_source  = 'csp_polygon'
//   review_status          = 'ready'
//
// POST { state?: string, county?: string, limit?: number, dry_run?: boolean }
// Returns { processed, matched, updated, errors }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const PARK_BOUNDARY_URL = "https://services2.arcgis.com/AhxrK3F6WM8ECvDi/arcgis/rest/services/ParkBoundaries/FeatureServer/0/query";
const CONCURRENCY       = 10;
const DEFAULT_LIMIT     = 1000;

// ── Query a single point against the park boundaries service ─────────────────

async function findPark(lat: number, lon: number): Promise<{ name: string; subtype: string } | null> {
  const params = new URLSearchParams({
    geometry:       `${lon},${lat}`,
    geometryType:   "esriGeometryPoint",
    spatialRel:     "esriSpatialRelIntersects",
    inSR:           "4326",
    outFields:      "UNITNAME,SUBTYPE",
    returnGeometry: "false",
    f:              "json",
  });

  try {
    const resp = await fetch(`${PARK_BOUNDARY_URL}?${params}`);
    if (!resp.ok) return null;
    const data = await resp.json();
    const features = data?.features ?? [];
    if (features.length === 0) return null;
    const attrs = features[0].attributes ?? {};
    return {
      name:    String(attrs.UNITNAME ?? ""),
      subtype: String(attrs.SUBTYPE ?? ""),
    };
  } catch {
    return null;
  }
}

// ── Concurrency limiter ───────────────────────────────────────────────────────

async function pLimit<T>(tasks: (() => Promise<T>)[], limit: number): Promise<T[]> {
  const results: T[] = new Array(tasks.length);
  let index = 0;
  async function worker() {
    while (index < tasks.length) {
      const i = index++;
      results[i] = await tasks[i]();
    }
  }
  await Promise.all(Array.from({ length: limit }, worker));
  return results;
}

// ── Handler ───────────────────────────────────────────────────────────────────

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { state?: string; county?: string; limit?: number; dry_run?: boolean } = {};
  try { body = await req.json(); } catch { /* empty body */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // ── Fetch candidate rows ────────────────────────────────────────────────────
  let query = supabase
    .from("beaches_staging_new")
    .select("id, display_name, latitude, longitude, governing_jurisdiction, county")
    .is("review_status", null)
    .neq("governing_jurisdiction", "governing state")
    .neq("governing_jurisdiction", "governing federal")
    .not("latitude", "is", null)
    .not("longitude", "is", null)
    .limit(body.limit ?? DEFAULT_LIMIT);

  if (body.state)  query = query.eq("state", body.state);
  if (body.county) query = query.eq("county", body.county);

  const { data: rows, error: fetchError } = await query;
  if (fetchError) return json({ error: fetchError.message }, 500);
  if (!rows?.length) return json({ processed: 0, matched: 0, updated: 0, errors: [] });

  // ── Run point-in-polygon for each row ───────────────────────────────────────
  const tasks = rows.map(row => async () => {
    const hit = await findPark(row.latitude, row.longitude);
    return {
      id:           row.id,
      display_name: row.display_name,
      county:       row.county,
      park:         hit?.name ?? null,
      subtype:      hit?.subtype ?? null,
    };
  });

  const results = await pLimit(tasks, CONCURRENCY);
  const matches = results.filter(r => r.park);

  if (body.dry_run) {
    return json({
      dry_run:   true,
      processed: rows.length,
      matched:   matches.length,
      preview:   matches.slice(0, 50).map(m => ({
        display_name: m.display_name,
        park:         m.park,
        subtype:      m.subtype,
        county:       m.county,
      })),
    });
  }

  // ── Write confirmed matches ─────────────────────────────────────────────────
  const writeErrors: string[] = [];
  let updated = 0;

  const writeTasks = matches.map(m => async () => {
    const { error } = await supabase
      .from("beaches_staging_new")
      .update({
        governing_jurisdiction: "governing state",
        governing_body:         m.park,
        governing_body_source:  "csp_polygon",
        governing_body_notes:   `Beach falls within ${m.park} boundary polygon (${m.subtype}).`,
        review_status:          "ready",
        review_notes:           "Governing jurisdiction confirmed state via CSP park boundary polygon match.",
      })
      .eq("id", m.id);
    if (error) writeErrors.push(`id ${m.id}: ${error.message}`);
    else updated++;
  });

  await pLimit(writeTasks, 10);

  return json({
    processed:  rows.length,
    matched:    matches.length,
    updated,
    errors:     writeErrors,
    preview:    matches.slice(0, 30).map(m => ({
      display_name: m.display_name,
      park:         m.park,
      county:       m.county,
    })),
  });
});
