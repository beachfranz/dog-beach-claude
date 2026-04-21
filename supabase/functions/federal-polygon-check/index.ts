// federal-polygon-check/index.ts
// Point-in-polygon match against US federal land boundaries.
// Catches beaches inside NPS, USFS, BLM, DOD, FWS, and USBR units that were
// not caught by our NPS places proximity match.
//
// Data source: Esri Living Atlas "USA Federal Lands" unified feature service.
// Single endpoint covering all six federal land-holding agencies with an
// Agency text field and unit_name field.
//
// Only processes rows where:
//   review_status IS NULL
//   governing_jurisdiction != 'governing federal' (already federal)
//
// Federal match takes priority over state/city — if we find a federal hit,
// we flip to federal regardless of current classification.
//
// Confirmed matches set:
//   governing_jurisdiction = 'governing federal'
//   governing_body         = <unit name>
//   governing_body_source  = 'federal_polygon'
//   review_status          = 'ready'
//
// POST { state?: string, county?: string, limit?: number, dry_run?: boolean }
// Returns { processed, matched, updated, errors, preview }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const FEDERAL_URL   = "https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/USA_Federal_Lands/FeatureServer/0/query";
const CONCURRENCY   = 10;
const DEFAULT_LIMIT = 1000;

// ── Query point against federal lands service ────────────────────────────────

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
    const attrs = features[0].attributes ?? {};
    return {
      agency: String(attrs.Agency ?? ""),
      unit:   String(attrs.unit_name ?? ""),
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
    const hit = await findFederalUnit(row.latitude, row.longitude);
    return {
      id:           row.id,
      display_name: row.display_name,
      county:       row.county,
      agency:       hit?.agency ?? null,
      unit:         hit?.unit ?? null,
    };
  });

  const results = await pLimit(tasks, CONCURRENCY);

  // Mixed-management units: polygon boundary encompasses land owned/operated by
  // state, county, and city entities as well as federal. Auto-locking these
  // produces false positives — skip and leave for manual review.
  const MIXED_MANAGEMENT_UNITS = new Set([
    "Santa Monica Mountains National Recreation Area",
  ]);
  const matches = results.filter(r => r.unit && !MIXED_MANAGEMENT_UNITS.has(r.unit));

  if (body.dry_run) {
    return json({
      dry_run:   true,
      processed: rows.length,
      matched:   matches.length,
      preview:   matches.map(m => ({
        display_name: m.display_name,
        agency:       m.agency,
        unit:         m.unit,
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
        governing_jurisdiction: "governing federal",
        governing_body:         m.unit,
        governing_body_source:  "federal_polygon",
        governing_body_notes:   `Beach falls within ${m.unit} (${m.agency}) boundary polygon.`,
        review_status:          "ready",
        review_notes:           `Governing jurisdiction confirmed federal via ${m.agency} boundary polygon match.`,
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
    preview:    matches.map(m => ({
      display_name: m.display_name,
      agency:       m.agency,
      unit:         m.unit,
    })),
  });
});
