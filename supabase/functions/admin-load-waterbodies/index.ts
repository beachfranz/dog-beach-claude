// admin-load-waterbodies/index.ts
// Loads USGS NHD Waterbodies (Large Scale) into public.waterbodies.
// Filters at fetch time to:
//   - CA bounding box (approx. -124.5/32.5 → -114.1/42.0)
//   - FTYPE IN (390 Lake/Pond, 436 Reservoir) — excludes swamps,
//     streams, ice masses, estuaries
//   - AREASQKM > 0.1 — excludes farm ponds, decorative water features
// Result is ~3,145 polygons covering every CA lake/reservoir that
// could host a public beach.
//
// Paginated fetch (NHD maxRecordCount = 2000), batched upsert via
// load_waterbodies_batch RPC. Admin-secret gated.

import { createClient }  from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }   from "../_shared/cors.ts";
import { requireAdmin }  from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const NHD_BASE = "https://hydro.nationalmap.gov/arcgis/rest/services/nhd/MapServer/12/query";
const WHERE    = "FTYPE IN (390,436) AND AREASQKM > 0.1";
const PAGE     = 500;   // NHD 500s on 2000-record pages with full geometry
const BATCH    = 25;

function buildUrl(offset: number): string {
  const params = new URLSearchParams({
    where:             WHERE,
    geometry:          "-124.5,32.5,-114.1,42.0",
    geometryType:      "esriGeometryEnvelope",
    spatialRel:        "esriSpatialRelIntersects",
    inSR:              "4326",
    outFields:         "OBJECTID,PERMANENT_IDENTIFIER,GNIS_ID,GNIS_NAME,REACHCODE,FTYPE,FCODE,AREASQKM,ELEVATION",
    returnGeometry:    "true",
    outSR:             "4326",
    f:                 "geojson",
    resultOffset:      String(offset),
    resultRecordCount: String(PAGE),
  });
  return `${NHD_BASE}?${params}`;
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (b: unknown, s = 200) => new Response(JSON.stringify(b), { status: s, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // ── Paginate through NHD ────────────────────────────────────────────────
  let offset = 0;
  let allFeatures: unknown[] = [];
  let totalFetchChars = 0;

  while (true) {
    const url = buildUrl(offset);
    let text: string;
    try {
      const resp = await fetch(url);
      if (!resp.ok) return json({ error: `NHD HTTP ${resp.status} at offset ${offset}` }, 502);
      text = await resp.text();
    } catch (err) {
      return json({ error: `NHD fetch failed at offset ${offset}: ${(err as Error).message}` }, 502);
    }
    totalFetchChars += text.length;
    const page = JSON.parse(text);
    const features = page.features ?? [];
    allFeatures = allFeatures.concat(features);
    if (features.length < PAGE) break;  // last page
    offset += PAGE;
  }

  if (allFeatures.length === 0) {
    return json({ error: "NHD returned no features" }, 502);
  }

  // ── Batch upsert ────────────────────────────────────────────────────────
  let affected = 0;
  let skipped  = 0;
  const errors: string[] = [];

  for (let i = 0; i < allFeatures.length; i += BATCH) {
    const batch = allFeatures.slice(i, i + BATCH);
    const { data, error } = await supabase.rpc("load_waterbodies_batch", { p_features: batch });
    if (error) {
      errors.push(`batch ${i}–${i + batch.length}: ${error.message}`);
      continue;
    }
    const r = data as { affected?: number; skipped?: number };
    affected += r?.affected ?? 0;
    skipped  += r?.skipped  ?? 0;
  }

  return json({
    total:       allFeatures.length,
    affected,
    skipped,
    batches:     Math.ceil(allFeatures.length / BATCH),
    fetch_chars: totalFetchChars,
    errors,
  });
});
