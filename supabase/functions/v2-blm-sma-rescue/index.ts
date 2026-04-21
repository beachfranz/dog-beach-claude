// v2-blm-sma-rescue/index.ts
// Rescue stage — catches federal and private lands the primary polygon
// classifiers missed, using the BLM California Land Status Surface Management
// Agency service. This service is more comprehensive for California than the
// Esri USA_Federal_Lands aggregate, particularly for:
//   - BLM-managed units (King Range NCA, etc.) that USA_Federal_Lands omits
//   - BIA-administered tribal lands
//   - Private land (for marking non-public beaches invalid)
//
// Only processes records currently in county_default (avoids fighting with
// already-classified records).
//
// SMA_ID domain (from service metadata):
//   Federal: BLM=2, BIA=3, USFS=915, USDA=914, NPS=2012, FWS=1535,
//            DOD=2365, USBR=2366, USACE=2367, USMC=2370, NAVY=2371,
//            ARMY=488, USAF=305, USCG=4896, Other=2378
//   State=2386, Local=2387, Private=2388, Undetermined=1

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const BLM_URL = "https://gis.blm.gov/caarcgis/rest/services/lands/BLM_CA_LandStatus_SurfaceManagementAgency/FeatureServer/0/query";
const CONCURRENCY   = 10;

const SMA_CODE_NAME: Record<number, string> = {
  2: "BLM", 3: "BIA", 305: "USAF", 488: "ARMY", 914: "USDA", 915: "USFS",
  1535: "FWS", 2012: "NPS", 2365: "DOD", 2366: "USBR", 2367: "USACE",
  2370: "USMC", 2371: "NAVY", 2378: "Other Federal", 2386: "State",
  2387: "Local Gov", 2388: "Private", 4896: "USCG", 1: "Undetermined",
};

const FEDERAL_CODES = new Set([2, 3, 305, 488, 914, 915, 1535, 2012, 2365, 2366, 2367, 2370, 2371, 2378, 4896]);
const PRIVATE_CODE  = 2388;

async function findSMA(lat: number, lon: number): Promise<{ sma_id: number; agency: string; unit: string | null } | null> {
  const params = new URLSearchParams({
    geometry:       `${lon},${lat}`,
    geometryType:   "esriGeometryPoint",
    spatialRel:     "esriSpatialRelIntersects",
    inSR:           "4326",
    outFields:      "SMA_ID,Tmp_Text_ca",
    returnGeometry: "false",
    f:              "json",
  });
  try {
    const resp = await fetch(`${BLM_URL}?${params}`);
    if (!resp.ok) return null;
    const data = await resp.json();
    const features = data?.features ?? [];
    if (features.length === 0) return null;
    const attrs = features[0].attributes;
    const id = Number(attrs.SMA_ID);
    return {
      sma_id: id,
      agency: SMA_CODE_NAME[id] ?? `SMA_${id}`,
      unit:   attrs.Tmp_Text_ca ?? null,
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

  let body: { dry_run?: boolean } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("id, display_name, latitude, longitude")
    .eq("governing_body_source", "county_default")
    .not("latitude", "is", null)
    .not("longitude", "is", null);
  if (error) return json({ error: error.message }, 500);
  if (!rows?.length) return json({ processed: 0, matched: 0 });

  const tasks = rows.map(r => async () => ({
    id:           r.id,
    display_name: r.display_name,
    hit:          await findSMA(r.latitude, r.longitude),
  }));
  const results = await pLimit(tasks, CONCURRENCY);

  // Categorize
  const federal: typeof results = [];
  const privateLand: typeof results = [];
  const other: typeof results = [];
  for (const r of results) {
    if (!r.hit) continue;
    if (FEDERAL_CODES.has(r.hit.sma_id))    federal.push(r);
    else if (r.hit.sma_id === PRIVATE_CODE)  privateLand.push(r);
    else                                      other.push(r);
  }

  if (body.dry_run) {
    return json({
      dry_run:   true,
      processed: rows.length,
      federal:   federal.length,
      private:   privateLand.length,
      other:     other.length,
      preview_federal: federal.slice(0, 20).map(r => ({
        display_name: r.display_name, agency: r.hit!.agency, unit: r.hit!.unit,
      })),
      preview_private: privateLand.slice(0, 20).map(r => ({
        display_name: r.display_name,
      })),
      preview_other:   other.slice(0, 10).map(r => ({
        display_name: r.display_name, agency: r.hit!.agency, sma_id: r.hit!.sma_id,
      })),
    });
  }

  let updated = 0;
  const writeErrors: string[] = [];

  for (const r of federal) {
    const { error } = await supabase
      .from("beaches_staging_new")
      .update({
        governing_jurisdiction: "governing federal",
        governing_body:         r.hit!.unit ? `${r.hit!.agency} (${r.hit!.unit})` : r.hit!.agency,
        governing_body_source:  "blm_sma_federal",
        governing_body_notes:   `Beach falls within federal land per BLM CA Surface Management Agency (SMA_ID=${r.hit!.sma_id} = ${r.hit!.agency}${r.hit!.unit ? `, ${r.hit!.unit}` : ""}).`,
        review_status:          "ready",
        review_notes:           "Rescued to federal via BLM CA SMA service — caught gap in primary federal polygon.",
      })
      .eq("id", r.id);
    if (error) writeErrors.push(`id ${r.id}: ${error.message}`);
    else updated++;
  }

  for (const r of privateLand) {
    const { error } = await supabase
      .from("beaches_staging_new")
      .update({
        review_status: "invalid",
        review_notes:  "Private land per BLM CA Surface Management Agency (SMA_ID=2388).",
      })
      .eq("id", r.id);
    if (error) writeErrors.push(`id ${r.id}: ${error.message}`);
    else updated++;
  }

  return json({
    processed: rows.length,
    federal:   federal.length,
    private:   privateLand.length,
    other:     other.length,
    updated,
    errors:    writeErrors,
  });
});
