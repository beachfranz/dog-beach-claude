// admin-load-tribal-lands/index.ts
// Loads BIA American Indian & Alaska Native Land Area Representations,
// bbox-filtered to California (138 polygons), into public.tribal_lands.
// The source layer lacks a state field, so we filter via geometry
// envelope over CA's bounding box.

import { createClient }  from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }   from "../_shared/cors.ts";
import { requireAdmin }  from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// CA bounding box (lon/lat): west -124.5, south 32.5, east -114.1, north 42.0
const TRIBAL_URL =
  "https://services.arcgis.com/cJ9YHowT8TU7DUyn/ArcGIS/rest/services/" +
  "BND___American_Indian___Alaska_Native_Land_Area_Representations__BIA_/FeatureServer/1/query" +
  "?geometry=-124.5,32.5,-114.1,42.0" +
  "&geometryType=esriGeometryEnvelope" +
  "&spatialRel=esriSpatialRelIntersects" +
  "&inSR=4326" +
  "&outFields=*&returnGeometry=true&outSR=4326&f=geojson";

const BATCH_SIZE = 30;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (b: unknown, s = 200) => new Response(JSON.stringify(b), { status: s, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let collection: { features: unknown[] };
  let fetchChars = 0;
  try {
    const resp = await fetch(TRIBAL_URL);
    if (!resp.ok) return json({ error: `Tribal Lands HTTP ${resp.status}` }, 502);
    const text = await resp.text();
    fetchChars = text.length;
    collection = JSON.parse(text);
  } catch (err) {
    return json({ error: `Tribal Lands fetch failed: ${(err as Error).message}` }, 502);
  }
  const features = collection.features ?? [];
  if (!Array.isArray(features) || features.length === 0) {
    return json({ error: "Tribal Lands response had no features" }, 502);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  let totalAffected = 0;
  let totalSkipped  = 0;
  const errors: string[] = [];

  for (let i = 0; i < features.length; i += BATCH_SIZE) {
    const batch = features.slice(i, i + BATCH_SIZE);
    const { data, error } = await supabase.rpc("load_tribal_lands_batch", { p_features: batch });
    if (error) {
      errors.push(`batch ${i}–${i + batch.length}: ${error.message}`);
      continue;
    }
    const r = data as { affected?: number; skipped?: number };
    totalAffected += r?.affected ?? 0;
    totalSkipped  += r?.skipped  ?? 0;
  }

  return json({
    total:       features.length,
    affected:    totalAffected,
    skipped:     totalSkipped,
    batches:     Math.ceil(features.length / BATCH_SIZE),
    fetch_chars: fetchChars,
    errors,
  });
});
