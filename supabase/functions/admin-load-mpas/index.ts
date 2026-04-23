// admin-load-mpas/index.ts
// Loads CDFW Marine Protected Areas (ds582) from the CA Open Data
// ArcGIS endpoint into public.mpas. 155 polygons statewide — single
// fetch, batch upsert via load_mpas_batch RPC.

import { createClient }  from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }   from "../_shared/cors.ts";
import { requireAdmin }  from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const MPA_URL =
  "https://services2.arcgis.com/Uq9r85Potqm3MfRV/arcgis/rest/services/" +
  "biosds582_fpu/FeatureServer/0/query" +
  "?where=1%3D1&outFields=*&returnGeometry=true&outSR=4326&f=geojson";

const BATCH_SIZE = 25;  // MPAs can be large polygons along coastline — keep batches small

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
    const resp = await fetch(MPA_URL);
    if (!resp.ok) return json({ error: `MPA HTTP ${resp.status}` }, 502);
    const text = await resp.text();
    fetchChars = text.length;
    collection = JSON.parse(text);
  } catch (err) {
    return json({ error: `MPA fetch failed: ${(err as Error).message}` }, 502);
  }
  const features = collection.features ?? [];
  if (!Array.isArray(features) || features.length === 0) {
    return json({ error: "MPA response had no features" }, 502);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  let totalAffected = 0;
  let totalSkipped  = 0;
  const errors: string[] = [];

  for (let i = 0; i < features.length; i += BATCH_SIZE) {
    const batch = features.slice(i, i + BATCH_SIZE);
    const { data, error } = await supabase.rpc("load_mpas_batch", { p_features: batch });
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
