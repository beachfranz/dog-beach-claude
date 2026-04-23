// admin-load-csp-parks/index.ts
// Loads the full California State Parks ParkBoundaries layer (462
// polygons) into public.csp_parks. Single fetch, batched upsert.

import { createClient }  from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }   from "../_shared/cors.ts";
import { requireAdmin }  from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const CSP_URL =
  "https://services2.arcgis.com/AhxrK3F6WM8ECvDi/arcgis/rest/services/" +
  "ParkBoundaries/FeatureServer/0/query" +
  "?where=1%3D1&outFields=*&returnGeometry=true&outSR=4326&f=geojson";

const BATCH_SIZE = 25;  // Some state parks are large (Anza-Borrego has huge perimeter)

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
    const resp = await fetch(CSP_URL);
    if (!resp.ok) return json({ error: `CSP HTTP ${resp.status}` }, 502);
    const text = await resp.text();
    fetchChars = text.length;
    collection = JSON.parse(text);
  } catch (err) {
    return json({ error: `CSP fetch failed: ${(err as Error).message}` }, 502);
  }
  const features = collection.features ?? [];
  if (!Array.isArray(features) || features.length === 0) {
    return json({ error: "CSP response had no features" }, 502);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  let totalAffected = 0;
  let totalSkipped  = 0;
  const errors: string[] = [];

  for (let i = 0; i < features.length; i += BATCH_SIZE) {
    const batch = features.slice(i, i + BATCH_SIZE);
    const { data, error } = await supabase.rpc("load_csp_parks_batch", { p_features: batch });
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
