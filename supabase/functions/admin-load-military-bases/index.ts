// admin-load-military-bases/index.ts
// Loads DoD Military Installations from the USA Military Bases layer,
// filtered to California (STATE_TERR = 'California' → 89 polygons),
// into public.military_bases.

import { createClient }  from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }   from "../_shared/cors.ts";
import { requireAdmin }  from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const MIL_URL =
  "https://services.arcgis.com/hRUr1F8lE8Jq2uJo/ArcGIS/rest/services/" +
  "milbases/FeatureServer/0/query" +
  "?where=STATE_TERR%3D%27California%27" +
  "&outFields=*" +
  "&returnGeometry=true" +
  "&outSR=4326" +
  "&f=geojson";

const BATCH_SIZE = 20;  // Camp Pendleton + Vandenberg are huge polygons — keep batches small

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
    const resp = await fetch(MIL_URL);
    if (!resp.ok) return json({ error: `Military Bases HTTP ${resp.status}` }, 502);
    const text = await resp.text();
    fetchChars = text.length;
    collection = JSON.parse(text);
  } catch (err) {
    return json({ error: `Military Bases fetch failed: ${(err as Error).message}` }, 502);
  }
  const features = collection.features ?? [];
  if (!Array.isArray(features) || features.length === 0) {
    return json({ error: "Military Bases response had no features" }, 502);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  let totalAffected = 0;
  let totalSkipped  = 0;
  const errors: string[] = [];

  for (let i = 0; i < features.length; i += BATCH_SIZE) {
    const batch = features.slice(i, i + BATCH_SIZE);
    const { data, error } = await supabase.rpc("load_military_bases_batch", { p_features: batch });
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
