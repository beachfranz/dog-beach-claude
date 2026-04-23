// admin-load-ccc/index.ts
// Loads California Coastal Commission Public Access Points from CCC's
// ArcGIS FeatureServer into public.ccc_access_points. One-shot admin
// endpoint — ~1,631 features fit comfortably in a single ArcGIS fetch
// (maxRecordCount=2000), then uploaded in chunks via load_ccc_batch RPC.
//
// Security model: admin-secret gated. Idempotent — safe to re-run when
// CCC updates the dataset.
//
// POST {}
// Returns { total, affected, skipped, batches, fetch_chars, errors }

import { createClient }  from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }   from "../_shared/cors.ts";
import { requireAdmin }  from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const CCC_URL =
  "https://services9.arcgis.com/wwVnNW92ZHUIr0V0/arcgis/rest/services/" +
  "AccessPoints/FeatureServer/0/query" +
  "?where=1%3D1" +
  "&outFields=*" +
  "&returnGeometry=true" +
  "&outSR=4326" +
  "&f=geojson";

const BATCH_SIZE = 100;  // RPC handles 100 points easily; small so request payload stays under 1MB

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (b: unknown, s = 200) => new Response(JSON.stringify(b), { status: s, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  // ── Fetch ───────────────────────────────────────────────────────────────
  let collection: { features: unknown[] };
  let fetchChars = 0;
  try {
    const resp = await fetch(CCC_URL);
    if (!resp.ok) return json({ error: `CCC ArcGIS HTTP ${resp.status}` }, 502);
    const text = await resp.text();
    fetchChars = text.length;
    collection = JSON.parse(text);
  } catch (err) {
    return json({ error: `CCC fetch failed: ${(err as Error).message}` }, 502);
  }
  const features = collection.features ?? [];
  if (!Array.isArray(features) || features.length === 0) {
    return json({ error: "CCC returned no features" }, 502);
  }

  // ── Upsert in batches via RPC ───────────────────────────────────────────
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  let totalAffected = 0;
  let totalSkipped  = 0;
  const errors: string[] = [];

  for (let i = 0; i < features.length; i += BATCH_SIZE) {
    const batch = features.slice(i, i + BATCH_SIZE);
    const { data, error } = await supabase.rpc("load_ccc_batch", { p_features: batch });
    if (error) {
      errors.push(`batch ${i}–${i + batch.length}: ${error.message}`);
      continue;
    }
    const result = data as { affected?: number; skipped?: number };
    totalAffected += result?.affected ?? 0;
    totalSkipped  += result?.skipped  ?? 0;
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
