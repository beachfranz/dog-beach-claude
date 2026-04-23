// admin-load-cpad-batch/index.ts
// Admin-authed thin wrapper around the load_cpad_batch RPC. The local
// Python driver (scripts/load_cpad.py) fetches CPAD features paginated
// from the CNRA ArcGIS FeatureServer and posts each batch here; we
// upsert via the SECURITY DEFINER RPC so the service role actually
// owns the writes.
//
// POST { features: [{ type: "Feature", properties: {...}, geometry: {...} }, ...] }
// Returns { total, inserted, updated, skipped }

import { createClient }  from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }   from "../_shared/cors.ts";
import { requireAdmin }  from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (b: unknown, s = 200) => new Response(JSON.stringify(b), { status: s, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: { features?: unknown[] };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const features = body.features;
  if (!Array.isArray(features) || features.length === 0) {
    return json({ error: "features array required (non-empty)" }, 400);
  }
  if (features.length > 500) {
    return json({ error: "batch too large (max 500 features per call)" }, 400);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  const { data, error } = await supabase.rpc("load_cpad_batch", { p_features: features });
  if (error) return json({ error: `RPC failed: ${error.message}` }, 500);

  return json(data);
});
