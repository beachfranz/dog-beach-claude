// admin-classify-points/index.ts
// Thin admin-authed wrapper around classify_points_to_state RPC. Takes
// an array of { fid, latitude, longitude } and returns each one tagged
// with the nearest state_code plus distance in meters.
//
// Used by the scripts/add_state_to_csv.py one-off to enrich
// US_beaches.csv with a STATE column.
//
// POST { points: [{fid, latitude, longitude}, ...] }
// Returns { classifications: [{fid, state_code, distance_m}, ...] }

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

  let body: { points?: Array<{ fid: unknown; latitude: number; longitude: number }> };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const points = body.points;
  if (!Array.isArray(points) || points.length === 0) {
    return json({ error: "points array required (non-empty)" }, 400);
  }
  if (points.length > 5000) {
    return json({ error: "batch too large (max 5000)" }, 400);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  const { data, error } = await supabase.rpc("classify_points_to_state", { p_points: points });
  if (error) return json({ error: `RPC failed: ${error.message}` }, 500);

  return json({ classifications: data ?? [] });
});
