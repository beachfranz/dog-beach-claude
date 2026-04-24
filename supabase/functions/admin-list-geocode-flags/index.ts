// admin-list-geocode-flags/index.ts
// Returns all orphan_geocode flagged beaches for a given state with the
// candidate polygon's centroid + closest-point-to-beach coords for map
// display in admin/geocode-review.html.
//
// POST { state: 'CA' } — defaults to CA per current scoping rule
// Returns { flags: [ { fid, name, beach_lat, beach_lon, tier, candidate_name,
//                      candidate_centroid, candidate_snap_point, distance_m, ... } ] }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }  from "../_shared/cors.ts";
import { requireAdmin } from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")    return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: { state?: string } = {};
  try { body = await req.json(); } catch { /* empty body OK */ }
  const stateCode = typeof body.state === "string" && body.state.length > 0 ? body.state : "CA";

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data, error } = await supabase
    .rpc("list_orphan_geocode_flags", { p_state: stateCode });
  if (error) return json({ error: error.message }, 500);

  const flags = (data ?? []).map((r: { data: unknown }) => r.data);
  return json({ state: stateCode, flags });
});
