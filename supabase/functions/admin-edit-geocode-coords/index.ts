// admin-edit-geocode-coords/index.ts
// Admin manually enters correct coordinates for a flagged beach. Updates
// geom, clears the flag, and logs with __resolution_mode='geocode_edited'.

import { createClient }   from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }    from "../_shared/cors.ts";
import { requireAdmin }   from "../_shared/admin-auth.ts";
import { logAdminWrite }  from "../_shared/admin-audit.ts";

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

  let body: { fid?: number; lat?: number; lon?: number };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }
  const { fid, lat, lon } = body;
  if (typeof fid !== "number") return json({ error: "fid (number) required" }, 400);
  if (typeof lat !== "number") return json({ error: "lat (number) required" }, 400);
  if (typeof lon !== "number") return json({ error: "lon (number) required" }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data, error } = await supabase.rpc("edit_geocode_coords",
    { p_fid: fid, p_lat: lat, p_lon: lon });
  if (error) return json({ error: error.message }, 500);

  const pair = (data ?? [])[0];
  if (pair) {
    await logAdminWrite(supabase, {
      functionName: "admin-edit-geocode-coords",
      action:       "update",
      req,
      before:       pair.before,
      after:        { ...pair.after, __resolution_mode: "geocode_edited" },
      success:      true,
    });
  }

  return json({ ok: true, fid, ...(pair ?? {}) });
});
