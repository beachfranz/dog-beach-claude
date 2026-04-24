// admin-dismiss-geocode/index.ts
// Admin dismisses an orphan_geocode flag: the beach is actually at these
// coordinates, don't re-flag it. Sets geocode_admin_confirmed = true for
// permanent suppression. Logs with __resolution_mode='geocode_dismissed'.

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

  let body: { fid?: number };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }
  const fid = body.fid;
  if (typeof fid !== "number") return json({ error: "fid (number) required" }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data, error } = await supabase.rpc("dismiss_geocode_flag", { p_fid: fid });
  if (error) return json({ error: error.message }, 500);

  const pair = (data ?? [])[0];
  if (pair) {
    await logAdminWrite(supabase, {
      functionName: "admin-dismiss-geocode",
      action:       "update",
      req,
      before:       pair.before,
      after:        { ...pair.after, __resolution_mode: "geocode_dismissed" },
      success:      true,
    });
  }

  return json({ ok: true, fid, ...(pair ?? {}) });
});
