// admin-mark-beach-inactive/index.ts
// Mark a us_beach_points row as inactive — typically used when orphan_geocode
// review concludes the beach isn't a real locatable beach (data-scrape
// artifact, wrong-named business, etc.). The row is preserved for history
// but excluded from active pipeline queries.
//
// Also clears any orphan_geocode flag so the beach doesn't re-appear in
// review. Logs to admin_audit with __resolution_mode='marked_inactive'.
//
// POST { fid: number }

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

  let body: { fid?: number; reason?: string };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }
  const fid    = body.fid;
  const reason = typeof body.reason === "string" && body.reason.length > 0 ? body.reason : null;
  if (typeof fid !== "number") return json({ error: "fid (number) required" }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data, error } = await supabase.rpc("mark_beach_inactive", {
    p_fid:    fid,
    p_reason: reason,
  });
  if (error) return json({ error: error.message }, 500);

  const pair = (data ?? [])[0];
  if (pair) {
    await logAdminWrite(supabase, {
      functionName: "admin-mark-beach-inactive",
      action:       "update",
      req,
      before:       pair.before,
      after:        { ...pair.after, __resolution_mode: "marked_inactive" },
      success:      true,
    });
  }

  return json({ ok: true, fid, ...(pair ?? {}) });
});
