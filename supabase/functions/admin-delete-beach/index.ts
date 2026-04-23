// admin-delete-beach/index.ts
// Hard-deletes a beaches row.
//
// FK behavior (verified 2026-04-22):
//   - beach_day_hourly_scores, beach_day_recommendations,
//     subscriber_locations, notification_log → ON DELETE CASCADE
//     (rows auto-removed)
//   - refresh_errors → ON DELETE SET NULL (error log rows retained,
//     pointer nulled)
// So a single DELETE FROM beaches is sufficient.
//
// Security model: same as admin-update-beach (obscure URL + service role
// server-side, no auth layer). See that file's header.
//
// POST { location_id: string }
// Returns { ok: true, deleted: location_id } or { error: string }

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
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: { location_id?: string };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const { location_id } = body;
  if (!location_id) return json({ error: "location_id required" }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // Snapshot the row before delete so the audit entry preserves what
  // was removed — the cascaded hourly/daily rows are gone too but the
  // beach row itself is the one that matters for recovery.
  const { data: beforeRow } = await supabase
    .from("beaches").select("*").eq("location_id", location_id).single();

  const { error, count } = await supabase
    .from("beaches")
    .delete({ count: "exact" })
    .eq("location_id", location_id);

  if (error) {
    await logAdminWrite(supabase, {
      functionName: "admin-delete-beach", action: "delete", req,
      locationId: location_id, before: beforeRow ?? null,
      success: false, error: error.message,
    });
    return json({ error: error.message }, 500);
  }
  if (count === 0) {
    await logAdminWrite(supabase, {
      functionName: "admin-delete-beach", action: "delete", req,
      locationId: location_id, before: null,
      success: false, error: "No row matched that location_id",
    });
    return json({ error: "No row matched that location_id" }, 404);
  }

  await logAdminWrite(supabase, {
    functionName: "admin-delete-beach", action: "delete", req,
    locationId: location_id, before: beforeRow ?? null, success: true,
  });

  return json({ ok: true, deleted: location_id });
});
