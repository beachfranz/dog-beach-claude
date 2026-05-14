// admin-gold-queue-update-status/index.ts
// Flips a queue row's status (pending → reviewed/skipped) after curator
// has saved truth via admin-save-gold-truth (or chosen to skip the row).
//
// POST { queue_id: number, status: 'reviewed'|'skipped', reviewed_by?: string }
// Returns { ok: true, row }
//
// Security: x-admin-secret + per-IP rate limit (requireAdmin).

import { createClient }   from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }    from "../_shared/cors.ts";
import { requireAdmin }   from "../_shared/admin-auth.ts";
import { logAdminWrite }  from "../_shared/admin-audit.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req) => {
  const cors = corsHeaders(req, ["POST", "OPTIONS"]);
  if (req.method === "OPTIONS") return new Response(null, { headers: cors });

  const fail = await requireAdmin(req, cors);
  if (fail) return fail;

  let body: any;
  try { body = await req.json(); }
  catch { return new Response(JSON.stringify({ error: "invalid_json" }), { status: 400, headers: cors }); }

  const queue_id    = Number(body?.queue_id);
  const status      = String(body?.status ?? "").trim();
  const reviewed_by = body?.reviewed_by ? String(body.reviewed_by) : null;

  if (!Number.isFinite(queue_id) || queue_id <= 0
      || !["reviewed","skipped","pending"].includes(status)) {
    return new Response(JSON.stringify({
      error: "missing_or_invalid_fields",
      required: ["queue_id (number)", "status ('reviewed'|'skipped'|'pending')"],
    }), { status: 400, headers: cors });
  }

  const supa = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const update: any = { status };
  if (status === "pending") {
    update.reviewed_at = null;
    update.reviewed_by = null;
  } else {
    update.reviewed_at = new Date().toISOString();
    update.reviewed_by = reviewed_by;
  }

  const { data: row, error } = await supa
    .from("gold_set_curation_queue")
    .update(update)
    .eq("id", queue_id)
    .select()
    .single();

  if (error) {
    return new Response(JSON.stringify({ error: error.message }),
                        { status: 500, headers: cors });
  }

  await logAdminWrite(supa, {
    functionName: "admin-gold-queue-update-status",
    action: "update",
    req,
    locationId: String(row?.gold_fid ?? queue_id),
    before: null,
    after: { queue_id, status, reviewed_by },
    success: true,
  });

  return new Response(JSON.stringify({ ok: true, row }), { headers: cors });
});
