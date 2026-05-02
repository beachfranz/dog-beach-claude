// admin-set-gold-exclude/index.ts
// Toggle a beach's exclusion from a gold-set roster.
// Curator uses this to drop a beach from the curation queue without
// destroying its membership row (so we can un-exclude later).
//
// POST { set_name: string, fid: number, excluded: boolean, notes?: string }
// Returns { ok: true, row: <updated membership row> }
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
  catch {
    return new Response(JSON.stringify({ error: "invalid_json" }), { status: 400, headers: cors });
  }

  const set_name = String(body?.set_name ?? "").trim();
  const fid      = Number(body?.fid);
  const excluded = body?.excluded === true;
  const notes    = body?.notes ? String(body.notes) : null;

  if (!set_name || !Number.isFinite(fid) || fid <= 0) {
    return new Response(JSON.stringify({
      error: "missing_required_fields",
      required: ["set_name (string)", "fid (number)", "excluded (boolean)"],
    }), { status: 400, headers: cors });
  }

  const supa = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const update: any = { excluded };
  if (notes !== null) update.notes = notes;

  const { data: row, error } = await supa
    .from("gold_set_membership")
    .update(update)
    .eq("set_name", set_name)
    .eq("fid", fid)
    .select()
    .single();

  if (error) {
    return new Response(JSON.stringify({ error: error.message }), {
      status: 500, headers: cors,
    });
  }

  await logAdminWrite(supa, {
    functionName: "admin-set-gold-exclude",
    action: "update",
    req,
    locationId: String(fid),
    before: null,
    after: { set_name, fid, excluded, notes },
    success: true,
  });

  return new Response(JSON.stringify({ ok: true, row }), { headers: cors });
});
