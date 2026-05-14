// admin-gold-set-approve/index.ts
// Writes the gold_set_signoff row that unblocks gold_set_review_gate in
// Dagster. Curator clicks "approve for production" once they've worked
// through enough of the queue to vouch for the gold-set.
//
// POST { state: string, approved: boolean, approved_by?: string, notes?: string }
// Returns { ok: true, row, gate_ready: boolean }
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

  const state       = String(body?.state ?? "").trim().toUpperCase();
  const approved    = body?.approved === true;
  const approved_by = body?.approved_by ? String(body.approved_by) : null;
  const notes       = body?.notes ? String(body.notes) : null;

  if (!state || state.length !== 2) {
    return new Response(JSON.stringify({
      error: "state required (2-letter code)",
    }), { status: 400, headers: cors });
  }

  const supa = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // Count gold rows for state for the signoff record
  const { data: cntRows } = await supa.rpc("gold_set_queue_counts_for_state",
                                           { p_state: state });
  const total_in_queue = (cntRows && cntRows[0]?.total) ?? 0;

  const row = {
    state,
    approved,
    approved_at: approved ? new Date().toISOString() : null,
    approved_by,
    rows_in_set: total_in_queue,
    notes,
  };

  // upsert by state
  const { data: existing } = await supa
    .from("gold_set_signoff")
    .select("id")
    .eq("state", state)
    .maybeSingle();

  let result;
  if (existing?.id) {
    result = await supa
      .from("gold_set_signoff")
      .update(row)
      .eq("id", existing.id)
      .select()
      .single();
  } else {
    result = await supa
      .from("gold_set_signoff")
      .insert(row)
      .select()
      .single();
  }

  if (result.error) {
    return new Response(JSON.stringify({ error: result.error.message }),
                        { status: 500, headers: cors });
  }

  const { data: gate_ready } = await supa.rpc(
    "gold_set_curation_complete_for_state", { p_state: state }
  );

  await logAdminWrite(supa, {
    functionName: "admin-gold-set-approve",
    action: existing?.id ? "update" : "insert",
    req,
    locationId: state,
    before: null,
    after: row,
    success: true,
  });

  return new Response(JSON.stringify({
    ok: true,
    row: result.data,
    gate_ready: gate_ready === true,
  }), { headers: cors });
});
