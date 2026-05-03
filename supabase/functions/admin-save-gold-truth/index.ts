// admin-save-gold-truth/index.ts
// Saves a curator-verified truth value to public.beach_policy_gold_set.
// Lets the curator UI write directly without a manual SQL step.
//
// POST {
//   fid: number,                       // arena head fid (beaches_gold.fid)
//   field_name: string,                // e.g. 'has_sections', 'feature_zones'
//   verified_value?: string | null,    // scalar truth (text/enum)
//   truth_value_json?: object | null,  // structured truth (sections[], etc.)
//   source_url?: string | null,
//   notes?: string | null,
//   verified_by?: string | null
// }
//
// Behaviour:
//   - One of verified_value or truth_value_json must be provided. Both is fine
//     for fields where you want the json blob plus a human-readable summary.
//   - Upsert key is (arena_group_id, field_name). Re-saving overwrites prior
//     truth for that (beach, field).
//   - To delete a truth row, send { fid, field_name, verified_value: null,
//     truth_value_json: null, _delete: true }. We keep delete explicit so a
//     stray empty save doesn't wipe state.
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
  try {
    body = await req.json();
  } catch {
    return new Response(JSON.stringify({ error: "invalid_json" }), {
      status: 400, headers: cors,
    });
  }

  const fid = Number(body?.fid);
  const field_name = String(body?.field_name ?? "").trim();
  if (!Number.isFinite(fid) || fid <= 0 || !field_name) {
    return new Response(JSON.stringify({
      error: "missing_required_fields",
      required: ["fid (number)", "field_name (string)"],
    }), { status: 400, headers: cors });
  }

  const supa = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // Confirm the beach exists (and grab fid passthrough — beach_policy_gold_set
  // has both fid and arena_group_id; in the post-path-3 world they're equal,
  // but keep both populated for back-compat).
  const { data: beach, error: bErr } = await supa
    .from("beaches_gold")
    .select("fid, name")
    .eq("fid", fid)
    .single();
  if (bErr || !beach) {
    return new Response(JSON.stringify({ error: "beach_not_found", fid }), {
      status: 404, headers: cors,
    });
  }

  // Explicit delete path
  if (body._delete === true) {
    const { error: dErr } = await supa
      .from("beach_policy_gold_set")
      .delete()
      .eq("arena_group_id", fid)
      .eq("field_name", field_name);
    if (dErr) {
      return new Response(JSON.stringify({ error: dErr.message }), {
        status: 500, headers: cors,
      });
    }
    await logAdminWrite(supa, {
      functionName: "admin-save-gold-truth",
      action: "delete",
      req,
      locationId: String(fid),
      before: { fid, field_name },
      after: null,
      success: true,
    });
    return new Response(JSON.stringify({
      ok: true, deleted: true, fid, field_name,
    }), { headers: cors });
  }

  const verified_value =
    body.verified_value === undefined ? null
    : body.verified_value === null ? null
    : String(body.verified_value);

  const truth_value_json =
    body.truth_value_json === undefined ? null
    : body.truth_value_json;  // pass through whatever JSON the curator sends

  if (verified_value === null && truth_value_json === null) {
    return new Response(JSON.stringify({
      error: "empty_truth",
      hint: "send either verified_value (text) or truth_value_json (object), or set _delete: true to remove the row",
    }), { status: 400, headers: cors });
  }

  const verified_by = (body.verified_by ?? "curator").toString().slice(0, 80);
  const source_url  = body.source_url ? String(body.source_url) : null;
  const notes       = body.notes ? String(body.notes) : null;
  const curator_confidence = (body.curator_confidence === "high" ||
                              body.curator_confidence === "medium" ||
                              body.curator_confidence === "low")
                             ? body.curator_confidence : null;

  // Upsert — psql doesn't have a REST equivalent for "ON CONFLICT
  // (arena_group_id, field_name) DO UPDATE", so do it as delete + insert
  // inside a single round-trip via the service-role client.
  const { error: delErr } = await supa
    .from("beach_policy_gold_set")
    .delete()
    .eq("arena_group_id", fid)
    .eq("field_name", field_name);
  if (delErr) {
    return new Response(JSON.stringify({ error: delErr.message }), {
      status: 500, headers: cors,
    });
  }

  const { data: inserted, error: insErr } = await supa
    .from("beach_policy_gold_set")
    .insert({
      fid,
      arena_group_id: fid,
      field_name,
      verified_value,
      truth_value_json,
      source_url,
      notes,
      verified_by,
      curator_confidence,
    })
    .select()
    .single();
  if (insErr) {
    return new Response(JSON.stringify({ error: insErr.message }), {
      status: 500, headers: cors,
    });
  }

  await logAdminWrite(supa, {
    functionName: "admin-save-gold-truth",
    action: "update",  // semantic: upsert; audit table only allows create/update/delete
    req,
    locationId: String(fid),
    before: null,
    after: { fid, field_name, has_json: truth_value_json !== null,
             verified_value: verified_value },
    success: true,
  });

  return new Response(JSON.stringify({ ok: true, row: inserted }), {
    headers: cors,
  });
});
