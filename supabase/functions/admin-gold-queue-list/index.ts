// admin-gold-queue-list/index.ts
// Returns the gold-set curation queue for a state, with light per-row
// metadata (current extraction value, sampled reason, beach name).
//
// GET ?state=MI[&status=pending|reviewed|skipped|all]
//
// Curator UI uses this to render the queue. For per-row alternatives /
// source quotes, the UI then opens admin-list-gold-candidates filtered to
// that fid+field (existing endpoint).
//
// Security: x-admin-secret + per-IP rate limit (requireAdmin).

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }  from "../_shared/cors.ts";
import { requireAdmin } from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req) => {
  const cors = corsHeaders(req, ["GET", "OPTIONS"]);
  if (req.method === "OPTIONS") return new Response(null, { headers: cors });

  const fail = await requireAdmin(req, cors);
  if (fail) return fail;

  const url = new URL(req.url);
  const state  = (url.searchParams.get("state") ?? "").toUpperCase();
  const status = url.searchParams.get("status") ?? "pending";

  if (!state) {
    return new Response(JSON.stringify({ error: "state required" }),
                        { status: 400, headers: cors });
  }

  const supa = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // Queue rows + light beach metadata join
  let q = supa
    .from("gold_set_curation_queue")
    .select(`
      id, state, gold_fid, field_name, sampled_at, sampled_reason,
      current_extraction_value, current_source, current_source_url,
      current_confidence, status, reviewed_at, reviewed_by,
      beach:beaches_gold!gold_fid (
        name, county_name, group_id, scoring_tier, location_id
      )
    `)
    .eq("state", state)
    .order("sampled_reason", { ascending: true })
    .order("gold_fid",       { ascending: true })
    .order("field_name",     { ascending: true });

  if (status !== "all") q = q.eq("status", status);

  const { data: rows, error } = await q;
  if (error) {
    return new Response(JSON.stringify({ error: error.message }),
                        { status: 500, headers: cors });
  }

  // Progress counts
  const { data: counts } = await supa.rpc("gold_set_queue_counts_for_state",
                                          { p_state: state });

  // Sign-off state
  const { data: signoff } = await supa
    .from("gold_set_signoff")
    .select("state, approved, approved_at, approved_by, rows_in_set, notes")
    .eq("state", state)
    .maybeSingle();

  // Gate readiness
  const { data: ready } = await supa.rpc("gold_set_curation_complete_for_state",
                                         { p_state: state });

  return new Response(JSON.stringify({
    state, status,
    counts: counts ?? null,
    signoff,
    gate_ready: ready === true,
    count: rows?.length ?? 0,
    queue: rows ?? [],
  }), { headers: cors });
});
