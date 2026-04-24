// admin-get-dupe-cluster/index.ts
// Returns every us_beach_points row in a given duplicate cluster, with full
// per-row provenance from every authority + enrichment source.
//
// The dedupe-review UI uses this to show rows side-by-side with evidence,
// so the admin can pick canonical from "which row has richer metadata"
// (per project_dedupe_canonical_rule.md).
//
// Thin wrapper — all the work lives in the get_dupe_cluster() RPC
// (see migration 20260424_dupe_cluster_rpcs.sql).
//
// POST { cluster_id: number }
// Returns { cluster_id, rows: [ { fid, name, state, ..., cpad: [...], ccc: [...], ... } ] }

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

  let body: { cluster_id?: number };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const cluster_id = body.cluster_id;
  if (typeof cluster_id !== "number")
    return json({ error: "cluster_id (number) required" }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data, error } = await supabase
    .rpc("get_dupe_cluster", { p_cluster_id: cluster_id });

  if (error) return json({ error: error.message }, 500);

  // RPC returns an array of { data: <row-jsonb> }. Unwrap.
  const rows = (data ?? []).map((r: { data: unknown }) => r.data);

  return json({ cluster_id, rows });
});
