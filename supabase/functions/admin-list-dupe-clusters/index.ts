// admin-list-dupe-clusters/index.ts
// Returns a summary row per pinned duplicate cluster for the picker list
// in the dedupe-review UI. Sorted auto-resolvable first, then CA, then by
// cluster_id. See migration 20260424_dupe_cluster_rpcs.sql for the RPC.
//
// POST (no body) — returns { clusters: [ { cluster_id, row_count, states, counties, names, auto_resolvable, min_fid } ] }

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

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  const { data, error } = await supabase.rpc("list_dupe_clusters");
  if (error) return json({ error: error.message }, 500);

  const clusters = (data ?? []).map((r: { data: unknown }) => r.data);
  return json({ clusters });
});
