// admin-bulk-resolve-identical-dupes/index.ts
// The "Process All" endpoint. Auto-resolves every duplicate cluster where
// all rows are ST_Equals AND have identical trim+lower name. Canonical =
// lowest fid in the cluster; the others become 'duplicate'.
//
// Scope is deliberately narrow — only the clusters where "pick canonical"
// is genuinely arbitrary on data alone. Clusters with any name variation
// or coord delta stay on needs_review for human review.
//
// POST {}                         — all states
// POST { state: 'CA' }             — scope to a single state
// POST { dry_run: true, state? }   — preview without writing
//
// Returns {
//   ok: true,
//   state:             <string|null>,
//   clusters_resolved: <int>,
//   rows_affected:     <int>,
//   by_cluster:        [ { cluster_id, canonical_fid, duplicate_fids } ],
//   dry_run:           <bool>
// }

import { createClient }   from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }    from "../_shared/cors.ts";
import { requireAdmin }   from "../_shared/admin-auth.ts";
import { logAdminWrite }  from "../_shared/admin-audit.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

type ResolvedRow = {
  cluster_id: number;
  before: { fid: number; duplicate_status: string | null };
  after:  { fid: number; duplicate_status: string | null };
};

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")    return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: { dry_run?: boolean; state?: string | null } = {};
  try { body = await req.json(); } catch { /* empty body OK */ }
  const dryRun = body.dry_run === true;
  const stateFilter = typeof body.state === "string" && body.state.length > 0 ? body.state : null;

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  if (dryRun) {
    // Preview: read which clusters qualify without mutating anything. Uses
    // a direct SQL query that mirrors the filter in the RPC so the count is
    // identical to what a real run would produce.
    const { data, error } = await supabase.rpc("list_dupe_clusters");
    if (error) return json({ error: error.message }, 500);
    const clusters = (data ?? [])
      .map((r: { data: { cluster_id: number; auto_resolvable: boolean; row_count: number; min_fid: number; names: string[]; states: string[] } }) => r.data)
      .filter((c) => c.auto_resolvable)
      .filter((c) => stateFilter === null || (c.states ?? []).includes(stateFilter));
    return json({
      ok:                true,
      state:             stateFilter,
      dry_run:           true,
      clusters_resolved: clusters.length,
      rows_affected:     clusters.reduce((s, c) => s + c.row_count, 0),
      by_cluster:        clusters.map((c) => ({
        cluster_id:    c.cluster_id,
        canonical_fid: c.min_fid,
        sample_name:   c.names?.[0] ?? null,
      })),
    });
  }

  // Real run: call the RPC. Returns per-row before/after grouped by cluster_id.
  const { data, error } = await supabase.rpc("bulk_resolve_identical_dupes", { p_state: stateFilter });
  if (error) return json({ error: error.message }, 500);

  const rows: ResolvedRow[] = data ?? [];

  // Emit one admin_audit entry per affected row.
  // __resolution_mode: 'auto_identical' marks these as rule-based auto-resolves
  // (lower fid wins when all cluster rows are ST_Equals + identical name).
  // Downstream training queries can filter:
  //   where (after->>'__resolution_mode') = 'manual'
  //   where (after->>'__resolution_mode') = 'auto_identical'
  for (const r of rows) {
    await logAdminWrite(supabase, {
      functionName: "admin-bulk-resolve-identical-dupes",
      action:       "update",
      req,
      before:       r.before,
      after:        { ...r.after, __resolution_mode: "auto_identical" },
      success:      true,
    });
  }

  // Summarize by cluster for the response
  const byCluster = new Map<number, { canonical_fid: number | null; duplicate_fids: number[] }>();
  for (const r of rows) {
    const entry = byCluster.get(r.cluster_id) ?? { canonical_fid: null, duplicate_fids: [] };
    if (r.after.duplicate_status === "canonical") entry.canonical_fid = r.after.fid;
    else if (r.after.duplicate_status === "duplicate") entry.duplicate_fids.push(r.after.fid);
    byCluster.set(r.cluster_id, entry);
  }

  return json({
    ok:                true,
    state:             stateFilter,
    dry_run:           false,
    clusters_resolved: byCluster.size,
    rows_affected:     rows.length,
    by_cluster:        [...byCluster.entries()].map(([cluster_id, v]) => ({
      cluster_id,
      canonical_fid:  v.canonical_fid,
      duplicate_fids: v.duplicate_fids,
    })),
  });
});
