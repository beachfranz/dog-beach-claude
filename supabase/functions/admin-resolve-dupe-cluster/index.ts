// admin-resolve-dupe-cluster/index.ts
// Promotes one row in a duplicate cluster as canonical; marks the others
// as 'duplicate'. Uses the resolve_dupe_cluster() RPC, which returns per-row
// before/after state so this wrapper can emit one admin_audit entry per
// affected fid. The audit log is the source of truth for dedupe history.
//
// POST { cluster_id: number, canonical_fid: number }
// Returns { ok: true, cluster_id, canonical_fid, affected: [{ fid, before, after }] }

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

  let body: { cluster_id?: number; canonical_fid?: number };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const { cluster_id, canonical_fid } = body;
  if (typeof cluster_id !== "number")
    return json({ error: "cluster_id (number) required" }, 400);
  if (typeof canonical_fid !== "number")
    return json({ error: "canonical_fid (number) required" }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data, error } = await supabase.rpc("resolve_dupe_cluster", {
    p_cluster_id:    cluster_id,
    p_canonical_fid: canonical_fid,
  });
  if (error) return json({ error: error.message }, 500);

  // Per-row audit: one admin_audit entry per affected row.
  // __resolution_mode tags the record for downstream training-data queries —
  // 'manual' here means a human reviewed the cluster's provenance and picked
  // the canonical fid, distinct from 'auto_identical' used by the bulk
  // auto-resolver.
  const affected: Array<{ before: Record<string, unknown>; after: Record<string, unknown> }> = data ?? [];
  for (const pair of affected) {
    await logAdminWrite(supabase, {
      functionName: "admin-resolve-dupe-cluster",
      action:       "update",
      req,
      before:       pair.before,
      after:        { ...pair.after, __resolution_mode: "manual" },
      success:      true,
    });
  }

  return json({ ok: true, cluster_id, canonical_fid, affected });
});
