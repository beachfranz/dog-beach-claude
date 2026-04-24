// admin-unpin-dupe-cluster/index.ts
// Clears dupe markings on every row in a cluster. Use when human review
// decides the rows aren't actually duplicates (e.g. the Trinidad/Carmel
// River case where two different beaches accidentally share coords).
//
// POST { cluster_id: number }
// Returns { ok: true, cluster_id, affected: [{ before, after }] }

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

  let body: { cluster_id?: number };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const cluster_id = body.cluster_id;
  if (typeof cluster_id !== "number")
    return json({ error: "cluster_id (number) required" }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data, error } = await supabase.rpc("unpin_dupe_cluster", {
    p_cluster_id: cluster_id,
  });
  if (error) return json({ error: error.message }, 500);

  // __resolution_mode: 'unpin' marks "human judged this wasn't really a dupe"
  // — a distinct decision from resolve (pick canonical) or auto-resolve.
  const affected: Array<{ before: Record<string, unknown>; after: Record<string, unknown> }> = data ?? [];
  for (const pair of affected) {
    await logAdminWrite(supabase, {
      functionName: "admin-unpin-dupe-cluster",
      action:       "update",
      req,
      before:       pair.before,
      after:        { ...pair.after, __resolution_mode: "unpin" },
      success:      true,
    });
  }

  return json({ ok: true, cluster_id, affected });
});
