// admin-associate-beach-only/index.ts
// Pin a beach to an explicit authority record (CPAD polygon / CCC point)
// WITHOUT moving its coordinates. Used when the coords are correct but
// the nearest-polygon heuristic would pick the wrong authority — admin
// wants the explicit association to win over spatial inference.
//
// POST {
//   fid:               number,
//   candidate_source:  'cpad' | 'ccc' | 'csp',
//   candidate_name:    string,
//   candidate_county:  string,
// }

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

  let body: { fid?: number; candidate_source?: string; candidate_name?: string; candidate_county?: string };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }
  const { fid, candidate_source, candidate_name, candidate_county } = body;
  if (typeof fid !== "number")              return json({ error: "fid required" }, 400);
  if (candidate_source !== "cpad" && candidate_source !== "ccc" && candidate_source !== "csp")
    return json({ error: "candidate_source must be cpad, ccc, or csp" }, 400);
  if (typeof candidate_name !== "string" || candidate_name.length === 0)
    return json({ error: "candidate_name required" }, 400);
  if (typeof candidate_county !== "string" || candidate_county.length === 0)
    return json({ error: "candidate_county required" }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data, error } = await supabase.rpc("associate_beach_only", {
    p_fid: fid,
    p_candidate_source: candidate_source,
    p_candidate_name:   candidate_name,
    p_candidate_county: candidate_county,
  });
  if (error) return json({ error: error.message }, 500);

  const pair = (data ?? [])[0];
  if (pair) {
    await logAdminWrite(supabase, {
      functionName: "admin-associate-beach-only",
      action:       "update",
      req,
      before:       pair.before,
      after:        { ...pair.after, __resolution_mode: "associate_only" },
      success:      true,
    });
  }

  return json({ ok: true, fid, ...(pair ?? {}) });
});
