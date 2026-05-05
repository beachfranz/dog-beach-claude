// admin-review-auto-polygon/index.ts
// Approve/reject a row in public.beach_polygons_auto_extracted.
// Body: { id: number, review_status: 'approved' | 'rejected' | 'pending',
//         reviewer_notes?: string, reviewer?: string }

import { createClient }   from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }    from "../_shared/cors.ts";
import { requireAdmin }   from "../_shared/admin-auth.ts";
import { logAdminWrite }  from "../_shared/admin-audit.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const VALID_STATUSES = new Set(["approved", "rejected", "pending"]);

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")    return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: {
    id?: number;
    review_status?: string;
    reviewer_notes?: string;
    reviewer?: string;
  };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const id = body.id;
  const status = body.review_status;
  if (typeof id !== "number")        return json({ error: "id (number) required" }, 400);
  if (!status || !VALID_STATUSES.has(status))
    return json({ error: `review_status must be one of ${[...VALID_STATUSES].join(", ")}` }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // Read before-state for audit
  const { data: before, error: beforeErr } = await supabase
    .from("beach_polygons_auto_extracted")
    .select("id, fid, review_status, reviewed_by, reviewed_at, reviewer_notes")
    .eq("id", id)
    .maybeSingle();
  if (beforeErr) return json({ error: beforeErr.message }, 500);
  if (!before)   return json({ error: `id ${id} not found` }, 404);

  const update: Record<string, unknown> = {
    review_status:   status,
    reviewer_notes:  body.reviewer_notes ?? before.reviewer_notes,
  };
  if (status === "pending") {
    // Reverting to pending clears the review trail
    update.reviewed_by = null;
    update.reviewed_at = null;
  } else {
    update.reviewed_by = body.reviewer ?? "admin";
    update.reviewed_at = new Date().toISOString();
  }

  const { data: after, error } = await supabase
    .from("beach_polygons_auto_extracted")
    .update(update)
    .eq("id", id)
    .select("id, fid, review_status, reviewed_by, reviewed_at, reviewer_notes")
    .single();
  if (error) {
    // The unique partial index will throw 23505 if you try to approve a 2nd
    // row for the same fid — surface that helpfully.
    const msg = error.message.includes("beach_polygons_auto_extracted_one_approved_per_fid")
      ? `Another row is already approved for fid ${before.fid}; reject the existing approval first.`
      : error.message;
    return json({ error: msg }, 400);
  }

  await logAdminWrite(supabase, {
    functionName: "admin-review-auto-polygon",
    action:       "update",
    req,
    before,
    after,
    success:      true,
  });

  return json({ ok: true, before, after });
});
