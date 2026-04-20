// resolve-staging-duplicates/index.ts
// Applies a dedup decision to a set of staging records.
//
// Actions:
//   resolve   — keep_id is marked 'reviewed', all other ids marked 'removed'
//               Pass new_name to rename the keeper at the same time (merge).
//   keep_both — all ids marked 'reviewed' (confirmed distinct beaches)
//
// POST { action: 'resolve' | 'keep_both', ids: number[], keep_id?: number, new_name?: string }
// Returns { ok: true, affected: number }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { action?: string; ids?: number[]; keep_id?: number; remove_id?: number; new_name?: string };
  try { body = await req.json(); }
  catch { return json({ error: "Invalid JSON" }, 400); }

  const { action, ids, keep_id, remove_id, new_name } = body;

  if (!action || !Array.isArray(ids) || ids.length === 0)
    return json({ error: "action and ids required" }, 400);

  if (action === "resolve" && keep_id == null)
    return json({ error: "keep_id required for resolve action" }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  let affected = 0;

  if (action === "keep_both") {
    const { error, count } = await supabase
      .from("beaches_staging")
      .update({ dedup_status: "reviewed", dedup_notes: "confirmed distinct" })
      .in("id", ids);
    if (error) return json({ error: error.message }, 500);
    affected = count ?? ids.length;
  }

  else if (action === "resolve") {
    const removeIds = ids.filter(id => id !== keep_id);

    // Mark keeper as reviewed (+ optional rename)
    const keeperUpdate: Record<string, unknown> = { dedup_status: "reviewed" };
    if (new_name?.trim()) keeperUpdate.display_name = new_name.trim();

    const { error: keepErr } = await supabase
      .from("beaches_staging")
      .update(keeperUpdate)
      .eq("id", keep_id);
    if (keepErr) return json({ error: keepErr.message }, 500);

    // Mark the rest as removed
    if (removeIds.length > 0) {
      const { error: removeErr } = await supabase
        .from("beaches_staging")
        .update({
          dedup_status: "removed",
          dedup_notes:  `duplicate of id=${keep_id}`,
        })
        .in("id", removeIds);
      if (removeErr) return json({ error: removeErr.message }, 500);
    }

    affected = ids.length;
  }

  else if (action === "remove_all") {
    // Mark every record in the group as removed (e.g. not a real beach).
    const { error: removeErr } = await supabase
      .from("beaches_staging")
      .update({ dedup_status: "removed", dedup_notes: "removed — not a beach" })
      .in("id", ids);
    if (removeErr) return json({ error: removeErr.message }, 500);
    affected = ids.length;
  }

  else if (action === "rename") {
    // Update display_name only — does not affect dedup_status.
    if (!new_name?.trim()) return json({ error: "new_name required for rename action" }, 400);
    if (ids.length !== 1)  return json({ error: "rename requires exactly one id" }, 400);
    const { error: renameErr } = await supabase
      .from("beaches_staging")
      .update({ display_name: new_name.trim() })
      .eq("id", ids[0]);
    if (renameErr) return json({ error: renameErr.message }, 500);
    affected = 1;
  }

  else if (action === "remove_one") {
    // Mark one record removed, mark the rest of the group as reviewed.
    if (remove_id == null) return json({ error: "remove_id required for remove_one action" }, 400);
    const keepIds = ids.filter(id => id !== remove_id);

    const { error: removeErr } = await supabase
      .from("beaches_staging")
      .update({ dedup_status: "removed", dedup_notes: `removed from group [${keepIds.join(",")}]` })
      .eq("id", remove_id);
    if (removeErr) return json({ error: removeErr.message }, 500);

    if (keepIds.length > 0) {
      const { error: keepErr } = await supabase
        .from("beaches_staging")
        .update({ dedup_status: "reviewed", dedup_notes: "confirmed distinct" })
        .in("id", keepIds);
      if (keepErr) return json({ error: keepErr.message }, 500);
    }

    affected = ids.length;
  }

  else {
    return json({ error: `Unknown action: ${action}` }, 400);
  }

  return json({ ok: true, affected });
});
