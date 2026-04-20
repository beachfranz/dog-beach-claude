// staging-record-admin/index.ts
// Generic update / delete for beaches_staging rows.
//
// POST { action: 'update', id: number, fields: Record<string, unknown> }
// POST { action: 'delete', id: number }
// Returns { ok: true } or { error: string }

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

  let body: { action?: string; id?: number; fields?: Record<string, unknown> };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const { action, id, fields } = body;
  if (!id)     return json({ error: "id required" }, 400);
  if (!action) return json({ error: "action required" }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  if (action === "update") {
    if (!fields || Object.keys(fields).length === 0)
      return json({ error: "fields required for update" }, 400);

    // Only allow updating safe columns
    const ALLOWED = new Set([
      "display_name", "city", "county", "state",
      "governing_body", "governing_jurisdiction",
      "dogs_allowed", "access_rule", "access_scope",
      "zone_description", "allowed_hours_text",
      "seasonal_start", "seasonal_end",
      "dogs_prohibited_start", "dogs_prohibited_end",
      "day_restrictions", "quality_tier",
      "review_status", "review_notes",
      "policy_source_url", "policy_confidence",
    ]);

    const safe: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(fields)) {
      if (ALLOWED.has(k)) safe[k] = v;
    }

    if (Object.keys(safe).length === 0)
      return json({ error: "No valid fields to update" }, 400);

    const { error } = await supabase
      .from("beaches_staging")
      .update(safe)
      .eq("id", id);

    if (error) return json({ error: error.message }, 500);
    return json({ ok: true });
  }

  if (action === "delete") {
    const { error } = await supabase
      .from("beaches_staging")
      .update({ dedup_status: "removed", dedup_notes: "removed via staging editor" })
      .eq("id", id);

    if (error) return json({ error: error.message }, 500);
    return json({ ok: true });
  }

  return json({ error: `Unknown action: ${action}` }, 400);
});
