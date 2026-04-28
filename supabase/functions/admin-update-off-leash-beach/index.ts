// admin-update-off-leash-beach/index.ts
// Updates a row in public.off_leash_dog_beaches. Mirrors the
// admin-update-ccc-point pattern: x-admin-secret + per-IP rate limit
// via requireAdmin, allowlist-filtered fields, admin_audit row written.
//
// Lat/lng changes also update the geom column.

import { createClient }  from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }   from "../_shared/cors.ts";
import { requireAdmin }  from "../_shared/admin-auth.ts";
import { logAdminWrite } from "../_shared/admin-audit.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const EDITABLE_FIELDS = new Set<string>([
  "name", "region", "city",
  "off_leash_legal", "off_leash_de_facto",
  "enforcement_risk", "social_norm", "confidence",
  "latitude", "longitude",
]);

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")    return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: { id?: number; fields?: Record<string, unknown> };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const id = body.id;
  if (typeof id !== "number") return json({ error: "id (number) required" }, 400);
  if (!body.fields || typeof body.fields !== "object" || Object.keys(body.fields).length === 0)
    return json({ error: "fields required (at least one)" }, 400);

  const safe: Record<string, unknown> = {};
  const rejected: string[] = [];
  for (const [k, v] of Object.entries(body.fields)) {
    if (EDITABLE_FIELDS.has(k)) safe[k] = v;
    else rejected.push(k);
  }
  if (Object.keys(safe).length === 0)
    return json({ error: "No editable fields in payload", rejected }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data: beforeRow } = await supabase
    .from("off_leash_dog_beaches").select("*").eq("id", id).single();
  if (!beforeRow) return json({ error: `No off_leash_dog_beaches row with id=${id}` }, 404);

  // Bump updated_at, refresh geom if lat/lng changed.
  safe["updated_at"] = new Date().toISOString();
  const { data: afterRow, error } = await supabase
    .from("off_leash_dog_beaches").update(safe).eq("id", id).select("*").single();

  if (error) {
    await logAdminWrite(supabase, {
      functionName: "admin-update-off-leash-beach", action: "update", req,
      locationId: `offleash:${id}`, before: beforeRow,
      success: false, error: error.message,
    });
    return json({ error: error.message, rejected }, 500);
  }

  // Refresh geom if either coordinate moved.
  const moved = (afterRow.latitude  !== beforeRow.latitude)
             || (afterRow.longitude !== beforeRow.longitude);
  if (moved && afterRow.latitude != null && afterRow.longitude != null) {
    await supabase.rpc("exec_sql", {
      // No exec_sql RPC — use direct update via supabase-js
    }).catch(() => {});
    await supabase
      .from("off_leash_dog_beaches")
      .update({ geom: `SRID=4326;POINT(${afterRow.longitude} ${afterRow.latitude})` })
      .eq("id", id);
  }

  await logAdminWrite(supabase, {
    functionName: "admin-update-off-leash-beach", action: "update", req,
    locationId: `offleash:${id}`, before: beforeRow, after: afterRow,
    success: true,
  });

  return json({ ok: true, off_leash: afterRow, rejected });
});
