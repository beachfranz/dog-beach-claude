// admin-update-beach-amenities/index.ts
//
// Upserts a single beach_amenities row from the admin curator surface.
// Slice 2.3 of "harmony phase 8 — curator on canonical tables"
// (project_curator_on_canonical_tables.md).
//
// beach_amenities is the canonical home for amenity booleans + parking +
// hours. PK = arena_group_id (FK to beaches_gold.fid). UPSERT semantics:
// new beach gets a new row with source='manual_curator'; existing row is
// updated and source is overridden to 'manual_curator' (manual edits
// override the auto_promoted_from_production backfill).
//
// POST { fid, fields: { col: value, ... } } — allowlist-filtered.
//
// Mirrors the security model of admin-update-beach-dog-policy:
// x-admin-secret + per-IP rate limit. No JWT verification.

import { createClient }  from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }   from "../_shared/cors.ts";
import { requireAdmin }  from "../_shared/admin-auth.ts";
import { logAdminWrite } from "../_shared/admin-audit.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// 11 editable amenity fields. Excludes pk (arena_group_id) and pipeline-
// managed (source, curated_at — set by this endpoint).
const EDITABLE_FIELDS = new Set<string>([
  "has_restrooms",
  "has_showers",
  "has_lifeguards",
  "has_drinking_water",
  "has_disabled_access",
  "has_food",
  "has_fire_pits",
  "has_picnic_area",
  "parking_type",
  "parking_notes",
  "hours_text",
  "notes",
]);

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")    return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: { fid?: number; fields?: Record<string, unknown> };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const fid = body.fid;
  if (typeof fid !== "number") return json({ error: "fid (number) required" }, 400);
  if (!body.fields || typeof body.fields !== "object" || Object.keys(body.fields).length === 0)
    return json({ error: "fields required (at least one)" }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // Verify beaches_gold row exists before allowing an amenities write.
  const { data: beach } = await supabase
    .from("beaches_gold").select("fid, name").eq("fid", fid).single();
  if (!beach) return json({ error: `No beaches_gold row with fid=${fid}` }, 404);

  // Snapshot current beach_amenities row (may not exist — INSERT path).
  const { data: beforeRow } = await supabase
    .from("beach_amenities").select("*").eq("arena_group_id", fid).single();

  // Allowlist-filter the incoming fields.
  const safe: Record<string, unknown> = {};
  const rejected: string[] = [];
  for (const [k, v] of Object.entries(body.fields)) {
    if (EDITABLE_FIELDS.has(k)) safe[k] = v;
    else rejected.push(k);
  }
  if (Object.keys(safe).length === 0)
    return json({ error: "No editable fields in payload", rejected }, 400);

  const upsertRow = {
    arena_group_id: fid,
    ...safe,
    source: "manual_curator",
    curated_at: new Date().toISOString(),
  };

  const { data: afterRow, error } = await supabase
    .from("beach_amenities")
    .upsert(upsertRow, { onConflict: "arena_group_id" })
    .select("*").single();

  const auditEntry = {
    functionName: "admin-update-beach-amenities" as const,
    action:       beforeRow ? "update" : "create",
    req,
    locationId:   String(fid),
    before:       beforeRow ?? null,
  };

  if (error) {
    await logAdminWrite(supabase, { ...auditEntry, success: false, error: error.message });
    return json({ error: error.message, rejected }, 500);
  }

  await logAdminWrite(supabase, { ...auditEntry, after: afterRow, success: true });

  return json({ ok: true, amenities: afterRow, beach_name: beach.name, rejected });
});
