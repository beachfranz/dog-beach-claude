// admin-update-beach-dog-policy/index.ts
//
// Upserts a single beach_dog_policy row from the admin curator surface.
// Slice 1.2 of "harmony phase 8 — curator on canonical tables"
// (see project_curator_on_canonical_tables.md).
//
// beach_dog_policy is the consumer-side dog-policy overlay that index.html
// reads. PK = arena_group_id (FK to beaches_gold.fid). UPSERT semantics:
// if no row exists for the beach, INSERT one with source='manual_curator'.
// If a row exists, UPDATE only the fields provided + bump curated_at;
// preserve the existing source unless an existing row says
// 'auto_promoted_from_production' or similar (manual edits override
// auto-promoted entries by setting source='manual_curator').
//
// POST { fid, fields: { col: value, ... } } — allowlist-filtered.
//
// Mirrors the security model of admin-update-beaches-gold:
// x-admin-secret + per-IP rate limit. No JWT verification.

import { createClient }  from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }   from "../_shared/cors.ts";
import { requireAdmin }  from "../_shared/admin-auth.ts";
import { logAdminWrite } from "../_shared/admin-audit.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// Editable dog-policy fields. Excludes pk (arena_group_id), pipeline-
// managed (source, curated_at — set by this endpoint), and the
// zone_rules_updated_at timestamp (server-set when zone_rules changes).
const EDITABLE_FIELDS = new Set<string>([
  "dogs_allowed",
  "leash_policy",
  "off_leash_flag",
  "dogs_prohibited_start",
  "dogs_prohibited_end",
  "dogs_allowed_areas",
  "access_rule",
  "notes",
  "zone_rules",   // jsonb — section-aware policy from the zone-rules-editor UI
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

  // Verify beaches_gold row exists before allowing a dog_policy write.
  const { data: beach } = await supabase
    .from("beaches_gold").select("fid, name").eq("fid", fid).single();
  if (!beach) return json({ error: `No beaches_gold row with fid=${fid}` }, 404);

  // Snapshot current beach_dog_policy row (may not exist yet — INSERT path).
  const { data: beforeRow } = await supabase
    .from("beach_dog_policy").select("*").eq("arena_group_id", fid).single();

  // Allowlist-filter the incoming fields.
  const safe: Record<string, unknown> = {};
  const rejected: string[] = [];
  for (const [k, v] of Object.entries(body.fields)) {
    if (EDITABLE_FIELDS.has(k)) safe[k] = v;
    else rejected.push(k);
  }
  if (Object.keys(safe).length === 0)
    return json({ error: "No editable fields in payload", rejected }, 400);

  // UPSERT shape: include arena_group_id + source + curated_at on every write.
  // Manual edits override auto-promoted entries by stamping source='manual_curator'.
  // Bump zone_rules_updated_at when zone_rules is part of this write.
  const now = new Date().toISOString();
  const upsertRow: Record<string, unknown> = {
    arena_group_id: fid,
    ...safe,
    source: "manual_curator",
    curated_at: now,
  };
  if ("zone_rules" in safe) upsertRow.zone_rules_updated_at = now;

  const { data: afterRow, error } = await supabase
    .from("beach_dog_policy")
    .upsert(upsertRow, { onConflict: "arena_group_id" })
    .select("*").single();

  const auditEntry = {
    functionName: "admin-update-beach-dog-policy" as const,
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

  return json({ ok: true, dog_policy: afterRow, beach_name: beach.name, rejected });
});
