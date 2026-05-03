// admin-update-beaches-gold/index.ts
//
// Updates a single beaches_gold row from the admin curator surface.
// This is slice 1 of "harmony phase 8 — curator on canonical tables"
// (see project_curator_on_canonical_tables.md). Replaces the spine-field
// arm of admin-update-location, which writes to the legacy locations_stage
// table. Both endpoints coexist during the transition; admin UI is not
// migrated yet.
//
// Two arms:
//   1. Field updates: { fid, fields: { col: value, ... } } — allowlist-filtered
//   2. Status transitions: { fid, status_change: { to: 'active'|'inactive', reason?: string } }
//      Reason required for inactive; restoring to active clears inactive_reason.
//
// beaches_gold has no "deleted" lifecycle; deletion is just is_active=false
// with an inactive_reason.
//
// Both arms write to admin_audit via logAdminWrite. fid is stored in the
// audit table's location_id column as text.
//
// Mirrors the security model of admin-update-location: x-admin-secret header
// + per-IP rate limit via requireAdmin(). No JWT verification.

import { createClient }  from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }   from "../_shared/cors.ts";
import { requireAdmin }  from "../_shared/admin-auth.ts";
import { logAdminWrite } from "../_shared/admin-audit.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// Curator-editable spine fields. Excludes generated (geom), promoted-
// from-evidence (cpad_unit_id, c1_jurisdiction_id, county_geoid — set
// by populate_polygon_containment_gold), and pipeline-managed
// identifiers (fid, location_id, group_id, source_*, promoted_*, etc.).
const EDITABLE_FIELDS = new Set<string>([
  // Identity
  "name", "display_name_override",
  // Location
  "lat", "lon",
  // Address (slice 3 — added 2026-05-03)
  "address",
  "address_street", "address_city", "address_state", "address_zip",
  "address_county",
  // Marketing / detail text
  "website", "description", "parking_text",
  // Scoring infra
  "noaa_station_id",
  "open_time", "close_time",
  // Lifecycle
  "is_active", "inactive_reason",
  "is_scoreable",
  "timezone",
]);

type StatusTo = "active" | "inactive";
const STATUS_VALUES: StatusTo[] = ["active", "inactive"];

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")    return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: {
    fid?: number;
    fields?: Record<string, unknown>;
    status_change?: { to?: string; reason?: string };
  };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const fid = body.fid;
  if (typeof fid !== "number") return json({ error: "fid (number) required" }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // Snapshot before any change so the audit row carries before/after.
  const { data: beforeRow } = await supabase
    .from("beaches_gold").select("*").eq("fid", fid).single();
  if (!beforeRow) return json({ error: `No beaches_gold row with fid=${fid}` }, 404);

  // Build the update payload — combines fields arm + status_change arm.
  const update: Record<string, unknown> = {};
  const rejected: string[] = [];

  if (body.fields && typeof body.fields === "object") {
    for (const [k, v] of Object.entries(body.fields)) {
      if (EDITABLE_FIELDS.has(k)) update[k] = v;
      else rejected.push(k);
    }
  }

  if (body.status_change) {
    const to = body.status_change.to as StatusTo | undefined;
    const reason = (body.status_change.reason ?? "").trim();
    if (!to || !STATUS_VALUES.includes(to))
      return json({ error: `status_change.to must be one of ${STATUS_VALUES.join(", ")}` }, 400);
    if (to === "inactive" && !reason)
      return json({ error: "reason required when transitioning to inactive" }, 400);

    if (to === "active") {
      update.is_active       = true;
      update.inactive_reason = null;
    } else {
      update.is_active       = false;
      update.inactive_reason = reason;
    }
  }

  if (Object.keys(update).length === 0)
    return json({ error: "Nothing to update — provide fields or status_change", rejected }, 400);

  const { data: afterRow, error } = await supabase
    .from("beaches_gold").update(update).eq("fid", fid).select("*").single();

  const auditEntry = {
    functionName: "admin-update-beaches-gold" as const,
    action:       "update",
    req,
    locationId:   String(fid),
    before:       beforeRow,
  };

  if (error) {
    await logAdminWrite(supabase, { ...auditEntry, success: false, error: error.message });
    return json({ error: error.message, rejected }, 500);
  }

  await logAdminWrite(supabase, {
    ...auditEntry,
    after: {
      ...afterRow,
      ...(body.status_change ? { __status_change_to: body.status_change.to } : {}),
    },
    success: true,
  });

  return json({ ok: true, beach: afterRow, rejected });
});
