// admin-update-location/index.ts
// Updates a single locations_stage row from the location-editor admin page.
// Two arms:
//
//   1. Field updates: { fid, fields: { col: value, ... } } — allowlist-filtered
//   2. Status transitions: { fid, status_change: { to: 'active'|'inactive'|'deleted', reason?: string } }
//      Reason required for inactive + deleted. Restoring to active clears the
//      reason fields.
//
// Both arms write to admin_audit via logAdminWrite. fid is stored in the
// audit table's location_id column as text (column predates the staging
// schema; treat the column as a freeform entity_id slot).
//
// Mirrors the security model of admin-update-beach: x-admin-secret header
// + per-IP rate limit via requireAdmin(). No JWT verification.

import { createClient }  from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }   from "../_shared/cors.ts";
import { requireAdmin }  from "../_shared/admin-auth.ts";
import { logAdminWrite } from "../_shared/admin-audit.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// Columns the editor may write directly. Excludes generated/system columns
// (geom, created_at, updated_at, fid, pipeline_*) and lifecycle columns
// (handled by status_change arm).
const EDITABLE_FIELDS = new Set<string>([
  "display_name", "latitude", "longitude", "timezone",
  "address_street", "address_city", "address_state", "address_zip",
  "address_county", "raw_address",
  "state_code", "county_name", "county_fips",
  "place_name", "place_fips", "place_type",
  "governing_body_name", "governing_body_type",
  "access_status", "description", "website",
  "dogs_allowed", "dogs_leash_required", "dogs_restricted_hours",
  "dogs_seasonal_rules", "dogs_zone_description",
  "open_time", "close_time", "hours_text",
  "has_parking", "parking_type", "parking_notes",
  "has_restrooms", "has_showers", "has_lifeguards", "has_drinking_water",
  "has_disabled_access", "has_food", "has_fire_pits", "has_picnic_area",
  "noaa_station_id", "noaa_station_name", "noaa_station_distance_m",
  "review_status", "review_notes",
]);

type StatusTo = "active" | "inactive" | "deleted";
const STATUS_VALUES: StatusTo[] = ["active", "inactive", "deleted"];

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
    .from("locations_stage").select("*").eq("fid", fid).single();
  if (!beforeRow) return json({ error: `No locations_stage row with fid=${fid}` }, 404);

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
    if ((to === "inactive" || to === "deleted") && !reason)
      return json({ error: `reason required when transitioning to ${to}` }, 400);

    if (to === "active") {
      update.is_active       = true;
      update.inactive_reason = null;
      update.deleted_at      = null;
      update.deleted_reason  = null;
    } else if (to === "inactive") {
      update.is_active       = false;
      update.inactive_reason = reason;
      update.deleted_at      = null;
      update.deleted_reason  = null;
    } else {
      // deleted
      update.is_active      = false;
      update.deleted_at     = new Date().toISOString();
      update.deleted_reason = reason;
    }
  }

  if (Object.keys(update).length === 0)
    return json({ error: "Nothing to update — provide fields or status_change", rejected }, 400);

  const { data: afterRow, error } = await supabase
    .from("locations_stage").update(update).eq("fid", fid).select("*").single();

  const auditAction = body.status_change ? "update" : "update";
  const auditEntry = {
    functionName: "admin-update-location" as const,
    action:       auditAction,
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

  return json({ ok: true, location: afterRow, rejected });
});
