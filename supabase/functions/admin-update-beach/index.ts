// admin-update-beach/index.ts
// Updates fields on a single beaches row.
//
// Security model (matches the dedup-era admin tooling):
//   - URL is obscure (page at admin/beach-editor.html, not linked anywhere
//     user-facing)
//   - Service role key lives in the edge function env only, never in the
//     browser
//   - Client calls this endpoint with the standard anon key
//   - NO JWT verification, NO email allowlist
//   - Security is by obscurity and recoverability, not authentication
//
// If this ever needs to become real-auth, the shape doesn't change — we'd
// add a JWT verify step at the top and an ADMIN_EMAILS env var check.
//
// POST { location_id: string, fields: Record<string, unknown> }
// Returns { ok: true, beach: <updated row>, rejected: string[] }

import { createClient }   from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }    from "../_shared/cors.ts";
import { requireAdmin }   from "../_shared/admin-auth.ts";
import { logAdminWrite }  from "../_shared/admin-audit.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// Columns the admin editor is allowed to update. Excludes generated columns
// (location geography), immutable identifiers (location_id, created_at), and
// internal scoring/besttime state.
const EDITABLE_FIELDS = new Set<string>([
  // Identity
  "display_name", "is_active", "timezone",
  // Location
  "latitude", "longitude", "address",
  // Governance
  "governing_body", "governing_jurisdiction",
  // Dogs
  "dogs_allowed", "dogs_leash_required", "dogs_policy_notes",
  "dogs_allowed_areas", "dogs_off_leash_area", "dogs_prohibited_areas",
  "dogs_prohibited_reason", "dogs_season_restrictions",
  "dogs_seasonal_closures", "dogs_daily_windows", "dogs_day_of_week_mask",
  "dogs_time_restrictions", "dogs_policy_source", "dogs_policy_source_url",
  "dogs_policy_updated_at",
  // Practical
  "hours_text", "hours_notes",
  "has_parking", "parking_type", "parking_notes",
  "has_restrooms", "has_showers", "has_lifeguards", "has_drinking_water",
  "has_disabled_access", "has_food", "has_fire_pits", "has_picnic_area",
  // Access & misc user-facing
  "access_rule", "description", "website",
  // NOAA
  "noaa_station_id", "noaa_station_name",
  // Enrichment (pipeline-managed, rarely edited)
  "enrichment_source", "enrichment_confidence", "enrichment_updated_at",
  // Legacy columns (kept editable in case you need to fix old data)
  "leash_policy", "dog_rules", "amenities", "restrooms", "parking_text",
  "off_leash_flag", "locality", "access_scope", "allowed_hours_text",
  "dogs_prohibited_start", "dogs_prohibited_end",
]);

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: { location_id?: string; fields?: Record<string, unknown> };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const { location_id, fields } = body;
  if (!location_id) return json({ error: "location_id required" }, 400);
  if (!fields || typeof fields !== "object" || Object.keys(fields).length === 0)
    return json({ error: "fields required (at least one)" }, 400);

  // Allowlist filter
  const safe: Record<string, unknown> = {};
  const rejected: string[] = [];
  for (const [k, v] of Object.entries(fields)) {
    if (EDITABLE_FIELDS.has(k)) safe[k] = v;
    else rejected.push(k);
  }

  if (Object.keys(safe).length === 0)
    return json({ error: "No editable fields in payload", rejected }, 400);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // Snapshot the row before the update so the audit entry can diff
  // before/after and record which fields actually changed.
  const { data: beforeRow } = await supabase
    .from("beaches").select("*").eq("location_id", location_id).single();

  const { data, error } = await supabase
    .from("beaches")
    .update(safe)
    .eq("location_id", location_id)
    .select("*")
    .single();

  if (error) {
    await logAdminWrite(supabase, {
      functionName: "admin-update-beach", action: "update", req,
      locationId: location_id, before: beforeRow ?? null,
      success: false, error: error.message,
    });
    return json({ error: error.message }, 500);
  }
  if (!data) {
    await logAdminWrite(supabase, {
      functionName: "admin-update-beach", action: "update", req,
      locationId: location_id, before: beforeRow ?? null,
      success: false, error: "No row updated (bad location_id?)",
    });
    return json({ error: "No row updated (bad location_id?)" }, 404);
  }

  await logAdminWrite(supabase, {
    functionName: "admin-update-beach", action: "update", req,
    locationId: location_id, before: beforeRow ?? null, after: data,
    success: true,
  });

  return json({ ok: true, beach: data, rejected });
});
