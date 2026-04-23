// admin-create-beach/index.ts
// Inserts a new row into beaches. Called from the admin editor's
// "Create" flow, which first re-extracts metadata from a user-provided
// URL, then lets the admin review and save.
//
// Security model: same as other admin-* functions (obscure URL +
// service-role server-side, no auth layer). See admin-update-beach.
//
// POST { location_id, fields: {...} }
// Returns { ok: true, beach: <inserted row>, rejected: string[] }
//   or  { error, code } — where code='duplicate' for a UNIQUE violation
//                         on location_id (so the UI can surface it nicely)

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }  from "../_shared/cors.ts";
import { requireAdmin } from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// Same allowlist as admin-update-beach. Excludes generated columns
// (location, source_type) and immutable fields (created_at).
const INSERTABLE_FIELDS = new Set<string>([
  "location_id",   // required for INSERT, unlike the UPDATE endpoint
  "display_name", "is_active", "timezone",
  "latitude", "longitude", "address",
  "governing_body", "governing_jurisdiction",
  "dogs_allowed", "dogs_leash_required", "dogs_policy_notes",
  "dogs_allowed_areas", "dogs_off_leash_area", "dogs_prohibited_areas",
  "dogs_prohibited_reason", "dogs_season_restrictions",
  "dogs_seasonal_closures", "dogs_daily_windows", "dogs_day_of_week_mask",
  "dogs_time_restrictions", "dogs_policy_source", "dogs_policy_source_url",
  "dogs_policy_updated_at",
  "hours_text", "hours_notes",
  "has_parking", "parking_type", "parking_notes",
  "has_restrooms", "has_showers", "has_lifeguards", "has_drinking_water",
  "has_disabled_access", "has_food", "has_fire_pits", "has_picnic_area",
  "access_rule", "description", "website",
  "noaa_station_id", "noaa_station_name",
  "enrichment_source", "enrichment_confidence", "enrichment_updated_at",
  // Legacy columns — retained for parity with admin-update-beach
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
  if (!/^[a-z0-9][a-z0-9-]{0,120}$/i.test(location_id)) {
    return json({ error: "location_id must be alphanumeric + hyphens, max 120 chars" }, 400);
  }
  if (!fields || typeof fields !== "object") {
    return json({ error: "fields required" }, 400);
  }
  if (!fields.display_name) {
    return json({ error: "display_name required" }, 400);
  }

  // Allowlist filter — drop unknown keys, keep track of what was rejected
  const row: Record<string, unknown> = { location_id };
  const rejected: string[] = [];
  for (const [k, v] of Object.entries(fields)) {
    if (k === "location_id") continue;   // already set from top-level field
    if (INSERTABLE_FIELDS.has(k)) row[k] = v;
    else rejected.push(k);
  }

  // Sensible defaults for required / expected columns
  if (row.is_active === undefined) row.is_active = false;
  if (!row.timezone)               row.timezone = "America/Los_Angeles";

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const { data, error } = await supabase
    .from("beaches")
    .insert(row)
    .select("*")
    .single();

  if (error) {
    // Postgres error code 23505 = UNIQUE violation. location_id is the PK,
    // so this always means "id already taken" — surface it as a typed code
    // the UI can handle without string-matching.
    if (error.code === "23505") {
      return json({ error: `location_id "${location_id}" is already taken`, code: "duplicate" }, 409);
    }
    return json({ error: error.message, code: error.code }, 500);
  }

  return json({ ok: true, beach: data, rejected });
});
