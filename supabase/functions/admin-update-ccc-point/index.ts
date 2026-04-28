// admin-update-ccc-point/index.ts
// Updates a row in public.ccc_access_points from the location editor's
// CCC editing flow. Mirrors admin-update-location: x-admin-secret +
// per-IP rate limit via requireAdmin, allowlist-filtered fields,
// admin_audit row written on every attempt.

import { createClient }  from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }   from "../_shared/cors.ts";
import { requireAdmin }  from "../_shared/admin-auth.ts";
import { logAdminWrite } from "../_shared/admin-audit.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const EDITABLE_FIELDS = new Set<string>([
  // Identity / location
  "name", "location", "description", "phone",
  "county", "district",
  "latitude", "longitude",
  // Top-level access flags
  "dog_friendly", "open_to_public", "fee", "restrictions",
  // Amenities
  "parking", "restrooms", "showers", "drinking_water",
  "food", "picnic_area", "fire_pits", "lifeguard",
  "disabled_access", "beach_wheelchair", "beach_wheelchair_program",
  "campground",
  // Beach feature flags
  "sandy_beach", "dunes", "rocky_shore", "upland_beach", "bluff",
  "bay_lagoon_lake", "urban_waterfront", "inland_area", "wetland",
  "stream_corridor", "offshore_reef",
  // Trails / paths
  "stairs_to_beach", "path_to_beach", "boardwalk", "blufftop_trails",
  "blufftop_park", "bike_path", "equestrian_trail",
  "cct_link", "cct_designation",
  // Activities
  "swimming", "diving", "snorkeling", "surfing", "fishing",
  "boating", "kayaking", "tidepool", "wildlife_viewing",
  "playground", "sport_fields", "volleyball", "windsurf_kite",
  // Landmarks
  "lighthouse", "pier", "historic_structure", "shipwrecks",
  // Map / lifecycle
  "google_maps_location", "apple_maps_location",
  "data_updated", "archived", "archived_reason",
]);

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")    return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: { objectid?: number; fields?: Record<string, unknown> };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const objectid = body.objectid;
  if (typeof objectid !== "number") return json({ error: "objectid (number) required" }, 400);
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
    .from("ccc_access_points").select("*").eq("objectid", objectid).single();
  if (!beforeRow) return json({ error: `No ccc_access_points row with objectid=${objectid}` }, 404);

  const { data: afterRow, error } = await supabase
    .from("ccc_access_points").update(safe).eq("objectid", objectid).select("*").single();

  if (error) {
    await logAdminWrite(supabase, {
      functionName: "admin-update-ccc-point", action: "update", req,
      locationId: `ccc:${objectid}`, before: beforeRow,
      success: false, error: error.message,
    });
    return json({ error: error.message, rejected }, 500);
  }

  await logAdminWrite(supabase, {
    functionName: "admin-update-ccc-point", action: "update", req,
    locationId: `ccc:${objectid}`, before: beforeRow, after: afterRow,
    success: true,
  });

  return json({ ok: true, ccc: afterRow, rejected });
});
