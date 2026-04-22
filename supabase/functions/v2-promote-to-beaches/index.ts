// v2-promote-to-beaches/index.ts
// Promotes selected rows from beaches_staging_new into the live `beaches`
// table. Intended to expand coverage from the current 5 curated beaches up
// to any subset of the 623 enriched ready records.
//
// Upsert semantics:
//   - If a live row already exists (matched by slug or a staging row with
//     promoted_location_id set), only the NEW enrichment columns are written.
//     Hand-curated fields (dog_rules, leash_policy, access_rule, off_leash_flag,
//     allowed_hours_text, parking_text, description, website, besttime_venue_id)
//     are never touched.
//   - If no live row exists, INSERT a new one. location_id slug generated
//     from display_name; numeric suffix on collision.
//
// Filter the promotion by passing a WHERE-style filter object. This first
// pass uses { city_or_governing_body_like: "Huntington Beach" }.
//
// POST {
//   dry_run?: boolean,
//   ids?: number[],            // specific staging ids to promote
//   city?: string,             // match beaches_staging_new.city exactly
//   governing_body_like?: string,  // ILIKE match on governing_body
//   is_active?: boolean,       // set on newly inserted rows (default false)
// }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// Fields copied from staging → beaches on both insert AND update-only-if-null.
// For existing beach rows, we ALWAYS fill these (they're enrichment, not curation).
const ENRICHMENT_FIELDS = [
  "dogs_allowed", "dogs_allowed_areas", "dogs_prohibited_areas",
  "dogs_leash_required", "dogs_off_leash_area",
  "dogs_policy_notes", "dogs_policy_source", "dogs_policy_source_url",
  "dogs_policy_updated_at", "dogs_time_restrictions", "dogs_season_restrictions",
  "dogs_seasonal_closures", "dogs_daily_windows", "dogs_day_of_week_mask",
  "dogs_prohibited_reason",
  "has_parking", "parking_type", "parking_notes",
  "hours_text", "hours_notes",
  "has_restrooms", "has_showers", "has_lifeguards",
  "has_drinking_water", "has_disabled_access",
  "governing_jurisdiction", "governing_body",
  "noaa_station_name",
  "enrichment_source", "enrichment_updated_at", "enrichment_confidence",
] as const;

// Fields set ONLY when INSERTING a new row. Not touched on update.
const INSERT_ONLY_FIELDS = [
  "display_name", "latitude", "longitude",
  "noaa_station_id",
  "address", // derive from city/state
] as const;

function slugify(s: string): string {
  return s.toLowerCase()
    .replace(/[''`]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}

async function generateLocationId(supabase: ReturnType<typeof createClient>, displayName: string): Promise<string> {
  const base = slugify(displayName);
  if (!base) return `beach-${Math.floor(Math.random() * 1_000_000)}`;
  // Check for collision
  let candidate = base;
  let n = 1;
  while (true) {
    const { data } = await supabase.from("beaches").select("location_id").eq("location_id", candidate).maybeSingle();
    if (!data) return candidate;
    n += 1;
    candidate = `${base}-${n}`;
    if (n > 20) return `${base}-${Date.now()}`;
  }
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (b: unknown, s = 200) => new Response(JSON.stringify(b), { status: s, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: {
    dry_run?: boolean;
    ids?: number[];
    city?: string;
    governing_body_like?: string;
    is_active?: boolean;
  } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // Build candidate query
  let q = supabase
    .from("beaches_staging_new")
    .select("*")
    .eq("review_status", "ready");

  if (body.ids && body.ids.length > 0) {
    q = q.in("id", body.ids);
  } else {
    if (body.city) q = q.eq("city", body.city);
    if (body.governing_body_like) q = q.ilike("governing_body", `%${body.governing_body_like}%`);
  }

  const { data: stagingRows, error: fErr } = await q;
  if (fErr) return json({ error: fErr.message }, 500);
  if (!stagingRows?.length) return json({ candidates: 0 });

  // Fetch live beaches near these lat/lons so we can match existing rows
  const { data: liveRows, error: lErr } = await supabase
    .from("beaches")
    .select("location_id, display_name, latitude, longitude");
  if (lErr) return json({ error: lErr.message }, 500);

  // Match heuristic: within 100m (haversine) OR exact display_name match
  function haversine(lat1: number, lon1: number, lat2: number, lon2: number): number {
    const R = 6_371_000;
    const φ1 = lat1 * Math.PI / 180, φ2 = lat2 * Math.PI / 180;
    const Δφ = (lat2 - lat1) * Math.PI / 180, Δλ = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(Δφ / 2) ** 2 + Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ / 2) ** 2;
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  }

  function findLive(s: Record<string, unknown>): { location_id: string } | null {
    const lat = Number(s.latitude), lon = Number(s.longitude);
    for (const l of liveRows ?? []) {
      if (l.display_name === s.display_name) return l;
      const d = haversine(lat, lon, Number(l.latitude), Number(l.longitude));
      if (d <= 100) return l;
    }
    return null;
  }

  const plan: Array<{
    staging_id: number;
    display_name: string;
    action: "insert" | "update";
    location_id: string | null;
    match_reason?: string;
    staging_row: Record<string, unknown>;
  }> = [];

  for (const s of stagingRows) {
    const existing = findLive(s);
    plan.push({
      staging_id:   s.id as number,
      display_name: s.display_name as string,
      action:       existing ? "update" : "insert",
      location_id:  existing?.location_id ?? null,
      match_reason: existing ? "name or ≤100m" : undefined,
      staging_row:  s,
    });
  }

  if (body.dry_run) {
    return json({
      dry_run:    true,
      candidates: stagingRows.length,
      inserts:    plan.filter(p => p.action === "insert").length,
      updates:    plan.filter(p => p.action === "update").length,
      preview:    plan.map(p => ({
        staging_id:   p.staging_id,
        display_name: p.display_name,
        action:       p.action,
        location_id:  p.location_id,
        match_reason: p.match_reason,
      })),
    });
  }

  const is_active = body.is_active ?? false;
  let inserted = 0;
  let updated  = 0;
  const errors: string[] = [];

  for (const p of plan) {
    const s = p.staging_row;

    // Build enrichment patch (only the allowed columns)
    const patch: Record<string, unknown> = {};
    for (const k of ENRICHMENT_FIELDS) {
      if (s[k] !== undefined) patch[k] = s[k];
    }

    if (p.action === "update" && p.location_id) {
      const { error } = await supabase
        .from("beaches")
        .update(patch)
        .eq("location_id", p.location_id);
      if (error) errors.push(`update ${p.location_id}: ${error.message}`);
      else updated++;
    } else {
      // Insert: include staging identity + enrichment
      const loc = await generateLocationId(supabase, String(s.display_name));
      const addressParts = [s.city, s.state, s.zip].filter(Boolean).join(", ");

      const insertRow: Record<string, unknown> = {
        ...patch,
        location_id:      loc,
        display_name:     s.display_name,
        latitude:         s.latitude,
        longitude:        s.longitude,
        noaa_station_id:  s.noaa_station_id ?? null,
        address:          addressParts || null,
        is_active,
        timezone:         "America/Los_Angeles",
      };

      const { error } = await supabase.from("beaches").insert(insertRow);
      if (error) errors.push(`insert ${s.display_name} (${loc}): ${error.message}`);
      else inserted++;
    }
  }

  return json({
    candidates: stagingRows.length,
    inserted,
    updated,
    is_active,
    errors,
  });
});
