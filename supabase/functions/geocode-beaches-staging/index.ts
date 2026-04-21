// geocode-beaches-staging/index.ts
// Reverse-geocodes beaches_staging_new rows. Three steps per beach:
//
//   1. Google Maps — address fields (city, county, state, zip, etc.)
//   2. Census Bureau Incorporated Places — governing_city / governing_jurisdiction
//        (city if incorporated, county if unincorporated)
//   3. Jurisdiction overrides (in priority order):
//        a. NPS match  → governing federal + review_status = ready
//        b. CSP match  → governing state  + review_status = ready
//        (Census result stands if neither matches)
//
// NPS and CSP place lists are loaded from the nps_places / csp_places tables
// once per invocation and matched per-row using proximity (≤300m) and name
// similarity (Dice ≥ 0.65, distance ≤ 20km).
//
// Only processes rows where geocode_status IS NULL and review_status IS NULL.
//
// POST { state?: string, county?: string, limit?: number }
// Returns { processed, succeeded, zero_results, errors, federal_matches, state_matches }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const GOOGLE_KEY           = Deno.env.get("GOOGLE_MAPS_API_KEY")!;

const GEOCODE_URL   = "https://maps.googleapis.com/maps/api/geocode/json";
const CENSUS_URL    = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates";
const CONCURRENCY   = 5;
const DEFAULT_LIMIT = 1000;

const PROXIMITY_M       = 300;
const PROXIMITY_MIN_SIM = 0.20;
const NAME_THRESHOLD    = 0.65;
const NAME_MAX_DIST_M   = 20_000;

// ── Google response types ─────────────────────────────────────────────────────

interface GeoComponent {
  long_name:  string;
  short_name: string;
  types:      string[];
}

// ── Matching helpers ──────────────────────────────────────────────────────────

function component(components: GeoComponent[], type: string): string | null {
  return components.find(c => c.types.includes(type))?.long_name ?? null;
}

function stripPlaceSuffix(name: string): string {
  return name.replace(/\s+(city|town|village|borough|municipality|township)$/i, "").trim();
}

function haversine(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 6_371_000;
  const φ1 = lat1 * Math.PI / 180, φ2 = lat2 * Math.PI / 180;
  const Δφ = (lat2 - lat1) * Math.PI / 180, Δλ = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(Δφ / 2) ** 2 + Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function normalise(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9 ]/g, "").replace(/\s+/g, " ").trim();
}

function dice(a: string, b: string): number {
  if (!a || !b) return 0;
  if (a === b)  return 1;
  const bg = (s: string) => {
    const m = new Map<string, number>();
    for (let i = 0; i < s.length - 1; i++) {
      const k = s.slice(i, i + 2);
      m.set(k, (m.get(k) ?? 0) + 1);
    }
    return m;
  };
  const ab = bg(a), bb = bg(b);
  let x = 0;
  for (const [k, n] of ab) x += Math.min(n, bb.get(k) ?? 0);
  return (2 * x) / (a.length - 1 + (b.length - 1));
}

interface Place { title: string; latitude: number | null; longitude: number | null; park: string; }

function matchPlace(
  beachName: string, lat: number, lon: number, places: Place[],
): { title: string; park: string; signal: string; distanceM: number | null } | null {
  const norm = normalise(beachName);
  const degBound = PROXIMITY_M / 100_000;

  // Proximity pass
  for (const p of places) {
    if (p.latitude === null || p.longitude === null) continue;
    if (Math.abs(p.latitude - lat) > degBound || Math.abs(p.longitude - lon) > degBound) continue;
    const d = haversine(lat, lon, p.latitude, p.longitude);
    if (d <= PROXIMITY_M && dice(norm, normalise(p.title)) >= PROXIMITY_MIN_SIM) {
      return { title: p.title, park: p.park, signal: "proximity", distanceM: Math.round(d) };
    }
  }

  // Name pass
  let best: { title: string; park: string; score: number; distanceM: number | null } | null = null;
  for (const p of places) {
    const sim = dice(norm, normalise(p.title));
    if (sim < NAME_THRESHOLD) continue;
    const d = (p.latitude !== null && p.longitude !== null)
      ? haversine(lat, lon, p.latitude, p.longitude) : null;
    if (d !== null && d > NAME_MAX_DIST_M) continue;
    if (!best || sim > best.score) {
      best = { title: p.title, park: p.park, score: sim, distanceM: d ? Math.round(d) : null };
    }
  }
  if (best) return { title: best.title, park: best.park, signal: "name", distanceM: best.distanceM };

  return null;
}

// ── Census: incorporated place lookup ─────────────────────────────────────────

async function fetchIncorporatedPlace(lat: number, lon: number): Promise<string | null> {
  const url = new URL(CENSUS_URL);
  url.searchParams.set("x", String(lon));
  url.searchParams.set("y", String(lat));
  url.searchParams.set("benchmark", "Public_AR_Current");
  url.searchParams.set("vintage", "Current_Current");
  url.searchParams.set("layers", "Incorporated Places");
  url.searchParams.set("format", "json");
  try {
    const resp = await fetch(url.toString());
    if (!resp.ok) return null;
    const data = await resp.json();
    const places: { NAME: string }[] = data?.result?.geographies?.["Incorporated Places"] ?? [];
    return places.length > 0 ? places[0].NAME : null;
  } catch { return null; }
}

// ── Reverse geocode one point ─────────────────────────────────────────────────

async function reverseGeocode(lat: number, lon: number): Promise<{
  status: string;
  fields: Record<string, string | null>;
} | null> {
  const url = new URL(GEOCODE_URL);
  url.searchParams.set("latlng", `${lat},${lon}`);
  url.searchParams.set("key", GOOGLE_KEY);

  let data: { status: string; results: { address_components: GeoComponent[] }[] };
  try {
    const resp = await fetch(url.toString());
    data = await resp.json();
  } catch { return null; }

  if (data.status !== "OK")      return { status: data.status ?? "ERROR", fields: {} };
  if (!data.results?.length)     return { status: "ZERO_RESULTS", fields: {} };

  const components = data.results[0].address_components;
  const locality   = component(components, "locality");
  const county     = component(components, "administrative_area_level_2");
  const state      = component(components, "administrative_area_level_1");

  const censusPlace = await fetchIncorporatedPlace(lat, lon);

  return {
    status: "OK",
    fields: {
      street_number:             component(components, "street_number"),
      route:                     component(components, "route"),
      city:                      locality,
      county,
      state,
      zip:                       component(components, "postal_code"),
      governing_city:            censusPlace ? stripPlaceSuffix(censusPlace) : null,
      governing_county:          county,
      governing_state:           state,
      governing_jurisdiction:    censusPlace ? "governing city" : "governing county",
      census_incorporated_place: censusPlace ?? "UNINCORPORATED",
      geocode_status:            "OK",
    },
  };
}

// ── Concurrency limiter ───────────────────────────────────────────────────────

async function pLimit<T>(tasks: (() => Promise<T>)[], limit: number): Promise<T[]> {
  const results: T[] = new Array(tasks.length);
  let index = 0;
  async function worker() {
    while (index < tasks.length) {
      const i = index++;
      results[i] = await tasks[i]();
    }
  }
  await Promise.all(Array.from({ length: limit }, worker));
  return results;
}

// ── Handler ───────────────────────────────────────────────────────────────────

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { state?: string; county?: string; limit?: number } = {};
  try { body = await req.json(); } catch { /* empty body */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  if (!GOOGLE_KEY) return json({ error: "GOOGLE_MAPS_API_KEY secret not available" }, 500);

  // ── Fetch rows to geocode ───────────────────────────────────────────────────
  let query = supabase
    .from("beaches_staging_new")
    .select("id, display_name, latitude, longitude")
    .is("geocode_status", null)
    .is("review_status", null)
    .limit(body.limit ?? DEFAULT_LIMIT);

  if (body.state)  query = query.eq("state", body.state);
  if (body.county) query = query.eq("county", body.county);

  const { data: rows, error: fetchError } = await query;
  if (fetchError) return json({ error: fetchError.message }, 500);
  if (!rows?.length) return json({ processed: 0, succeeded: 0, zero_results: 0, errors: 0, federal_matches: 0, state_matches: 0 });

  // ── Load NPS and CSP place lists ────────────────────────────────────────────
  const [npsResult, cspResult] = await Promise.all([
    supabase.from("nps_places").select("title, latitude, longitude, park_full_name"),
    supabase.from("csp_places").select("park_name, latitude, longitude"),
  ]);

  const npsPlaces: Place[] = (npsResult.data ?? []).map(p => ({
    title: p.title, latitude: p.latitude, longitude: p.longitude, park: p.park_full_name,
  }));
  const cspPlaces: Place[] = (cspResult.data ?? []).map(p => ({
    title: p.park_name, latitude: p.latitude, longitude: p.longitude, park: p.park_name,
  }));

  // ── Geocode concurrently ────────────────────────────────────────────────────
  const tasks = rows.map(row => () =>
    reverseGeocode(row.latitude, row.longitude)
      .then(result => ({ id: row.id, display_name: row.display_name, result }))
  );

  const geocoded = await pLimit(tasks, CONCURRENCY);

  // ── Build updates with jurisdiction overrides ───────────────────────────────
  let succeeded     = 0;
  let zeroResults   = 0;
  let errors        = 0;
  let federalCount  = 0;
  let stateCount    = 0;
  const statuses: Record<string, number> = {};

  const updates = geocoded.map(({ id, display_name, result }) => {
    const status = result?.status ?? "ERROR";
    statuses[status] = (statuses[status] ?? 0) + 1;

    if (!result || status === "ERROR") { errors++; return { id, geocode_status: "ERROR" }; }
    if (status === "ZERO_RESULTS")     { zeroResults++; return { id, geocode_status: "ZERO_RESULTS" }; }
    if (status !== "OK")               { errors++; return { id, geocode_status: status }; }

    succeeded++;
    const fields: Record<string, string | null> = { ...result.fields };

    const lat = rows.find(r => r.id === id)!.latitude;
    const lon = rows.find(r => r.id === id)!.longitude;

    // NPS override (federal)
    const npsMatch = matchPlace(display_name, lat, lon, npsPlaces);
    if (npsMatch) {
      federalCount++;
      return {
        id, ...fields,
        governing_jurisdiction: "governing federal",
        governing_body:         npsMatch.park,
        governing_body_source:  "nps_api",
        governing_body_notes:   `NPS match via ${npsMatch.signal}: "${npsMatch.title}"${npsMatch.distanceM !== null ? ` (${npsMatch.distanceM}m)` : ""} in ${npsMatch.park}.`,
        nps_match_score:        String(1),
        nps_match_name:         npsMatch.title,
        nps_match_park:         npsMatch.park,
        review_status:          "ready",
        review_notes:           "Governing jurisdiction confirmed federal via NPS API match.",
      };
    }

    // CSP override (state)
    const cspMatch = matchPlace(display_name, lat, lon, cspPlaces);
    if (cspMatch) {
      stateCount++;
      return {
        id, ...fields,
        governing_jurisdiction: "governing state",
        governing_body:         cspMatch.park,
        governing_body_source:  "csp_arcgis",
        governing_body_notes:   `CSP match via ${cspMatch.signal}: "${cspMatch.title}"${cspMatch.distanceM !== null ? ` (${cspMatch.distanceM}m)` : ""}.`,
        csp_match_score:        String(1),
        csp_match_name:         cspMatch.title,
        review_status:          "ready",
        review_notes:           "Governing jurisdiction confirmed state via CSP ArcGIS match.",
      };
    }

    return { id, ...fields };
  });

  // ── Write ───────────────────────────────────────────────────────────────────
  const upsertErrors: string[] = [];
  const writeTasks = updates.map(({ id, ...fields }) => async () => {
    const { error } = await supabase
      .from("beaches_staging_new").update(fields).eq("id", id);
    if (error) upsertErrors.push(`id ${id}: ${error.message}`);
  });
  await pLimit(writeTasks, 10);

  return json({
    processed:       rows.length,
    succeeded,
    zero_results:    zeroResults,
    errors,
    federal_matches: federalCount,
    state_matches:   stateCount,
    statuses,
    upsert_errors:   upsertErrors,
  });
});
