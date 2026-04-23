// admin-find-noaa-station/index.ts
// Finds the closest NOAA CO-OPS tide-prediction station to a given lat/lon.
// Mirrors v2-noaa-station-match's parameters (reference stations only,
// MAX_DISTANCE_M = 50 km) so the admin editor picks the same station the
// pipeline would for a fresh beach.
//
// Security model: same as other admin-* functions (obscure URL,
// service-role server-side, no auth layer). See admin-update-beach.
//
// POST { latitude: number, longitude: number, state_code?: string,
//        max_distance_m?: number }
// Returns { station: { id, name, lat, lon, state }, distance_m, stations_loaded }
// On no-match within radius:
//   { station: null, closest: { ... }, distance_m, reason: 'too_far', stations_loaded }
// On error: { error }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }  from "../_shared/cors.ts";
import { requireAdmin } from "../_shared/admin-auth.ts";
import { requireSource } from "../_shared/config.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const DEFAULT_MAX_DISTANCE_M = 50_000;  // matches v2-noaa-station-match

interface Station { id: string; name: string; lat: number; lon: number; state: string; }

async function loadStations(url: string, stateCode: string | null): Promise<Station[]> {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`NOAA station list HTTP ${resp.status}`);
  const data = await resp.json();
  const all = data?.stations ?? [];
  return all
    .filter((s: { state?: string; reference_id?: string }) => {
      if (s.reference_id) return false;                             // subordinate stations — skip
      if (stateCode && s.state !== stateCode) return false;         // state filter when provided
      return true;
    })
    .map((s: { id: string; name: string; lat: number; lng: number; state: string }) => ({
      id:    String(s.id),
      name:  String(s.name),
      lat:   Number(s.lat),
      lon:   Number(s.lng),
      state: String(s.state ?? ""),
    }))
    .filter((s: Station) => Number.isFinite(s.lat) && Number.isFinite(s.lon));
}

function haversine(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 6_371_000;
  const φ1 = lat1 * Math.PI / 180, φ2 = lat2 * Math.PI / 180;
  const Δφ = (lat2 - lat1) * Math.PI / 180, Δλ = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(Δφ / 2) ** 2 + Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function findClosest(lat: number, lon: number, stations: Station[]): { station: Station; dist_m: number } | null {
  let best: { station: Station; dist_m: number } | null = null;
  for (const s of stations) {
    const d = haversine(lat, lon, s.lat, s.lon);
    if (!best || d < best.dist_m) best = { station: s, dist_m: d };
  }
  return best;
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (b: unknown, s = 200) => new Response(JSON.stringify(b), { status: s, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let body: { latitude?: number; longitude?: number; state_code?: string; max_distance_m?: number };
  try { body = await req.json(); } catch { return json({ error: "Invalid JSON" }, 400); }

  const lat = Number(body.latitude);
  const lon = Number(body.longitude);
  if (!Number.isFinite(lat) || lat < -90 || lat > 90) {
    return json({ error: "latitude required (number between -90 and 90)" }, 400);
  }
  if (!Number.isFinite(lon) || lon < -180 || lon > 180) {
    return json({ error: "longitude required (number between -180 and 180)" }, 400);
  }

  const stateCode = body.state_code ? body.state_code.toUpperCase() : null;
  const maxDist   = body.max_distance_m ?? DEFAULT_MAX_DISTANCE_M;
  const supabase  = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  let source;
  try { source = await requireSource(supabase, "noaa_tide_stations"); }
  catch (e) { return json({ error: (e as Error).message }, 500); }

  let stations: Station[];
  try { stations = await loadStations(source.url, stateCode); }
  catch (e) { return json({ error: (e as Error).message }, 502); }
  if (stations.length === 0) {
    return json({ error: `No reference tide stations loaded${stateCode ? ` for state '${stateCode}'` : ""}` }, 404);
  }

  const hit = findClosest(lat, lon, stations);
  if (!hit) return json({ error: "No stations to compare against" }, 500);

  if (hit.dist_m > maxDist) {
    return json({
      station:         null,
      closest:         { id: hit.station.id, name: hit.station.name, state: hit.station.state },
      distance_m:      Math.round(hit.dist_m),
      reason:          "too_far",
      max_distance_m:  maxDist,
      stations_loaded: stations.length,
    });
  }

  return json({
    station:         hit.station,
    distance_m:      Math.round(hit.dist_m),
    stations_loaded: stations.length,
  });
});
