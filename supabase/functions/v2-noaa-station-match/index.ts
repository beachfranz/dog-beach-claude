// v2-noaa-station-match/index.ts
// Assigns the closest NOAA CO-OPS tide-prediction station to each beach in
// beaches_staging_new. Mirrors the noaa_station_id column used by the live
// beaches table for tide data.
//
// Process:
//   1. Fetch all CA tide-prediction stations from NOAA CO-OPS MD API (~192)
//   2. For each ready beach, compute haversine distance to every station
//      and pick the closest.
//   3. Skip beaches where the closest station is > MAX_DISTANCE_M — those
//      are inland lake/reservoir beaches without tides.
//
// Writes: noaa_station_id (text), noaa_station_name, noaa_station_distance_m.
//
// Safe to re-run: only writes the three NOAA columns; idempotent when station
// list hasn't changed.
//
// POST { dry_run?: boolean, max_distance_m?: number, limit?: number }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import { requireSource, stateCodeFromName } from "../_shared/config.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const DEFAULT_MAX_DISTANCE_M = 50_000;  // 50km — beyond this is inland

interface Station { id: string; name: string; lat: number; lon: number; }

async function loadStations(url: string, stateCode: string): Promise<Station[]> {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`NOAA station list HTTP ${resp.status}`);
  const data = await resp.json();
  const all = data?.stations ?? [];
  // Filter to reference stations only in the requested state. Subordinate
  // stations (non-empty reference_id) don't serve direct tide predictions via
  // daily-beach-refresh's NOAA call.
  return all
    .filter((s: { state?: string; reference_id?: string }) =>
      s.state === stateCode && !s.reference_id)
    .map((s: { id: string; name: string; lat: number; lng: number }) => ({
      id:   String(s.id),
      name: String(s.name),
      lat:  Number(s.lat),
      lon:  Number(s.lng),
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

  let body: { state_code?: string; dry_run?: boolean; max_distance_m?: number; limit?: number } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const stateCode = body.state_code ?? "CA";
  const maxDist   = body.max_distance_m ?? DEFAULT_MAX_DISTANCE_M;
  const supabase  = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  let source;
  try { source = await requireSource(supabase, "noaa_tide_stations"); }
  catch (e) { return json({ error: (e as Error).message }, 500); }

  const stations = await loadStations(source.url, stateCode);
  if (stations.length === 0) return json({ state_code: stateCode, error: `no reference tide stations returned for ${stateCode}` }, 500);

  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("id, display_name, latitude, longitude, state")
    .eq("review_status", "ready")
    .not("latitude", "is", null)
    .not("longitude", "is", null)
    .limit(body.limit ?? 5000);
  if (error) return json({ error: error.message }, 500);
  const filtered = (rows ?? []).filter(r => stateCodeFromName(r.state) === stateCode);
  if (!filtered.length) return json({ state_code: stateCode, stations_loaded: stations.length, processed: 0 });

  const matches: Array<{ id: number; display_name: string; station: Station; dist_m: number }> = [];
  const skipped: Array<{ id: number; display_name: string; dist_m: number }> = [];

  for (const r of filtered) {
    const hit = findClosest(r.latitude, r.longitude, stations);
    if (!hit) continue;
    if (hit.dist_m > maxDist) {
      skipped.push({ id: r.id, display_name: r.display_name, dist_m: Math.round(hit.dist_m) });
      continue;
    }
    matches.push({ id: r.id, display_name: r.display_name, station: hit.station, dist_m: hit.dist_m });
  }

  if (body.dry_run) {
    return json({
      dry_run:         true,
      stations_loaded: stations.length,
      processed:       filtered.length,
      matched:         matches.length,
      skipped_inland:  skipped.length,
      preview_match:   matches.slice(0, 10).map(m => ({
        display_name:    m.display_name,
        station_id:      m.station.id,
        station_name:    m.station.name,
        distance_m:      Math.round(m.dist_m),
      })),
      preview_skipped: skipped.slice(0, 10),
    });
  }

  let updated = 0;
  const writeErrors: string[] = [];
  for (const m of matches) {
    const { error } = await supabase
      .from("beaches_staging_new")
      .update({
        noaa_station_id:         m.station.id,
        noaa_station_name:       m.station.name,
        noaa_station_distance_m: Math.round(m.dist_m),
      })
      .eq("id", m.id);
    if (error) writeErrors.push(`id ${m.id}: ${error.message}`);
    else updated++;
  }

  return json({
    stations_loaded: stations.length,
    processed:       rows.length,
    matched:         matches.length,
    updated,
    skipped_inland:  skipped.length,
    errors:          writeErrors,
  });
});
