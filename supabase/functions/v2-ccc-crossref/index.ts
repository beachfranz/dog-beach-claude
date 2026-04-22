// v2-ccc-crossref/index.ts
// Pipeline stage 7 — cross-reference against California Coastal Commission
// Public Access Points. CCC is a curated human-maintained registry of
// actual public coastal access points.
//
// This is NOT a jurisdiction classifier. It:
//   (a) validates that the record is a real beach / coastal access point
//   (b) populates ccc_dog_friendly hint (Yes/No/null) for Phase 2 dog policy
//   (c) stores the CCC-matched name for cross-reference
//
// Runs on ALL unlocked records (does not skip already-classified ones —
// even locked federal/state records benefit from getting the dog hint).
//
// Match criteria: CCC point within 200m + dice similarity >= 0.4 on names,
// OR very-close proximity (<=50m) regardless of name.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import { getSource, getStateConfig, stateCodeFromName } from "../_shared/config.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const PROXIMITY_NAME_M  = 200;
const PROXIMITY_STRONG  = 50;
const NAME_MIN_SIM      = 0.4;

interface CccPoint { name: string; lat: number; lon: number; dog: string | null; county: string | null; }

async function loadCccPoints(url: string): Promise<CccPoint[]> {
  const params = new URLSearchParams({
    where:             "1=1",
    outFields:         "Name,LATITUDE,LONGITUDE,DOG_FRIEND,COUNTY",
    returnGeometry:    "false",
    f:                 "json",
    resultRecordCount: "5000",
  });
  const resp = await fetch(`${url}?${params}`);
  const data = await resp.json();
  const features = data?.features ?? [];
  return features
    .map((f: { attributes: Record<string, unknown> }) => ({
      name: String(f.attributes.Name ?? ""),
      lat:  Number(f.attributes.LATITUDE),
      lon:  Number(f.attributes.LONGITUDE),
      dog:  (f.attributes.DOG_FRIEND ? String(f.attributes.DOG_FRIEND).trim() : "") || null,
      county: f.attributes.COUNTY ? String(f.attributes.COUNTY) : null,
    }))
    .filter((p: CccPoint) => Number.isFinite(p.lat) && Number.isFinite(p.lon));
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

function matchCcc(name: string, lat: number, lon: number, points: CccPoint[]):
  { point: CccPoint; distance_m: number; name_sim: number } | null
{
  const norm = normalise(name);
  const degBound = PROXIMITY_NAME_M / 100_000;
  let best: { point: CccPoint; distance_m: number; name_sim: number } | null = null;

  for (const p of points) {
    if (Math.abs(p.lat - lat) > degBound || Math.abs(p.lon - lon) > degBound) continue;
    const d = haversine(lat, lon, p.lat, p.lon);
    if (d > PROXIMITY_NAME_M) continue;

    const sim = dice(norm, normalise(p.name));
    const acceptable = d <= PROXIMITY_STRONG || sim >= NAME_MIN_SIM;
    if (!acceptable) continue;

    if (!best || d < best.distance_m) {
      best = { point: p, distance_m: d, name_sim: sim };
    }
  }
  return best;
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { state_code?: string; dry_run?: boolean; limit?: number } = {};
  try { body = await req.json(); } catch { /* empty */ }

  const stateCode = body.state_code ?? "CA";
  const supabase  = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  const cfg = await getStateConfig(supabase, stateCode);
  if (!cfg?.has_coastal_access_source) {
    return json({ state_code: stateCode, skipped: true, reason: "state has no coastal_access_points source configured" });
  }

  const source = await getSource(supabase, "coastal_access_points", stateCode);
  if (!source) return json({ error: `No pipeline_sources row for coastal_access_points (state=${stateCode})` }, 500);

  const cccPoints = await loadCccPoints(source.url);
  if (!cccPoints.length) return json({ error: "coastal access load returned no features" }, 500);

  const { data: rows, error } = await supabase
    .from("beaches_staging_new")
    .select("id, display_name, latitude, longitude, state")
    .or("review_status.is.null,review_status.eq.ready")
    .not("latitude", "is", null)
    .not("longitude", "is", null)
    .limit(body.limit ?? 5000);
  if (error) return json({ error: error.message }, 500);
  const filtered = (rows ?? []).filter(r => stateCodeFromName(r.state) === stateCode);
  if (!filtered.length) return json({ state_code: stateCode, processed: 0, matched: 0, updated: 0, ccc_loaded: cccPoints.length });

  const matches: Array<{
    id: number; display_name: string; ccc_name: string; distance_m: number;
    sim: number; dog: string | null;
  }> = [];

  for (const r of filtered) {
    const m = matchCcc(r.display_name, r.latitude, r.longitude, cccPoints);
    if (m) {
      matches.push({
        id:           r.id,
        display_name: r.display_name,
        ccc_name:     m.point.name,
        distance_m:   Math.round(m.distance_m),
        sim:          Math.round(m.name_sim * 100) / 100,
        dog:          m.point.dog,
      });
    }
  }

  if (body.dry_run) {
    const dogCount = {
      yes: matches.filter(m => m.dog === "Yes").length,
      no:  matches.filter(m => m.dog === "No").length,
      unknown: matches.filter(m => m.dog !== "Yes" && m.dog !== "No").length,
    };
    return json({
      dry_run:    true,
      state_code: stateCode,
      ccc_loaded: cccPoints.length,
      processed:  filtered.length,
      matched:    matches.length,
      dog_hint:   dogCount,
      preview:    matches.slice(0, 30),
    });
  }

  let updated = 0;
  const writeErrors: string[] = [];
  for (const m of matches) {
    const { error } = await supabase
      .from("beaches_staging_new")
      .update({
        ccc_match_name:       m.ccc_name,
        ccc_match_distance_m: m.distance_m,
        ccc_dog_friendly:     m.dog,
      })
      .eq("id", m.id);
    if (error) writeErrors.push(`id ${m.id}: ${error.message}`);
    else updated++;
  }

  return json({
    state_code: stateCode,
    ccc_loaded: cccPoints.length,
    processed:  filtered.length,
    matched:    matches.length,
    updated,
    errors:     writeErrors,
  });
});
