// find-staging-duplicates/index.ts
// Read-only scan of beaches_staging for duplicate candidates.
//
// Three detection methods:
//   1. Proximity     — records within proximity_threshold_m meters of each other
//   2. Name + city   — identical display_name + city (case-insensitive)
//   3. Address       — identical formatted_address (case-insensitive)
//
// POST { state?: string, quality_tier?: string, proximity_threshold_m?: number }
// Returns { summary, proximity_clusters, name_city_matches, address_matches }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const DEFAULT_THRESHOLD_M  = 50;
const POLICY_MATCH_RADIUS_M = 200;

// ── Haversine distance ────────────────────────────────────────────────────────

function distanceMeters(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R    = 6_371_000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a    = Math.sin(dLat / 2) ** 2
             + Math.cos(lat1 * Math.PI / 180)
             * Math.cos(lat2 * Math.PI / 180)
             * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// ── Union-Find for proximity clustering ───────────────────────────────────────

function makeUnionFind(n: number) {
  const parent = Array.from({ length: n }, (_, i) => i);
  function find(x: number): number {
    if (parent[x] !== x) parent[x] = find(parent[x]);
    return parent[x];
  }
  function union(x: number, y: number) {
    parent[find(x)] = find(y);
  }
  return { find, union };
}

// ── Types ─────────────────────────────────────────────────────────────────────

interface BeachRow {
  id:                number;
  display_name:      string;
  city:              string | null;
  county:            string | null;
  state:             string | null;
  latitude:          number;
  longitude:         number;
  formatted_address: string | null;
  quality_tier:      string;
  source_fid:        number | null;
}

interface ProdPolicy {
  display_name:    string;
  access_rule:     string | null;
  off_leash_flag:  boolean | null;
  leash_policy:    string | null;
}

interface RecordSummary {
  id:           number;
  display_name: string;
  city:         string | null;
  county:       string | null;
  latitude:     number;
  longitude:    number;
  quality_tier: string;
  source_fid:   number | null;
  dog_policy:   ProdPolicy | null;
}

// ── Handler ───────────────────────────────────────────────────────────────────

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")   return json({ error: "POST only" }, 405);

  let body: { state?: string; quality_tier?: string; proximity_threshold_m?: number } = {};
  try { body = await req.json(); } catch { /* empty body is fine */ }

  const {
    state,
    quality_tier,
    proximity_threshold_m = DEFAULT_THRESHOLD_M,
  } = body;

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // ── Fetch staging records ─────────────────────────────────────────────────

  let query = supabase
    .from("beaches_staging")
    .select("id, display_name, city, county, state, latitude, longitude, formatted_address, quality_tier, source_fid")
    .is("dedup_status", null)
    .limit(10000);

  if (state)        query = query.eq("state", state);
  if (quality_tier) query = query.eq("quality_tier", quality_tier);

  const { data: records, error } = await query;
  if (error) return json({ error: error.message }, 500);

  const rows = (records ?? []) as BeachRow[];

  // ── Fetch production beach policy data ───────────────────────────────────
  // Pull the full production beaches table (small) and match by proximity.

  const { data: prodBeaches } = await supabase
    .from("beaches")
    .select("display_name, latitude, longitude, access_rule, off_leash_flag, leash_policy");

  const prodRows = (prodBeaches ?? []) as Array<{
    display_name: string;
    latitude: number | null;
    longitude: number | null;
    access_rule: string | null;
    off_leash_flag: boolean | null;
    leash_policy: string | null;
  }>;

  function nearestProdPolicy(lat: number, lon: number): ProdPolicy | null {
    let best: ProdPolicy | null = null;
    let bestDist = POLICY_MATCH_RADIUS_M;
    for (const pb of prodRows) {
      if (pb.latitude == null || pb.longitude == null) continue;
      const d = distanceMeters(lat, lon, pb.latitude, pb.longitude);
      if (d < bestDist) {
        bestDist = d;
        best = {
          display_name:   pb.display_name,
          access_rule:    pb.access_rule,
          off_leash_flag: pb.off_leash_flag,
          leash_policy:   pb.leash_policy,
        };
      }
    }
    return best;
  }

  const toSummary = (r: BeachRow): RecordSummary => ({
    id: r.id, display_name: r.display_name, city: r.city, county: r.county,
    latitude: r.latitude, longitude: r.longitude,
    quality_tier: r.quality_tier, source_fid: r.source_fid,
    dog_policy: nearestProdPolicy(r.latitude, r.longitude),
  });

  // ── 1. Proximity clustering ───────────────────────────────────────────────

  const uf = makeUnionFind(rows.length);

  for (let i = 0; i < rows.length; i++) {
    for (let j = i + 1; j < rows.length; j++) {
      const d = distanceMeters(
        rows[i].latitude, rows[i].longitude,
        rows[j].latitude, rows[j].longitude,
      );
      if (d <= proximity_threshold_m) uf.union(i, j);
    }
  }

  const clusterMap: Map<number, number[]> = new Map();
  for (let i = 0; i < rows.length; i++) {
    const root = uf.find(i);
    if (!clusterMap.has(root)) clusterMap.set(root, []);
    clusterMap.get(root)!.push(i);
  }

  const proximityClusters = [];
  for (const [, members] of clusterMap) {
    if (members.length < 2) continue;
    let maxDist = 0;
    for (let a = 0; a < members.length; a++) {
      for (let b = a + 1; b < members.length; b++) {
        const d = distanceMeters(
          rows[members[a]].latitude, rows[members[a]].longitude,
          rows[members[b]].latitude, rows[members[b]].longitude,
        );
        if (d > maxDist) maxDist = d;
      }
    }
    proximityClusters.push({
      max_distance_m: Math.round(maxDist * 10) / 10,
      records: members.map(i => toSummary(rows[i])),
    });
  }
  proximityClusters.sort((a, b) => b.records.length - a.records.length);

  // ── 2. Name + city matches ────────────────────────────────────────────────

  const nameCityMap: Map<string, BeachRow[]> = new Map();
  for (const r of rows) {
    const key = `${r.display_name.toLowerCase().trim()}||${(r.city ?? "").toLowerCase().trim()}`;
    if (!nameCityMap.has(key)) nameCityMap.set(key, []);
    nameCityMap.get(key)!.push(r);
  }

  const nameCityMatches = [];
  for (const [key, group] of nameCityMap) {
    if (group.length < 2) continue;
    const [name, city] = key.split("||");
    nameCityMatches.push({
      display_name: name,
      city:         city || null,
      records:      group.map(toSummary),
    });
  }

  // ── 3. Address matches ────────────────────────────────────────────────────

  const addressMap: Map<string, BeachRow[]> = new Map();
  for (const r of rows) {
    if (!r.formatted_address) continue;
    const key = r.formatted_address.toLowerCase().trim();
    if (!addressMap.has(key)) addressMap.set(key, []);
    addressMap.get(key)!.push(r);
  }

  const addressMatches = [];
  for (const [addr, group] of addressMap) {
    if (group.length < 2) continue;
    addressMatches.push({
      formatted_address: addr,
      records: group.map(toSummary),
    });
  }

  // ── Summary ───────────────────────────────────────────────────────────────

  const proximityRecordIds = new Set(proximityClusters.flatMap(c => c.records.map(r => r.id)));
  const nameCityRecordIds  = new Set(nameCityMatches.flatMap(c => c.records.map(r => r.id)));
  const addressRecordIds   = new Set(addressMatches.flatMap(c => c.records.map(r => r.id)));

  return json({
    summary: {
      total_records:         rows.length,
      proximity_threshold_m,
      proximity_clusters:    proximityClusters.length,
      records_in_proximity:  proximityRecordIds.size,
      name_city_clusters:    nameCityMatches.length,
      records_in_name_city:  nameCityRecordIds.size,
      address_clusters:      addressMatches.length,
      records_in_address:    addressRecordIds.size,
    },
    proximity_clusters: proximityClusters,
    name_city_matches:  nameCityMatches,
    address_matches:    addressMatches,
  });
});
