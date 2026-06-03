// audit-noaa-stations/index.ts
// Fleet-wide health audit of every distinct NOAA tide station id
// referenced from beaches_gold. Probes the NOAA mdapi station endpoint
// for each station and reports broken ones (non-2xx) along with the
// number of affected beaches.
//
// Why this exists: beaches_gold.noaa_station_id is silently load-bearing.
// NOAA periodically retires/renumbers stations. When a station 404s,
// daily-beach-refresh writes neutral tide scores (status='go') with no
// alarm — the beach scoring just quietly drops tide signal.
//
// Probe target:
//   https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/<id>.json
//
// Concurrency: chunks of 10 in series (per [[chunked-subprocess]]).
// Each NOAA fetch ~200-500ms; 50-100 distinct station ids → ~5-15 sec.
// Fits the 150s edge-fn budget comfortably.
//
// Security: x-admin-secret + per-IP rate limit (same as every other
// admin/audit function). Fires from orch via _orch_w_noaa_station_health_audit.
//
// Response (capped to ~1KB so net._http_response stays scannable):
//   {
//     checked: N,
//     ok: M,
//     broken_count: K,
//     broken: [ {station_id, status_code, beach_count, beach_names: [...max 5]}, ... max 10 ],
//     truncated: bool
//   }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }  from "../_shared/cors.ts";
import { requireAdmin } from "../_shared/admin-auth.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const NOAA_MDAPI_BASE = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations";
const CHUNK_SIZE      = 10;
const FETCH_TIMEOUT_MS = 8_000;     // per probe
const MAX_BROKEN_REPORTED = 10;     // cap response size
const MAX_BEACH_NAMES_PER_STATION = 5;

interface StationRow {
  noaa_station_id: string;
  fids: number[];
  names: string[];
}

interface ProbeResult {
  station_id: string;
  status_code: number;
  ok: boolean;
  error?: string;
}

async function probeStation(id: string): Promise<ProbeResult> {
  const url = `${NOAA_MDAPI_BASE}/${encodeURIComponent(id)}.json`;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const resp = await fetch(url, { signal: ctrl.signal });
    // NOAA returns 200 even for known-bad ids in some envelopes, but the
    // body carries an error. Treat HTTP non-2xx as broken; we don't parse
    // the body to keep the audit cheap.
    return { station_id: id, status_code: resp.status, ok: resp.ok };
  } catch (e) {
    return {
      station_id: id,
      status_code: 0,
      ok: false,
      error: (e as Error).message ?? "fetch failed",
    };
  } finally {
    clearTimeout(timer);
  }
}

async function probeAll(ids: string[]): Promise<ProbeResult[]> {
  const out: ProbeResult[] = [];
  for (let i = 0; i < ids.length; i += CHUNK_SIZE) {
    const chunk = ids.slice(i, i + CHUNK_SIZE);
    const results = await Promise.all(chunk.map(probeStation));
    out.push(...results);
  }
  return out;
}

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (b: unknown, s = 200) => new Response(JSON.stringify(b), { status: s, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST")    return json({ error: "POST only" }, 405);

  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // Build per-station beach inventory in one round-trip.
  const { data: beaches, error: qErr } = await supabase
    .from("beaches_gold")
    .select("fid, name, noaa_station_id")
    .eq("is_active", true)
    .not("noaa_station_id", "is", null);

  if (qErr) return json({ error: `beaches_gold query failed: ${qErr.message}` }, 500);

  const byStation = new Map<string, StationRow>();
  for (const b of (beaches ?? []) as Array<{ fid: number; name: string | null; noaa_station_id: string | null }>) {
    const id = (b.noaa_station_id ?? "").trim();
    if (!id) continue;
    let row = byStation.get(id);
    if (!row) {
      row = { noaa_station_id: id, fids: [], names: [] };
      byStation.set(id, row);
    }
    row.fids.push(b.fid);
    if (b.name) row.names.push(b.name);
  }

  const stationIds = Array.from(byStation.keys()).sort();
  if (stationIds.length === 0) {
    return json({ checked: 0, ok: 0, broken_count: 0, broken: [], truncated: false });
  }

  const probes = await probeAll(stationIds);
  const broken: Array<{
    station_id: string;
    status_code: number;
    error?: string;
    beach_count: number;
    beach_names: string[];
  }> = [];

  let okCount = 0;
  for (const p of probes) {
    if (p.ok) {
      okCount++;
      continue;
    }
    const row = byStation.get(p.station_id)!;
    broken.push({
      station_id: p.station_id,
      status_code: p.status_code,
      ...(p.error ? { error: p.error.slice(0, 80) } : {}),
      beach_count: row.fids.length,
      beach_names: row.names.slice(0, MAX_BEACH_NAMES_PER_STATION),
    });
  }

  // Sort broken by impact (most affected beaches first), then station_id.
  broken.sort((a, b) => b.beach_count - a.beach_count || a.station_id.localeCompare(b.station_id));
  const truncated = broken.length > MAX_BROKEN_REPORTED;
  const brokenReported = broken.slice(0, MAX_BROKEN_REPORTED);

  return json({
    checked:      stationIds.length,
    ok:           okCount,
    broken_count: broken.length,
    broken:       brokenReported,
    truncated,
  });
});
