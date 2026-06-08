// refresh-tide-stations/index.ts
//
// Per-station tide cache loader for the tide_grid_hourly reference layer
// (migration 20260607j). Replaces the per-beach NOAA fetch path inside
// daily-beach-refresh, which was busting the 150s Edge timeout at ~11%
// CA coverage. The grid is 13× more efficient — 296 stations cover
// 3,899 active beaches.
//
// POST body knobs:
//   { "limit": 200 }              cells per invocation cap (default 250)
//   { "force_full": true }        bypass stale filter — refetch all
//   { "station_ids": [...] }      restrict to specific station_ids
//
// Auth: x-admin-secret. Internal loader, not consumer-facing.

// deno-lint-ignore-file no-explicit-any
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ADMIN_SECRET         = Deno.env.get("ADMIN_SECRET") ?? "";

const NOAA_BASE = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter";

// Window: yesterday → +14 days. Yesterday gives us a small bit of past
// coverage so a consumer querying "today, 0:00 onward" doesn't race the
// loader's day rollover.
const PAST_DAYS    = 1;
const FORECAST_DAYS = 14;

const DEFAULT_LIMIT = 250;
const FETCH_TIMEOUT_MS = 20_000;
const FETCH_MAX_RETRIES = 3;
const FETCH_BACKOFF_MS = [0, 1000, 3000];
const UPSERT_CHUNK = 1000;
// Modest parallelism — NOAA tolerates this fine, total job is short.
const CONCURRENCY = 4;

Deno.serve(async (req: Request): Promise<Response> => {
  const headers = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers });

  const provided = req.headers.get("x-admin-secret") ?? "";
  if (!ADMIN_SECRET || provided !== ADMIN_SECRET) {
    return new Response(JSON.stringify({ error: "unauthorized" }), { status: 401, headers });
  }

  let body: any = {};
  try { body = await req.json(); } catch (_e) {}

  const limit       = Math.min(Number(body.limit ?? DEFAULT_LIMIT), 500);
  const forceFull   = !!body.force_full;
  const explicitIds: string[] | null = Array.isArray(body.station_ids) && body.station_ids.length
    ? body.station_ids.map(String) : null;

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    auth: { persistSession: false },
  });

  const t0 = Date.now();

  // ─── 1. Pick stations ───────────────────────────────────────────────
  let stations: string[] = [];
  if (explicitIds) {
    stations = explicitIds;
  } else {
    const { data, error } = await supabase.rpc("_tide_picker_stale_stations", {
      p_limit: limit, p_force: forceFull,
    });
    if (error) {
      return new Response(
        JSON.stringify({ error: "picker_failed", detail: error.message }),
        { status: 500, headers });
    }
    stations = (data ?? []).map((r: any) => String(r.station_id));
  }

  if (stations.length === 0) {
    return new Response(JSON.stringify({
      skipped: true, reason: "no stale stations",
      elapsed_ms: Date.now() - t0,
    }), { status: 200, headers });
  }

  // ─── 2. Date window ─────────────────────────────────────────────────
  const now = new Date();
  const beginDate = formatNoaaDate(addDays(now, -PAST_DAYS));
  const endDate   = formatNoaaDate(addDays(now,  FORECAST_DAYS));

  // ─── 3. Fetch + upsert, with bounded concurrency ────────────────────
  let totalUpserted = 0;
  let stationsOk    = 0;
  let stationsErr   = 0;
  const errors: Array<{ station: string; err: string }> = [];
  const queue = [...stations];
  const workers = Array.from({ length: Math.min(CONCURRENCY, stations.length) }, () =>
    worker(supabase, queue, beginDate, endDate));
  const fetchedAt = new Date().toISOString();
  for await (const result of merge(workers)) {
    if (result.ok) {
      totalUpserted += result.upserted;
      stationsOk += 1;
    } else {
      stationsErr += 1;
      errors.push({ station: result.station, err: result.err });
    }
    // Bump last_fetched_at (or last_error) on inventory regardless so
    // the next picker tick doesn't keep returning the same failing
    // station forever. Errors keep last_fetched_at as-is so retry
    // remains prioritized.
    const patch: any = { last_error: result.ok ? null : result.err.slice(0, 200) };
    if (result.ok) patch.last_fetched_at = fetchedAt;
    await supabase
      .from("tide_station_inventory")
      .update(patch)
      .eq("station_id", result.station);
  }

  return new Response(JSON.stringify({
    ok: true,
    stations_total:  stations.length,
    stations_ok:     stationsOk,
    stations_error:  stationsErr,
    rows_upserted:   totalUpserted,
    window:          `${beginDate} → ${endDate}`,
    elapsed_ms:      Date.now() - t0,
    errors_sample:   errors.slice(0, 5),
  }), { status: 200, headers });
});

// ─── Worker ───────────────────────────────────────────────────────────
async function* worker(
  supabase: any,
  queue: string[],
  beginDate: string,
  endDate: string,
): AsyncGenerator<{ ok: boolean; station: string; upserted: number; err: string }> {
  while (queue.length > 0) {
    const station = queue.shift();
    if (!station) break;
    try {
      const rows = await fetchTideForStation(station, beginDate, endDate);
      let upserted = 0;
      for (let i = 0; i < rows.length; i += UPSERT_CHUNK) {
        const chunk = rows.slice(i, i + UPSERT_CHUNK);
        const { error } = await supabase
          .from("tide_grid_hourly")
          .upsert(chunk, { onConflict: "station_id,forecast_ts" });
        if (error) throw new Error(`upsert: ${error.message}`);
        upserted += chunk.length;
      }
      yield { ok: true, station, upserted, err: "" };
    } catch (e) {
      yield { ok: false, station, upserted: 0, err: (e as Error).message };
    }
  }
}

// Merge multiple async generators round-robin-ish into one.
async function* merge<T>(gens: AsyncGenerator<T>[]): AsyncGenerator<T> {
  const promises = gens.map((g, i) => g.next().then(r => ({ r, i })));
  const live = new Set(gens.map((_, i) => i));
  while (live.size > 0) {
    const winners = await Promise.race(
      Array.from(live).map(i => promises[i].then(p => ({ p, i }))),
    );
    const { p, i } = winners;
    if (p.r.done) {
      live.delete(i);
    } else {
      yield p.r.value as T;
      promises[i] = gens[i].next().then(r => ({ r, i }));
    }
  }
}

// ─── NOAA fetch ───────────────────────────────────────────────────────
async function fetchTideForStation(
  station: string, beginDate: string, endDate: string,
): Promise<Array<{ station_id: string; forecast_ts: string; tide_height_ft: number; tide_direction: string | null; source: string }>> {
  const params = new URLSearchParams({
    station,
    product:    "predictions",
    datum:      "MLLW",
    units:      "english",     // feet
    time_zone:  "gmt",          // request UTC so we store timestamptz directly
    interval:   "h",
    format:     "json",
    begin_date: beginDate,
    end_date:   endDate,
  });
  const url = `${NOAA_BASE}?${params}`;

  let lastErr = "";
  for (let attempt = 0; attempt < FETCH_MAX_RETRIES; attempt++) {
    if (FETCH_BACKOFF_MS[attempt] > 0) {
      await new Promise(r => setTimeout(r, FETCH_BACKOFF_MS[attempt]));
    }
    try {
      const resp = await fetch(url, { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) });
      if (!resp.ok) {
        const txt = await resp.text();
        lastErr = `HTTP ${resp.status}: ${txt.slice(0, 120)}`;
        if (resp.status >= 400 && resp.status < 500) break;
        continue;
      }
      const json = await resp.json();
      if (json.error) {
        lastErr = `noaa: ${json.error.message}`;
        break;
      }
      if (!Array.isArray(json.predictions)) {
        lastErr = "noaa: missing predictions";
        break;
      }
      const rows = (json.predictions as Array<{ t: string; v: string }>)
        .map(p => {
          // NOAA returns t as "YYYY-MM-DD HH:MM" (in GMT per our request).
          // Convert to ISO timestamptz.
          const iso = p.t.replace(" ", "T") + ":00Z";
          const h = parseFloat(p.v);
          if (!Number.isFinite(h)) return null;
          return {
            station_id:     station,
            forecast_ts:    iso,
            tide_height_ft: h,
            tide_direction: null as string | null,   // computed below
            source:         "noaa_coops_predictions",
          };
        })
        .filter(Boolean) as Array<any>;

      // Compute tide_direction by looking at neighbors. Last row gets
      // null (no forward neighbor to compare).
      rows.sort((a, b) => a.forecast_ts < b.forecast_ts ? -1 : 1);
      for (let i = 0; i < rows.length - 1; i++) {
        rows[i].tide_direction = rows[i + 1].tide_height_ft > rows[i].tide_height_ft
          ? "rising" : (rows[i + 1].tide_height_ft < rows[i].tide_height_ft ? "falling" : null);
      }
      return rows;
    } catch (e) {
      lastErr = (e as Error).message;
    }
  }
  throw new Error(lastErr || "fetch failed");
}

function formatNoaaDate(d: Date): string {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}${m}${dd}`;
}
function addDays(d: Date, days: number): Date {
  const r = new Date(d);
  r.setUTCDate(r.getUTCDate() + days);
  return r;
}
