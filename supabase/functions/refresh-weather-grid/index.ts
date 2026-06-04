// refresh-weather-grid/index.ts
//
// Tiered weather-grid refresh — per-cell forecast-horizon staleness model.
//
// Three crons own disjoint forecast windows so we don't redundantly refresh
// hours another tier handles:
//
//   t1  next 48h          every 1h   forecast_days=2, past_days=0
//   t2  hours 48-96       every 6h   forecast_days=4, past_days=0, upsert ts>=now+48h
//   t3  hours 96-168      every 12h  forecast_days=7, past_days=3, upsert ts>=now+96h
//                         + past_days=3 observations (sole owner)
//
// Picker is a flat scan of weather_grid on the tier's last_fetched_tX column
// (sub-100ms). Each batch's bumper updates that column for the cells whose
// Open-Meteo fetch succeeded so the next picker sees them as fresh.
//
// Per-invocation cell cap (default 200) keeps the function under Supabase
// Edge 150s. Repeats until result.skipped=true (cron orchestrator).
//
// Knobs (POST body):
//   { "tier": "t1" | "t2" | "t3" }   required
//   { "limit": 200 }                 cells per invocation (default 200)
//   { "force_full": true }           bypass stale filter
//   { "state_filter": "CA" }         restrict to one state

// deno-lint-ignore-file no-explicit-any
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ADMIN_SECRET         = Deno.env.get("ADMIN_SECRET") ?? "";

const OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast";

const HOURLY_FIELDS = [
  "temperature_2m",
  "apparent_temperature",
  "precipitation_probability",
  "precipitation",
  "weathercode",
  "windspeed_10m",
  "uv_index",
  "cloud_cover",
  "is_day",
].join(",");

const BATCH_SIZE = 50;           // cells per Open-Meteo call (down from 100 — smaller payload, finer-grained 5xx scope)
const DEFAULT_LIMIT = 200;       // cells per invocation
const UPSERT_CHUNK = 500;        // rows per UPSERT statement
const FETCH_TIMEOUT_MS = 30_000; // per-attempt timeout
const FETCH_MAX_RETRIES = 3;     // total attempts incl. first try
const FETCH_BACKOFF_MS = [0, 1000, 4000];  // delay before attempt N

interface StaleCell {
  grid_lat: number;
  grid_lon: number;
  stale_threshold_hours: number;
}

interface BatchResponse {
  hourly?: {
    time?: string[];
    temperature_2m?: number[];
    apparent_temperature?: number[];
    windspeed_10m?: number[];
    precipitation_probability?: number[];
    precipitation?: number[];
    weathercode?: number[];
    uv_index?: number[];
    cloud_cover?: number[];
    is_day?: number[];
  };
}

Deno.serve(async (req: Request): Promise<Response> => {
  const headers = corsHeaders(req, "POST, OPTIONS");

  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers });
  }

  // Admin-secret gate (consistent with other writer fns)
  const provided = req.headers.get("x-admin-secret") ?? "";
  if (!ADMIN_SECRET || provided !== ADMIN_SECRET) {
    return new Response(
      JSON.stringify({ error: "unauthorized" }),
      { status: 401, headers: { ...headers, "Content-Type": "application/json" } },
    );
  }

  let body: any = {};
  try {
    body = await req.json();
  } catch (_e) {
    body = {};
  }

  const limit: number       = Math.min(Number(body.limit ?? DEFAULT_LIMIT), 1000);
  const forceFull: boolean  = !!body.force_full;
  const stateFilter: string | null = body.state_filter ?? null;
  const tier: string        = String(body.tier ?? "t1");

  if (!["t1", "t2", "t3"].includes(tier)) {
    return new Response(
      JSON.stringify({ error: "invalid_tier", detail: `tier must be one of t1, t2, t3 (got '${tier}')` }),
      { status: 400, headers: { ...headers, "Content-Type": "application/json" } },
    );
  }

  // Tier config — what each cron owns. forecastDays/pastDays drive the
  // Open-Meteo request; upsertMinFutureH defines the lower bound of forecast
  // hours we WRITE (t2 skips first 48h because t1 owns them; t3 skips first
  // 96h because t1+t2 own them). t3 is the only tier that writes past
  // observations.
  const TIER_CONFIG: Record<string, {
    forecastDays:     number;
    pastDays:         number;
    upsertMinFutureH: number;  // skip forecast hours where (ts - now) < this many hours
    writeObserved:    boolean;
    pickerRpc:        string;
    bumpColumn:       string;
  }> = {
    t1: { forecastDays: 2, pastDays: 0, upsertMinFutureH: 0,  writeObserved: false,
          pickerRpc: "_orch_pick_stale_weather_cells_t1", bumpColumn: "last_fetched_t1" },
    t2: { forecastDays: 4, pastDays: 0, upsertMinFutureH: 48, writeObserved: false,
          pickerRpc: "_orch_pick_stale_weather_cells_t2", bumpColumn: "last_fetched_t2" },
    t3: { forecastDays: 7, pastDays: 3, upsertMinFutureH: 96, writeObserved: true,
          pickerRpc: "_orch_pick_stale_weather_cells_t3", bumpColumn: "last_fetched_t3" },
  };
  const cfg = TIER_CONFIG[tier];

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    auth: { persistSession: false },
  });

  const t0 = Date.now();

  // ─── 1. Pick stale cells for this tier ───────────────────────────────
  const { data: staleCells, error: queryErr } = await supabase
    .rpc(cfg.pickerRpc, {
      p_limit: limit,
      p_state: stateFilter,
      p_force: forceFull,
    });

  if (queryErr) {
    return new Response(
      JSON.stringify({ error: "stale_cell_query_failed", detail: queryErr.message }),
      { status: 500, headers: { ...headers, "Content-Type": "application/json" } },
    );
  }

  const cells: StaleCell[] = (staleCells ?? []).map((r: any) => ({
    grid_lat: Number(r.grid_lat),
    grid_lon: Number(r.grid_lon),
    stale_threshold_hours: 0,  // unused under tiered model
  }));

  if (cells.length === 0) {
    return new Response(
      JSON.stringify({ skipped: true, reason: "no stale cells", elapsed_ms: Date.now() - t0 }),
      { status: 200, headers: { ...headers, "Content-Type": "application/json" } },
    );
  }

  // ─── 2. Batched multi-location Open-Meteo fetch ────────────────────
  const nowUtcMs = Date.now();
  let apiCalls = 0;
  let totalUpserted = 0;
  let failedCells = 0;
  const errors: string[] = [];

  for (let bStart = 0; bStart < cells.length; bStart += BATCH_SIZE) {
    const batch = cells.slice(bStart, bStart + BATCH_SIZE);
    const lats = batch.map(c => c.grid_lat.toFixed(3)).join(",");
    const lons = batch.map(c => c.grid_lon.toFixed(3)).join(",");

    const url = new URL(OPEN_METEO_URL);
    url.searchParams.set("latitude", lats);
    url.searchParams.set("longitude", lons);
    url.searchParams.set("hourly", HOURLY_FIELDS);
    url.searchParams.set("temperature_unit", "fahrenheit");
    url.searchParams.set("windspeed_unit", "mph");
    url.searchParams.set("precipitation_unit", "mm");
    url.searchParams.set("timezone", "UTC");
    url.searchParams.set("past_days", String(cfg.pastDays));
    url.searchParams.set("forecast_days", String(cfg.forecastDays));

    // Retry on 5xx/timeout/network. Open-Meteo intermittently 502s under load;
    // 4xx (client errors) bail immediately. 2026-06-04 outage: 4h of 502s
    // failed every T2 fire before retry was added.
    let data: BatchResponse[] = [];
    let batchOk = false;
    let lastBatchErr = "";
    for (let attempt = 0; attempt < FETCH_MAX_RETRIES; attempt++) {
      if (FETCH_BACKOFF_MS[attempt] > 0) {
        await new Promise(r => setTimeout(r, FETCH_BACKOFF_MS[attempt]));
      }
      try {
        const resp = await fetch(url.toString(), {
          signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
        });
        if (!resp.ok) {
          const txt = await resp.text();
          lastBatchErr = `HTTP ${resp.status}: ${txt.slice(0, 120)}`;
          if (resp.status >= 400 && resp.status < 500) break;  // 4xx — don't retry
          continue;  // 5xx — retry
        }
        const j = await resp.json();
        data = Array.isArray(j) ? j : [j];
        batchOk = true;
        apiCalls += 1;
        break;
      } catch (e) {
        lastBatchErr = (e as Error).message;
        // Retry on timeout/network errors
      }
    }
    if (!batchOk) {
      errors.push(`batch ${bStart}: ${lastBatchErr} (after ${FETCH_MAX_RETRIES} attempts)`);
      failedCells += batch.length;
      continue;
    }

    if (data.length !== batch.length) {
      errors.push(`batch ${bStart}: got ${data.length} responses for ${batch.length} cells`);
    }

    // ─── 3. Parse + collect rows (filtered by tier window) ──────────
    // Per-tier filter:
    //   t1: all future hours up to +48h (forecast_days=2 limits this naturally)
    //   t2: only hours where (ts - now) >= 48h  (t1 owns the first 48h)
    //   t3: only hours where (ts - now) >= 96h FOR FORECASTS, plus all past
    //       observed hours (t3 is the sole owner of past_days=3)
    const minFutureMs = cfg.upsertMinFutureH * 3_600_000;
    const rows: any[] = [];
    const successfulCellIdx: number[] = [];
    for (let ci = 0; ci < batch.length; ci++) {
      const cell = batch[ci];
      const cellData = data[ci] ?? {};
      const hourly = cellData.hourly ?? {};
      const times = hourly.time ?? [];
      if (times.length === 0) {
        failedCells += 1;
        continue;
      }
      successfulCellIdx.push(ci);
      for (let i = 0; i < times.length; i++) {
        // "YYYY-MM-DDTHH:MM" in UTC (timezone=UTC param)
        const forecastTs = new Date(times[i] + "Z");
        const tsMs = forecastTs.getTime();
        const isObserved = tsMs < nowUtcMs;

        // Filter: write only the hours this tier owns.
        if (isObserved) {
          if (!cfg.writeObserved) continue;  // only t3 writes past hours
        } else {
          if (tsMs - nowUtcMs < minFutureMs) continue;  // future hour outside this tier's window
        }

        rows.push({
          grid_lat:      cell.grid_lat,
          grid_lon:      cell.grid_lon,
          forecast_ts:   forecastTs.toISOString(),
          is_observed:   isObserved,
          temp_air:      hourly.temperature_2m?.[i]            ?? null,
          feels_like:    hourly.apparent_temperature?.[i]      ?? null,
          wind_speed:    hourly.windspeed_10m?.[i]             ?? null,
          precip_chance: hourly.precipitation_probability?.[i] ?? null,
          precip_mm:     hourly.precipitation?.[i]             ?? null,
          weather_code:  hourly.weathercode?.[i]               ?? null,
          uv_index:      hourly.uv_index?.[i]                  ?? null,
          cloud_cover:   hourly.cloud_cover?.[i]               ?? null,
          is_day:        hourly.is_day?.[i] === 1 ? true
                       : hourly.is_day?.[i] === 0 ? false : null,
          fetched_at:    new Date().toISOString(),
        });
      }
    }

    // ─── 4. Upsert chunked ───────────────────────────────────────────
    for (let cs = 0; cs < rows.length; cs += UPSERT_CHUNK) {
      const chunk = rows.slice(cs, cs + UPSERT_CHUNK);
      const { error: upErr } = await supabase
        .from("weather_grid_hourly")
        .upsert(chunk, { onConflict: "grid_lat,grid_lon,forecast_ts" });
      if (upErr) {
        errors.push(`upsert chunk ${cs}: ${upErr.message.slice(0, 200)}`);
      } else {
        totalUpserted += chunk.length;
      }
    }

    // ─── 5. Bump the tier's last_fetched timestamp on processed cells ──
    // One UPDATE per batch covering all cells whose Open-Meteo response
    // succeeded (regardless of whether the rows filter kept anything).
    if (successfulCellIdx.length > 0) {
      const bumpPairs = successfulCellIdx.map(ci => batch[ci]);
      const nowIso = new Date().toISOString();
      // Use a server-side update via RPC-free approach: chunked upsert into
      // weather_grid would replace all columns. Instead, do one composite
      // OR'd filter via Supabase's .or() builder. For 100 cells this is OK.
      const orFilter = bumpPairs
        .map(c => `and(grid_lat.eq.${c.grid_lat},grid_lon.eq.${c.grid_lon})`)
        .join(",");
      const { error: bumpErr } = await supabase
        .from("weather_grid")
        .update({ [cfg.bumpColumn]: nowIso })
        .or(orFilter);
      if (bumpErr) errors.push(`bump ${cfg.bumpColumn} batch=${bStart}: ${bumpErr.message.slice(0, 200)}`);
    }
  }

  const elapsedMs = Date.now() - t0;

  return new Response(
    JSON.stringify({
      cells_processed:   cells.length,
      cells_failed:      failedCells,
      rows_upserted:     totalUpserted,
      api_calls:         apiCalls,
      elapsed_ms:        elapsedMs,
      errors_sample:     errors.slice(0, 5),
      knobs: { tier, limit, force_full: forceFull, state_filter: stateFilter },
    }),
    { status: 200, headers: { ...headers, "Content-Type": "application/json" } },
  );
});
