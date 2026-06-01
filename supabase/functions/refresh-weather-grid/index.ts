// refresh-weather-grid/index.ts
//
// TS port of scripts/dagster/dog_beach/dog_beach/assets/weather_grid.py
// (refresh_weather_grid asset). Fetches Open-Meteo hourly weather for
// stale cells in public.weather_grid and upserts into public.weather_grid_hourly.
//
// Replaces the laptop-bound Dagster execution so the orchestrator
// (orch_jobs) can fire this server-side via net.http_post.
//
// Logic:
//   1. SELECT stale cells from public.weather_grid with tier-based
//      threshold (hourly-tier beach -> hot 1h; daily/DP -> warm 6h;
//      orphan -> cold 24h)
//   2. Process in batches of 100 cells via Open-Meteo multi-location
//      GET requests
//   3. Upsert hourly rows into public.weather_grid_hourly
//
// Per-invocation cell cap (default 200) keeps the function under the
// Supabase Edge 150s hard timeout. Repeat the call until result.skipped=true.
// The orchestrator handles cadence via cron schedule.
//
// Knobs (POST body):
//   { "limit": 200 }            cells per invocation (default 200)
//   { "force_full": true }      bypass stale filter (force refresh all)
//   { "state_filter": "CA" }    restrict to one state
//   { "hot_h": 1, "warm_h": 6, "cold_h": 24 }   stale thresholds
//
// Source-of-truth Python remains in repo for local dev / debugging.

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

const BATCH_SIZE = 100;          // cells per Open-Meteo call
const DEFAULT_LIMIT = 200;       // cells per invocation
const UPSERT_CHUNK = 500;        // rows per UPSERT statement

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
  const hotH:  number = Number(body.hot_h  ?? 1);
  const warmH: number = Number(body.warm_h ?? 6);
  const coldH: number = Number(body.cold_h ?? 24);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    auth: { persistSession: false },
  });

  const t0 = Date.now();

  // ─── 1. Pick stale cells ────────────────────────────────────────────
  const { data: staleCells, error: queryErr } = await supabase
    .rpc("_orch_pick_stale_weather_cells", {
      p_hot_h:    hotH,
      p_warm_h:   warmH,
      p_cold_h:   coldH,
      p_force:    forceFull,
      p_state:    stateFilter,
      p_limit:    limit,
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
    stale_threshold_hours: Number(r.stale_threshold_hours),
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
    url.searchParams.set("past_days", "3");
    url.searchParams.set("forecast_days", "7");

    let data: BatchResponse[] = [];
    try {
      const resp = await fetch(url.toString(), {
        signal: AbortSignal.timeout(60_000),
      });
      if (!resp.ok) {
        const txt = await resp.text();
        errors.push(`batch ${bStart}: HTTP ${resp.status}: ${txt.slice(0, 200)}`);
        failedCells += batch.length;
        continue;
      }
      apiCalls += 1;
      const j = await resp.json();
      data = Array.isArray(j) ? j : [j];
    } catch (e) {
      errors.push(`batch ${bStart}: ${(e as Error).message}`);
      failedCells += batch.length;
      continue;
    }

    if (data.length !== batch.length) {
      errors.push(`batch ${bStart}: got ${data.length} responses for ${batch.length} cells`);
    }

    // ─── 3. Parse + collect rows ─────────────────────────────────────
    const rows: any[] = [];
    for (let ci = 0; ci < batch.length; ci++) {
      const cell = batch[ci];
      const cellData = data[ci] ?? {};
      const hourly = cellData.hourly ?? {};
      const times = hourly.time ?? [];
      if (times.length === 0) {
        failedCells += 1;
        continue;
      }
      for (let i = 0; i < times.length; i++) {
        // "YYYY-MM-DDTHH:MM" in UTC (timezone=UTC param)
        const forecastTs = new Date(times[i] + "Z");
        const isObserved = forecastTs.getTime() < nowUtcMs;
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
          // Explicit so ON CONFLICT DO UPDATE refreshes the staleness clock
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
      knobs: { limit, force_full: forceFull, state_filter: stateFilter,
               hot_h: hotH, warm_h: warmH, cold_h: coldH },
    }),
    { status: 200, headers: { ...headers, "Content-Type": "application/json" } },
  );
});
