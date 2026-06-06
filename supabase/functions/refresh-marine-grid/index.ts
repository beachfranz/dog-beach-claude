// refresh-marine-grid/index.ts
//
// Marine forecast loader — sibling of refresh-weather-grid.
//
// Single-tier model (vs weather's t1/t2/t3):
//   * Open-Meteo Marine returns up to 7-day forecast in one call
//   * Marine state changes slowly; no need for layered cadence
//   * One cron, one stale threshold (1h via picker)
//
// Per-invocation cell cap (default 400) keeps the function under Supabase
// Edge 150s. Cron orchestrator repeats until result.skipped=true.
//
// Knobs (POST body):
//   { "limit": 400 }               cells per invocation
//   { "force_full": true }         bypass stale filter
//   { "state_filter": "CA" }       restrict to one state
//   { "forecast_days": 7 }         Open-Meteo forecast horizon (max 7)

// deno-lint-ignore-file no-explicit-any
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const ADMIN_SECRET         = Deno.env.get("ADMIN_SECRET") ?? "";

const OPEN_METEO_URL = "https://marine-api.open-meteo.com/v1/marine";

const HOURLY_FIELDS = [
  "wave_height",
  "wave_period",
  "wave_direction",
  "swell_wave_height",
  "swell_wave_period",
  "swell_wave_direction",
  "wind_wave_height",
  "wind_wave_period",
  "wind_wave_direction",
  "sea_surface_temperature",
  "ocean_current_velocity",
  "ocean_current_direction",
].join(",");

const BATCH_SIZE = 25;            // cells per Open-Meteo call. Smaller than
                                  // weather's 50 because marine response
                                  // payload is larger (12 fields × 168 hours).
const DEFAULT_LIMIT = 400;        // cells per invocation
const UPSERT_CHUNK = 500;         // rows per UPSERT statement
const FETCH_TIMEOUT_MS = 30_000;
const FETCH_MAX_RETRIES = 3;
const FETCH_BACKOFF_MS = [0, 1000, 4000];

interface StaleCell {
  grid_lat: number;
  grid_lon: number;
}

interface MarineResponse {
  hourly?: {
    time?: string[];
    wave_height?: (number | null)[];
    wave_period?: (number | null)[];
    wave_direction?: (number | null)[];
    swell_wave_height?: (number | null)[];
    swell_wave_period?: (number | null)[];
    swell_wave_direction?: (number | null)[];
    wind_wave_height?: (number | null)[];
    wind_wave_period?: (number | null)[];
    wind_wave_direction?: (number | null)[];
    sea_surface_temperature?: (number | null)[];
    ocean_current_velocity?: (number | null)[];
    ocean_current_direction?: (number | null)[];
  };
}

Deno.serve(async (req: Request): Promise<Response> => {
  const headers = corsHeaders(req, "POST, OPTIONS");

  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers });
  }

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

  const limit: number              = Math.min(Number(body.limit ?? DEFAULT_LIMIT), 1000);
  const forceFull: boolean         = !!body.force_full;
  const stateFilter: string | null = body.state_filter ?? null;
  const forecastDays: number       = Math.min(Math.max(Number(body.forecast_days ?? 7), 1), 7);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    auth: { persistSession: false },
  });

  const t0 = Date.now();

  // ─── 1. Pick stale cells ────────────────────────────────────────────
  const { data: staleCells, error: queryErr } = await supabase
    .rpc("_orch_pick_stale_marine_cells", {
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
  }));

  if (cells.length === 0) {
    return new Response(
      JSON.stringify({ skipped: true, reason: "no stale cells", elapsed_ms: Date.now() - t0 }),
      { status: 200, headers: { ...headers, "Content-Type": "application/json" } },
    );
  }

  // ─── 2. Batched fetch + parse ───────────────────────────────────────
  let apiCalls = 0;
  let totalUpserted = 0;
  let failedCells = 0;
  let markedInland = 0;
  const errors: string[] = [];
  const inlandCells: StaleCell[] = [];

  for (let bStart = 0; bStart < cells.length; bStart += BATCH_SIZE) {
    const batch = cells.slice(bStart, bStart + BATCH_SIZE);
    // Fetch at cell CENTER (grid corner + 0.05° offset) so the API picks
    // the right marine cell when our 0.1° bin straddles a model boundary.
    const lats = batch.map(c => (c.grid_lat + 0.05).toFixed(3)).join(",");
    const lons = batch.map(c => (c.grid_lon + 0.05).toFixed(3)).join(",");

    const url = new URL(OPEN_METEO_URL);
    url.searchParams.set("latitude", lats);
    url.searchParams.set("longitude", lons);
    url.searchParams.set("hourly", HOURLY_FIELDS);
    url.searchParams.set("timezone", "UTC");
    url.searchParams.set("forecast_days", String(forecastDays));

    let data: MarineResponse[] = [];
    let batchOk = false;
    let lastBatchErr = "";
    let was400 = false;

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
          if (resp.status === 400) {
            // Inland or unsupported region. Mark all cells in this batch
            // as inland — the API doesn't tell us which one failed when
            // batched, so we conservatively mark the whole batch and the
            // picker will probe individually on re-test.
            was400 = true;
          }
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
      }
    }

    if (!batchOk) {
      if (was400) {
        // Cells are inland — record for bulk-mark below
        inlandCells.push(...batch);
        errors.push(`batch ${bStart}: 400 (inland) — marking ${batch.length} cells`);
      } else {
        errors.push(`batch ${bStart}: ${lastBatchErr} (after ${FETCH_MAX_RETRIES} attempts)`);
        failedCells += batch.length;
      }
      continue;
    }

    if (data.length !== batch.length) {
      errors.push(`batch ${bStart}: got ${data.length} responses for ${batch.length} cells`);
    }

    // ─── 3. Parse + collect rows ────────────────────────────────────
    const rows: any[] = [];
    const successfulCellIdx: number[] = [];

    for (let ci = 0; ci < batch.length; ci++) {
      const cell = batch[ci];
      const cellData = data[ci] ?? {};
      const hourly = cellData.hourly ?? {};
      const times = hourly.time ?? [];
      if (times.length === 0) {
        // No data for this cell — likely inland even though batch wasn't 400.
        // Mark individually.
        inlandCells.push(cell);
        continue;
      }
      successfulCellIdx.push(ci);

      for (let i = 0; i < times.length; i++) {
        const forecastTs = new Date(times[i] + "Z");
        const swellH = hourly.swell_wave_height?.[i];
        const swellP = hourly.swell_wave_period?.[i];
        // Computed at load: swell power per meter of wavefront.
        // 0.49 * height² * period (kJ/m). Better safety predictor than
        // wave_height alone — captures long-period punch.
        const swellPower = (swellH != null && swellP != null)
          ? Number((0.49 * swellH * swellH * swellP).toFixed(1))
          : null;

        rows.push({
          grid_lat:                    cell.grid_lat,
          grid_lon:                    cell.grid_lon,
          forecast_ts:                 forecastTs.toISOString(),
          wave_height_m:               hourly.wave_height?.[i]              ?? null,
          wave_period_s:               hourly.wave_period?.[i]              ?? null,
          wave_direction_deg:          hourly.wave_direction?.[i]           ?? null,
          swell_wave_height_m:         swellH                               ?? null,
          swell_wave_period_s:         swellP                               ?? null,
          swell_wave_direction_deg:    hourly.swell_wave_direction?.[i]     ?? null,
          wind_wave_height_m:          hourly.wind_wave_height?.[i]         ?? null,
          wind_wave_period_s:          hourly.wind_wave_period?.[i]         ?? null,
          wind_wave_direction_deg:     hourly.wind_wave_direction?.[i]      ?? null,
          sst_c:                       hourly.sea_surface_temperature?.[i]  ?? null,
          ocean_current_velocity_ms:   hourly.ocean_current_velocity?.[i]   ?? null,
          ocean_current_direction_deg: hourly.ocean_current_direction?.[i]  ?? null,
          swell_power_kj_m:            swellPower,
          fetched_at:                  new Date().toISOString(),
        });
      }
    }

    // ─── 4. Upsert chunked ──────────────────────────────────────────
    for (let cs = 0; cs < rows.length; cs += UPSERT_CHUNK) {
      const chunk = rows.slice(cs, cs + UPSERT_CHUNK);
      const { error: upErr } = await supabase
        .from("marine_grid_hourly")
        .upsert(chunk, { onConflict: "grid_lat,grid_lon,forecast_ts" });
      if (upErr) {
        errors.push(`upsert chunk ${cs}: ${upErr.message.slice(0, 200)}`);
      } else {
        totalUpserted += chunk.length;
      }
    }

    // ─── 5. Bump last_fetched_at on processed cells ─────────────────
    if (successfulCellIdx.length > 0) {
      const bumpPairs = successfulCellIdx.map(ci => batch[ci]);
      const nowIso = new Date().toISOString();
      const orFilter = bumpPairs
        .map(c => `and(grid_lat.eq.${c.grid_lat},grid_lon.eq.${c.grid_lon})`)
        .join(",");
      const { error: bumpErr } = await supabase
        .from("marine_grid")
        .update({ last_fetched_at: nowIso })
        .or(orFilter);
      if (bumpErr) errors.push(`bump last_fetched_at batch=${bStart}: ${bumpErr.message.slice(0, 200)}`);
    }
  }

  // ─── 6. Mark inland cells in bulk (one RPC call total) ────────────
  if (inlandCells.length > 0) {
    const { data: markedCount, error: markErr } = await supabase
      .rpc("_orch_mark_marine_cells_inland", {
        p_cells: inlandCells.map(c => ({
          grid_lat: c.grid_lat,
          grid_lon: c.grid_lon,
        })),
      });
    if (markErr) {
      errors.push(`mark inland: ${markErr.message.slice(0, 200)}`);
    } else {
      markedInland = Number(markedCount ?? 0);
    }
  }

  const elapsedMs = Date.now() - t0;

  return new Response(
    JSON.stringify({
      cells_processed:   cells.length - inlandCells.length,
      cells_failed:      failedCells,
      cells_marked_inland: markedInland,
      rows_upserted:     totalUpserted,
      api_calls:         apiCalls,
      elapsed_ms:        elapsedMs,
      errors_sample:     errors.slice(0, 5),
      knobs: { limit, force_full: forceFull, state_filter: stateFilter, forecast_days: forecastDays },
    }),
    { status: 200, headers: { ...headers, "Content-Type": "application/json" } },
  );
});
