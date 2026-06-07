// ensure-user-weather-cell/index.ts
//
// On-demand single-cell weather-grid warmup for find.html's "Walk from
// here" strip. Mirror of the t1 path in refresh-weather-grid but bounded
// to ONE cell — the visitor's bin-floored coords.
//
// Why this exists: the W2 reference-layer rule
// ([[weather-grid-reference-layer]] + [[grid-consumers-require-cell-warmth-gate]])
// forbids consumers from falling back to direct Open-Meteo on partial
// cells. find.html needs walkability data for the user's lat/lng — if
// her cell isn't in the inventory yet (inland visitor, no beach nearby)
// or hasn't been refreshed by the hourly loader, the strip would be cold.
// This fn does the one-shot registration + fetch so consumer reads can
// proceed against weather_for_point like any other beach.
//
// Public (anon-callable). Surface is bounded: writes ≤48 rows into
// weather_grid_hourly for ONE cell per call. Open-Meteo cost ≈ one HTTP
// request. Idempotent — calling twice in 1s for the same cell second-
// shots no-ops the fetch (cell is warm after first call).
//
// POST { lat: number, lng: number }
// → { grid_lat, grid_lon, is_warm, fetched_rows, skipped: boolean }

// deno-lint-ignore-file no-explicit-any
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast";

// Same field set as refresh-weather-grid for parity. weather_for_point
// SETOF returns the rows verbatim, so consumers see identical shape no
// matter which loader wrote the cell.
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

const FETCH_TIMEOUT_MS = 15_000;
const FETCH_MAX_RETRIES = 3;
const FETCH_BACKOFF_MS = [0, 800, 2400];

Deno.serve(async (req: Request): Promise<Response> => {
  const headers = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers });

  let body: any = {};
  try { body = await req.json(); } catch (_e) { body = {}; }

  const lat = Number(body.lat);
  const lng = Number(body.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)
      || lat < -90 || lat > 90 || lng < -180 || lng > 180) {
    return new Response(
      JSON.stringify({ error: "invalid_coords", detail: "lat must be in [-90,90], lng in [-180,180]" }),
      { status: 400, headers },
    );
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    auth: { persistSession: false },
  });

  // ─── 1. Idempotent register + warmth check ──────────────────────────
  const { data: regRows, error: regErr } = await supabase
    .rpc("register_user_weather_cell", { p_lat: lat, p_lng: lng });
  if (regErr || !regRows || !regRows[0]) {
    return new Response(
      JSON.stringify({ error: "register_failed", detail: regErr?.message ?? "no row returned" }),
      { status: 500, headers },
    );
  }
  const grid_lat = Number(regRows[0].out_grid_lat);
  const grid_lon = Number(regRows[0].out_grid_lon);
  let   is_warm  = !!regRows[0].out_is_warm;

  if (is_warm) {
    // Already warm — no fetch needed. Reader can proceed.
    return new Response(
      JSON.stringify({ grid_lat, grid_lon, is_warm: true, fetched_rows: 0, skipped: true }),
      { status: 200, headers },
    );
  }

  // ─── 2. Cold cell — fetch t1 horizon (next 48h) from Open-Meteo ────
  const url = new URL(OPEN_METEO_URL);
  url.searchParams.set("latitude",  grid_lat.toFixed(3));
  url.searchParams.set("longitude", grid_lon.toFixed(3));
  url.searchParams.set("hourly", HOURLY_FIELDS);
  url.searchParams.set("temperature_unit", "fahrenheit");
  url.searchParams.set("windspeed_unit", "mph");
  url.searchParams.set("precipitation_unit", "mm");
  url.searchParams.set("timezone", "UTC");
  url.searchParams.set("past_days", "0");
  url.searchParams.set("forecast_days", "2");

  let omData: any = null;
  let lastErr = "";
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
        lastErr = `HTTP ${resp.status}: ${txt.slice(0, 100)}`;
        if (resp.status >= 400 && resp.status < 500) break;  // client error — don't retry
        continue;
      }
      omData = await resp.json();
      break;
    } catch (e) {
      lastErr = (e as Error).message;
    }
  }
  if (!omData?.hourly?.time) {
    return new Response(
      JSON.stringify({
        grid_lat, grid_lon, is_warm: false, fetched_rows: 0,
        skipped: false,
        error: "openmeteo_fetch_failed", detail: lastErr,
      }),
      { status: 502, headers },
    );
  }

  // ─── 3. Build hourly rows ───────────────────────────────────────────
  const nowUtcMs = Date.now();
  const h = omData.hourly;
  const times: string[] = h.time ?? [];
  const fetchedAt = new Date().toISOString();
  const rows: any[] = [];
  for (let i = 0; i < times.length; i++) {
    // "YYYY-MM-DDTHH:MM" + Z (timezone=UTC param)
    const forecastTs = new Date(times[i] + "Z");
    const tsMs = forecastTs.getTime();
    const isObserved = tsMs < nowUtcMs;
    // t1 owns next 48h forecast only — skip past observations (t3's job)
    if (isObserved) continue;

    rows.push({
      grid_lat,
      grid_lon,
      forecast_ts:   forecastTs.toISOString(),
      is_observed:   false,
      temp_air:      h.temperature_2m?.[i]            ?? null,
      feels_like:    h.apparent_temperature?.[i]      ?? null,
      wind_speed:    h.windspeed_10m?.[i]             ?? null,
      precip_chance: h.precipitation_probability?.[i] ?? null,
      precip_mm:     h.precipitation?.[i]             ?? null,
      weather_code:  h.weathercode?.[i]               ?? null,
      uv_index:      h.uv_index?.[i]                  ?? null,
      cloud_cover:   h.cloud_cover?.[i]               ?? null,
      is_day:        h.is_day?.[i] === 1 ? true
                   : h.is_day?.[i] === 0 ? false : null,
      fetched_at:    fetchedAt,
      source:        "user_on_demand",
    });
  }

  // ─── 4. Upsert + bump last_fetched_t1 ───────────────────────────────
  if (rows.length > 0) {
    const { error: upErr } = await supabase
      .from("weather_grid_hourly")
      .upsert(rows, { onConflict: "grid_lat,grid_lon,forecast_ts" });
    if (upErr) {
      return new Response(
        JSON.stringify({
          grid_lat, grid_lon, is_warm: false, fetched_rows: 0,
          error: "upsert_failed", detail: upErr.message.slice(0, 200),
        }),
        { status: 500, headers },
      );
    }
    const { error: bumpErr } = await supabase
      .from("weather_grid")
      .update({ last_fetched_t1: fetchedAt })
      .eq("grid_lat", grid_lat)
      .eq("grid_lon", grid_lon);
    if (bumpErr) {
      // Non-fatal — rows are written, just the timestamp didn't bump.
      // Next loader tick will re-fetch. Log via response so caller can see.
      return new Response(
        JSON.stringify({
          grid_lat, grid_lon, is_warm: true, fetched_rows: rows.length,
          warning: "bump_failed", detail: bumpErr.message.slice(0, 200),
        }),
        { status: 200, headers },
      );
    }
  }

  // Re-check warmth post-write so caller can read confidently.
  const { data: warmCheck } = await supabase
    .from("weather_grid_hourly")
    .select("forecast_ts", { count: "exact", head: true })
    .eq("grid_lat", grid_lat)
    .eq("grid_lon", grid_lon)
    .gte("forecast_ts", new Date().toISOString())
    .lt("forecast_ts", new Date(Date.now() + 24 * 3600_000).toISOString());

  is_warm = (warmCheck as any)?.count >= 22 || rows.length >= 22;

  return new Response(
    JSON.stringify({ grid_lat, grid_lon, is_warm, fetched_rows: rows.length, skipped: false }),
    { status: 200, headers },
  );
});
