// get-dog-park-now/index.ts
// Live NOW refresh for one or many dog parks. Open-Meteo only.
// Writes is_now=true row to dog_park_day_hourly_scores, overwriting
// any existing forecast for that hour. Cleared/refreshed each call.
//
// GET  ?fid=<n>                      → refresh single dog park, return its NOW row
// POST { fids?: number[] }           → refresh all (or listed) parks (used by cron)

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import {
  scoreDogParkHours,
  type RawHourData,
  type ScoredHour,
  type DogParkScoringConfig,
} from "../_shared/scoring_dog_park.ts";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const STATE_TZ: Record<string, string> = {
  CA: "America/Los_Angeles", OR: "America/Los_Angeles", WA: "America/Los_Angeles",
  NV: "America/Los_Angeles", AZ: "America/Phoenix",
  NM: "America/Denver", UT: "America/Denver", CO: "America/Denver",
  TX: "America/Chicago",
  FL: "America/New_York", NY: "America/New_York", NH: "America/New_York",
  MA: "America/New_York", MD: "America/New_York", DE: "America/New_York",
};
const tzForState = (s: string) => STATE_TZ[s] ?? "America/Los_Angeles";

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "GET, POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  const runAt = new Date();

  let targetFids: number[] | null = null;
  let skipRecentHours: number | null = null;
  // chunked-drain cap. 2026-06-03: Promise.all over 2,589 scoreable parks
  // hits WORKER_RESOURCE_LIMIT (HTTP 546). Cron now fires every 5 min with
  // limit=100, mirroring daily-dog-park-refresh's chunked pattern. Hard-
  // capped at 500 to keep one call safe.
  let limitParks: number | null = null;
  // National scope (Franz 2026-06-05): default is NO state filter — see
  // daily-dog-park-refresh for full rationale. Body.state overrides:
  //   "ALL"/null → no filter (default) ; "MD" / etc → just that state.
  let stateFilters: string[] = [];

  if (req.method === "GET") {
    const fid = new URL(req.url).searchParams.get("fid");
    if (fid) targetFids = [parseInt(fid, 10)].filter(Number.isFinite);
  } else if (req.method === "POST") {
    const body = await req.json().catch(() => ({}));
    if (Array.isArray(body?.fids) && body.fids.length > 0) {
      targetFids = body.fids
        .map((x: unknown) => typeof x === "number" ? x : parseInt(String(x), 10))
        .filter(Number.isFinite);
    }
    if (typeof body?.state === "string") {
      const s = body.state.toUpperCase();
      stateFilters = s === "ALL" ? [] : [s];
    }
    if (typeof body?.skip_recent_hours === "number" && body.skip_recent_hours > 0) {
      skipRecentHours = Math.min(body.skip_recent_hours, 24);
    }
    if (typeof body?.limit === "number" && body.limit > 0) {
      limitParks = Math.min(body.limit, 500);
    }
  }

  // 2026-06-05 LATE: switched to all-in-one RPC to bypass PostgREST
  // db-max-rows=1000 cap. With today's MVP+ scope lift (national
  // pool ~2,589 parks), the prior `from("dog_parks_gold").eq(...)` was
  // silently truncating, leaving ~1,589 parks invisible to NOW refresh.
  // RPC selects, filters by skip_recent_hours (on hourly_scores.updated_at
  // WHERE is_now=true), and orders oldest-NOW-first DB-side. Result is
  // ≤ p_limit so the cap never engages. See 20260605h.
  const { data: dueRows, error: dueErr } = await supabase.rpc(
    "dog_parks_due_for_now_refresh",
    {
      p_target_fids:       targetFids?.length ? targetFids : null,
      p_state_filter:      stateFilters.length > 0 ? stateFilters : null,
      p_skip_recent_hours: skipRecentHours,
      p_limit:             limitParks,
    },
  );
  if (dueErr) return json({ error: `dog_parks_due_for_now_refresh: ${dueErr.message}` }, 500);
  const dueFids = (dueRows ?? []).map((r: { fid: number }) => r.fid);
  if (dueFids.length === 0) {
    return json({
      ok: true,
      message: skipRecentHours !== null
        ? `all parks refreshed within ${skipRecentHours}h`
        : "no parks found",
      count: 0,
    });
  }

  // Load detail for just those fids (≤ limit rows; well under cap).
  const [parksRes, configRes] = await Promise.all([
    supabase
      .from("dog_parks_gold")
      .select(`
        fid, name, display_name_override, lat, lon, state, surface,
        dog_park_dog_policy(hours_open_time, hours_close_time)
      `)
      .in("fid", dueFids),
    supabase.from("dog_park_scoring_config")
      .select("*")
      .eq("is_active", true)
      .limit(1)
      .single(),
  ]);

  if (parksRes.error) return json({ error: `parks load: ${parksRes.error.message}` }, 500);
  if (!parksRes.data?.length) return json({ error: "no parks found after detail load" }, 500);
  if (configRes.error || !configRes.data) return json({ error: "no active config" }, 500);

  type Row = {
    fid: number; name: string; display_name_override: string | null;
    lat: number; lon: number; state: string; surface: string | null;
    dog_park_dog_policy: { hours_open_time: string | null; hours_close_time: string | null }
      | { hours_open_time: string | null; hours_close_time: string | null }[] | null;
  };
  const parks = (parksRes.data as Row[]).map((g) => {
    const dp = Array.isArray(g.dog_park_dog_policy) ? g.dog_park_dog_policy[0] : g.dog_park_dog_policy;
    return {
      fid: g.fid,
      display_name: g.display_name_override ?? g.name ?? `Dog Park ${g.fid}`,
      latitude: g.lat,
      longitude: g.lon,
      state: g.state,
      surface: g.surface,
      hours_open_time: dp?.hours_open_time ?? null,
      hours_close_time: dp?.hours_close_time ?? null,
    };
  });
  const skippedRecent = 0;  // applied DB-side by the RPC

  const config = configRes.data as DogParkScoringConfig;

  const results = await Promise.all(parks.map((p) => refreshNow(p, config, supabase, runAt)));

  if (req.method === "GET" && results.length === 1) {
    const r = results[0];
    if (!r.ok) return json({ error: r.error }, 500);
    return json(r.row);
  }

  return json({
    ok: true,
    runAt: runAt.toISOString(),
    total: results.length,
    ok_count: results.filter((r) => r.ok).length,
    skipped_recent: skippedRecent,
    results: results.map((r) => ({ fid: r.fid, ok: r.ok, error: r.error })),
  });
});

// ─── Per-park NOW refresh ────────────────────────────────────────────────────

interface Park {
  fid: number;
  display_name: string;
  latitude: number;
  longitude: number;
  state: string;
  surface: string | null;
  hours_open_time: string | null;
  hours_close_time: string | null;
}

async function refreshNow(
  park: Park,
  config: DogParkScoringConfig,
  supabase: ReturnType<typeof createClient>,
  runAt: Date,
): Promise<{ fid: number; ok: boolean; row?: unknown; error?: string }> {
  try {
    const tz = tzForState(park.state);

    // Current local date+hour for this park
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: tz,
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", hour12: false,
    }).formatToParts(runAt);
    const get = (t: string) => parts.find((p) => p.type === t)?.value ?? "";
    const localDate = `${get("year")}-${get("month")}-${get("day")}`;
    const localHour = parseInt(get("hour"), 10) % 24;

    // Fetch current weather
    // W2.4 cutover: weather reads from weather_grid_hourly via RPC. Falls
    // back to direct Open-Meteo if grid cell is unloaded.
    let weather: CurrentWeather;
    try {
      const gridWeather = await fetchCurrentWeatherFromGrid(park.latitude, park.longitude, supabase);
      weather = gridWeather ?? await fetchCurrentWeather(park.latitude, park.longitude, tz);
    } catch (_) {
      weather = await fetchCurrentWeather(park.latitude, park.longitude, tz);
    }

    // Determine if park is open this hour
    const openMin = timeToMinutes(park.hours_open_time);
    const closeMin = timeToMinutes(park.hours_close_time);
    const isParkOpen = openMin !== null && closeMin !== null
      ? (localHour * 60) >= openMin && (localHour * 60) < closeMin
      : weather.is_day; // dawn-dusk fallback

    const rawHour: RawHourData = {
      forecastTs: localToUtcIso(localDate, localHour, tz),
      localDate,
      localHour,
      hourLabel: formatHour(localHour),
      isDaylight: weather.is_day,
      weatherCode: weather.weather_code,
      tempAir: weather.temperature_2m,
      feelsLike: weather.apparent_temperature,
      windSpeed: weather.wind_speed_10m,
      precipChance: weather.precip_chance,
      uvIndex: weather.uv_index,
      cloudCover: weather.cloud_cover,
      isParkOpen,
    };

    const [scored] = scoreDogParkHours([rawHour], park.surface as any, config);
    const row = buildNowRow(scored, park, config, runAt, tz);

    // Clear prior is_now row(s) for this park
    await supabase
      .from("dog_park_day_hourly_scores")
      .update({ is_now: false })
      .eq("dog_park_fid", park.fid)
      .eq("is_now", true);

    const { error: upsertErr } = await supabase
      .from("dog_park_day_hourly_scores")
      .upsert(row, { onConflict: "dog_park_fid,forecast_ts" });
    if (upsertErr) throw new Error(upsertErr.message);

    // Re-apply v2 best window + day-level + per-hour v2 score so the
    // recommendation row stays pegged to the latest hourly data. Soft-fail.
    // Mirrors get-beach-now's wiring. Per Franz 2026-05-30 task #9.
    const { error: bwErr } = await supabase.rpc(
      "apply_v2_best_window_to_recommendations",
      { p_fid: park.fid, p_date: row.local_date },
    );
    if (bwErr) console.warn(`[dog_park:${park.fid}] apply_v2 window soft-fail:`, bwErr.message);

    // Read back v2 fields for response so dog-park.html NOW path has them
    let hourScoreV2: number | null = null;
    let hourStatusV2: string | null = null;
    const { data: v2row } = await supabase
      .from("dog_park_day_hourly_scores")
      .select("hour_score_v2")
      .eq("dog_park_fid", park.fid)
      .eq("forecast_ts", row.forecast_ts)
      .maybeSingle();
    hourScoreV2 = (v2row?.hour_score_v2 as number | null) ?? null;

    const { data: stRow } = await supabase.rpc("v2_compute_hour_status_dog_park", {
      p_uv:         row.uv_index ?? null,
      p_asphalt:    row.asphalt_temp ?? null,
      p_wind:       row.wind_speed ?? null,
      p_precip:     row.precip_chance ?? null,
      p_feels_like: row.feels_like ?? null,
      p_is_closed:  false,
    });
    hourStatusV2 = (stRow as unknown as string | null) ?? null;

    return {
      fid: park.fid,
      ok: true,
      row: { ...row, hour_score_v2: hourScoreV2, hour_status_v2: hourStatusV2 },
    };
  } catch (err) {
    console.error(`[dog_park:${park.fid}] NOW refresh error:`, String(err));
    return { fid: park.fid, ok: false, error: String(err) };
  }
}

// ─── Row builder ─────────────────────────────────────────────────────────────

function buildNowRow(
  h: ScoredHour,
  park: { fid: number },
  config: DogParkScoringConfig,
  runAt: Date,
  tz: string,
) {
  return {
    dog_park_fid: park.fid,
    local_date: h.localDate,
    forecast_ts: h.forecastTs,
    local_hour: h.localHour,
    hour_label: h.hourLabel,
    is_daylight: h.isDaylight,
    is_candidate_window: h.isCandidateWindow,
    is_in_best_window: false,
    is_now: true,
    weather_code: h.weatherCode,
    temp_air: h.tempAir,
    feels_like: h.feelsLike,
    wind_speed: h.windSpeed,
    precip_chance: h.precipChance,
    uv_index: h.uvIndex,
    asphalt_temp: h.asphaltTemp,
    // hour_status + per-metric *_status columns retired per Franz
    // 2026-05-30 v1-retirement task #11. Columns dropped by task #12.
    hour_score: h.hourScore,
    wind_score: h.componentScores.wind_score ?? null,
    rain_score: h.componentScores.rain_score ?? null,
    temp_score: h.componentScores.temp_score ?? null,
    uv_score: h.componentScores.uv_score ?? null,
    weather_score: h.componentScores.weather_score ?? null,
    surface_score: h.componentScores.surface_score ?? null,
    positive_reason_codes: h.positiveReasonCodes,
    risk_reason_codes: h.riskReasonCodes,
    explainability: h.componentScores,
    hour_text: h.hourText,
    timezone: tz,
    scoring_version: config.scoring_version,
    generated_at: runAt.toISOString(),
    updated_at: runAt.toISOString(),
    weather_refreshed_at: runAt.toISOString(),
  };
}

// ─── Weather fetch ───────────────────────────────────────────────────────────

interface CurrentWeather {
  temperature_2m: number;
  apparent_temperature: number;
  wind_speed_10m: number;
  weather_code: number;
  uv_index: number;
  cloud_cover: number;
  precip_chance: number;
  is_day: boolean;
}

// W2.4 — read current-hour weather from weather_grid_hourly via RPC.
async function fetchCurrentWeatherFromGrid(
  lat: number,
  lng: number,
  supabase: ReturnType<typeof createClient>,
): Promise<CurrentWeather | null> {
  const now = new Date();
  const startTs = new Date(now.getTime() - 60 * 60 * 1000);
  const endTs = new Date(now.getTime() + 60 * 60 * 1000);
  const { data, error } = await supabase.rpc("weather_for_point", {
    p_lat: lat,
    p_lng: lng,
    p_start_ts: startTs.toISOString(),
    p_end_ts: endTs.toISOString(),
  });
  if (error || !Array.isArray(data) || data.length === 0) return null;
  const nowMs = now.getTime();
  const closest = (data as Array<Record<string, unknown>>).reduce((best, row) => {
    const tsMs = new Date(row.forecast_ts as string).getTime();
    const bestMs = best ? new Date(best.forecast_ts as string).getTime() : Infinity;
    return Math.abs(tsMs - nowMs) < Math.abs(bestMs - nowMs) ? row : best;
  }, null as Record<string, unknown> | null);
  if (!closest) return null;
  return {
    temperature_2m:       closest.temp_air      as number,
    apparent_temperature: (closest.feels_like as number) ?? (closest.temp_air as number),
    wind_speed_10m:       closest.wind_speed    as number,
    weather_code:         closest.weather_code  as number,
    uv_index:             (closest.uv_index    as number) ?? 0,
    cloud_cover:          (closest.cloud_cover as number) ?? 0,
    precip_chance:        (closest.precip_chance as number) ?? 0,
    is_day:               closest.is_day === true,
  };
}

async function fetchCurrentWeather(lat: number, lng: number, tz: string): Promise<CurrentWeather> {
  const params = new URLSearchParams({
    latitude: String(lat),
    longitude: String(lng),
    current: "temperature_2m,apparent_temperature,wind_speed_10m,weather_code,uv_index,cloud_cover,is_day",
    hourly: "precipitation_probability",
    forecast_days: "1",
    temperature_unit: "fahrenheit",
    windspeed_unit: "mph",
    timezone: tz,
  });

  const res = await fetch(`https://api.open-meteo.com/v1/forecast?${params}`);
  if (!res.ok) throw new Error(`Open-Meteo error ${res.status}`);
  const data = await res.json();
  const cur = data.current;
  const nowHour = new Date().getHours();
  const precip = data.hourly?.precipitation_probability?.[nowHour] ?? 0;

  return {
    temperature_2m: cur.temperature_2m,
    apparent_temperature: cur.apparent_temperature ?? cur.temperature_2m,
    wind_speed_10m: cur.wind_speed_10m,
    weather_code: cur.weather_code,
    uv_index: cur.uv_index ?? 0,
    cloud_cover: cur.cloud_cover ?? 0,
    precip_chance: precip,
    is_day: cur.is_day === 1,
  };
}

// ─── Utilities ───────────────────────────────────────────────────────────────

function timeToMinutes(t: string | null): number | null {
  if (!t) return null;
  const m = t.match(/^(\d{1,2}):(\d{2})/);
  return m ? parseInt(m[1], 10) * 60 + parseInt(m[2], 10) : null;
}

function formatHour(h: number): string {
  if (h === 0 || h === 24) return "12am";
  if (h === 12) return "12pm";
  return h < 12 ? `${h}am` : `${h - 12}pm`;
}

function localToUtcIso(localDate: string, localHour: number, tz: string): string {
  const [Y, Mo, D] = localDate.split("-").map(Number);
  const utcGuess = Date.UTC(Y, Mo - 1, D, localHour, 0);
  const dtf = new Intl.DateTimeFormat("en-US", {
    timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", hour12: false,
  });
  const parts = Object.fromEntries(dtf.formatToParts(new Date(utcGuess)).map((p) => [p.type, p.value]));
  const rendered = Date.UTC(
    parseInt(parts.year, 10),
    parseInt(parts.month, 10) - 1,
    parseInt(parts.day, 10),
    parseInt(parts.hour === "24" ? "0" : parts.hour, 10),
    parseInt(parts.minute, 10),
  );
  const offsetMs = rendered - utcGuess;
  return new Date(utcGuess - offsetMs).toISOString();
}
