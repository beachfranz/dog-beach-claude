// daily-dog-park-refresh/index.ts
// Sibling to daily-beach-refresh. Open-Meteo only (no NOAA, no BestTime).
// Reads scoreable dog parks + dog_park_dog_policy hours, fetches weather,
// scores via scoring_dog_park.ts, upserts hourly + daily rows.
//
// Admin-gated. CORS-safe.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import { fetchWeather, wmoToSummaryWeather } from "./openmeteo.ts";
import {
  scoreDogParkHours,
  selectBestWindowsDogPark,
  applyBestWindowFlagsDogPark,
  type RawHourData,
  type ScoredHour,
  type BestWindow,
  type DogParkScoringConfig,
} from "../_shared/scoring_dog_park.ts";

// ─── Types ───────────────────────────────────────────────────────────────────

type DayStatus = "go" | "advisory" | "caution" | "no_go";

interface DogPark {
  fid: number;
  name: string;
  display_name: string;
  latitude: number;
  longitude: number;
  state: string;
  surface: string | null;
  has_fence: boolean | null;
  has_drinking_water: boolean | null;
  hours_open_time: string | null;
  hours_close_time: string | null;
  off_leash_flag: boolean | null;
}

interface RefreshResult {
  fid: number;
  ok: boolean;
  daysProcessed?: number;
  error?: string;
  phases?: Record<string, "ok" | "error" | "skipped">;
}

// ─── State → timezone (CA-only v1; extend per [[dog-park-subpipeline-scope]]) ─

const STATE_TZ: Record<string, string> = {
  CA: "America/Los_Angeles",
  OR: "America/Los_Angeles",
  WA: "America/Los_Angeles",
  NV: "America/Los_Angeles",
  AZ: "America/Phoenix",
  NM: "America/Denver",
  UT: "America/Denver",
  CO: "America/Denver",
  TX: "America/Chicago",
  FL: "America/New_York",
  NY: "America/New_York",
  NH: "America/New_York",
  MA: "America/New_York",
  MD: "America/New_York",
  DE: "America/New_York",
};

function tzForState(state: string): string {
  return STATE_TZ[state] ?? "America/Los_Angeles";
}

// ─── Env ─────────────────────────────────────────────────────────────────────

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// ─── Entry point ─────────────────────────────────────────────────────────────

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST") return new Response("Method not allowed", { status: 405, headers: cors });

  const { requireAdmin } = await import("../_shared/admin-auth.ts");
  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

  let targetFids: number[] | null = null;
  // MVP+ scope (Franz 2026-05-30): CA + OR + WA + MD + UT. Override via body.state:
  //   "ALL" → all states ; "MD" / etc → that single state.
  // The old default was "CA" which silently starved every other state's
  // daily refresh after they launched.
  let stateFilters: string[] = ["CA", "OR", "WA", "MD", "UT"];
  let skipRecentHours: number | null = null;
  try {
    const body = await req.json().catch(() => ({}));
    if (Array.isArray(body?.fids) && body.fids.length > 0) targetFids = body.fids;
    if (typeof body?.state === "string") {
      const s = body.state.toUpperCase();
      stateFilters = s === "ALL" ? [] : [s];
    }
    if (typeof body?.skip_recent_hours === "number" && body.skip_recent_hours > 0) {
      skipRecentHours = Math.min(body.skip_recent_hours, 168);
    }
  } catch { /* no body — defaults */ }

  console.log("daily-dog-park-refresh — fids:", targetFids?.length ?? "all",
    "states:", stateFilters.length === 0 ? "ALL" : stateFilters.join(","),
    "skip_recent_hours:", skipRecentHours);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  const runAt = new Date();

  // 1. Load active scoring config
  const { data: configRow, error: cfgErr } = await supabase
    .from("dog_park_scoring_config")
    .select("*")
    .eq("is_active", true)
    .single();
  if (cfgErr || !configRow) {
    return new Response(JSON.stringify({ error: "no active dog_park_scoring_config" }),
      { status: 500, headers: cors });
  }
  const config = configRow as DogParkScoringConfig;
  console.log("Loaded config:", config.scoring_version);

  // 2. Load scoreable dog parks
  let q = supabase
    .from("dog_parks_gold")
    .select(`
      fid, name, display_name_override, lat, lon, state, surface,
      has_fence, has_drinking_water,
      dog_park_dog_policy!inner(hours_open_time, hours_close_time, off_leash_flag)
    `)
    .eq("is_active", true)
    .eq("is_scoreable", true);
  if (stateFilters.length > 0) q = q.in("state", stateFilters);
  if (targetFids) q = q.in("fid", targetFids);

  const { data: parksRaw, error: parksErr } = await q;
  if (parksErr) {
    return new Response(JSON.stringify({ error: `parks load: ${parksErr.message}` }),
      { status: 500, headers: cors });
  }
  if (!parksRaw || parksRaw.length === 0) {
    return new Response(JSON.stringify({ ok: true, message: "no parks to refresh", count: 0 }),
      { headers: cors });
  }

  const parks: DogPark[] = parksRaw.map((p: any) => {
    const dp = Array.isArray(p.dog_park_dog_policy) ? p.dog_park_dog_policy[0] : p.dog_park_dog_policy;
    return {
      fid: p.fid,
      name: p.name,
      display_name: p.display_name_override ?? p.name ?? `Dog Park ${p.fid}`,
      latitude: p.lat,
      longitude: p.lon,
      state: p.state,
      surface: p.surface,
      has_fence: p.has_fence,
      has_drinking_water: p.has_drinking_water,
      hours_open_time: dp?.hours_open_time ?? null,
      hours_close_time: dp?.hours_close_time ?? null,
      off_leash_flag: dp?.off_leash_flag ?? null,
    };
  });
  console.log(`Loaded ${parks.length} dog parks`);

  // 3. Optional: skip parks refreshed recently
  let toProcess = parks;
  if (skipRecentHours !== null) {
    const since = new Date(runAt.getTime() - skipRecentHours * 3_600_000).toISOString();
    const { data: recent } = await supabase
      .from("dog_park_day_hourly_scores")
      .select("dog_park_fid")
      .gte("updated_at", since);
    const recentFids = new Set((recent ?? []).map((r: any) => r.dog_park_fid));
    toProcess = parks.filter((p) => !recentFids.has(p.fid));
    console.log(`Skipping ${parks.length - toProcess.length} parks refreshed within ${skipRecentHours}h`);
  }

  // 4. Process serially (351 parks × ~1s/park ≈ 6 min wall clock)
  const results: RefreshResult[] = [];
  for (const park of toProcess) {
    try {
      results.push(await processPark(park, config, supabase, runAt));
    } catch (err) {
      console.error(`[${park.fid}] crash:`, err);
      results.push({ fid: park.fid, ok: false, error: String(err) });
    }
  }

  const okCount = results.filter((r) => r.ok).length;
  const errCount = results.length - okCount;
  console.log(`Refresh done: ${okCount} ok / ${errCount} err`);

  return new Response(JSON.stringify({
    ok: true,
    total: results.length,
    ok_count: okCount,
    err_count: errCount,
    results,
  }), { headers: cors });
});

// ─── Per-park refresh ────────────────────────────────────────────────────────

async function processPark(
  park: DogPark,
  config: DogParkScoringConfig,
  supabase: ReturnType<typeof createClient>,
  runAt: Date,
): Promise<RefreshResult> {
  const phases: Record<string, "ok" | "error" | "skipped"> = {};
  const tz = tzForState(park.state);

  // a. Weather (W2.2 cutover — reads from weather_grid_hourly via RPC
  // instead of per-park Open-Meteo fetch. Drops per-park cost and removes
  // network calls from the per-entity loop. See [[weather-grid-reference-layer]].
  // Falls back to direct Open-Meteo if the grid cell isn't loaded yet —
  // covers cells added mid-refresh.)
  let weather: Awaited<ReturnType<typeof fetchWeather>>;
  let weatherSource: "grid" | "direct" = "grid";
  try {
    weather = await fetchWeatherFromGrid(park, tz, supabase);
    if (weather.hours.length === 0) {
      console.warn(`[dog_park:${park.fid}] Grid empty for cell; falling back to direct Open-Meteo fetch`);
      weather = await fetchWeather({
        id: String(park.fid),
        latitude: park.latitude,
        longitude: park.longitude,
        timezone: tz,
      });
      weatherSource = "direct";
    }
    phases.openmeteo = "ok";
  } catch (err) {
    await logError(supabase, park.fid, "openmeteo", err);
    phases.openmeteo = "error";
    return { fid: park.fid, ok: false, error: String(err), phases };
  }

  // b. Hours — fall back to dawn-dusk via Open-Meteo sunrise/sunset if cascade
  // hours are null. Per [[dogpark-rules-are-operator-posted]] dog parks are
  // open dawn-dusk by default. (Grid lookup returns empty days[] so the
  // is_day=1 fallback in buildRawHours kicks in — equivalent to dawn-dusk.)
  const sunriseByDate = new Map<string, string>();
  const sunsetByDate = new Map<string, string>();
  for (const d of weather.days) {
    sunriseByDate.set(d.date, d.sunrise);
    sunsetByDate.set(d.date, d.sunset);
  }

  // c. Build raw hours
  const rawHours = buildRawHours(park, weather, sunriseByDate, sunsetByDate, tz);

  // c.5 Mud-caution check — dirt-surface parks get a caution flag when
  // 72h precip ≥ 5mm. Per [[weather-grid-reference-layer]]. Uses the
  // precipitation_history view (same source as bacteria-risk for beaches).
  let mudCaution = false;
  if (park.surface === "dirt" || park.surface === "decomposed_granite" || park.surface === "wood_chips") {
    try {
      const { data: precipData, error: precipErr } = await supabase.rpc("precip_72h_for_point", {
        p_lat:         park.latitude,
        p_lng:         park.longitude,
        p_anchor_date: new Date().toISOString().slice(0, 10),
      });
      if (!precipErr && typeof precipData === "number" && precipData >= 5) {
        mudCaution = true;
        console.log(`[dog_park:${park.fid}] Mud caution: ${precipData}mm in 72h on ${park.surface}`);
      }
    } catch (_) {
      // best-effort; mud caution isn't critical to scoring
    }
  }

  // d. Score
  let scored: ScoredHour[];
  try {
    scored = scoreDogParkHours(rawHours, park.surface as any, config);
    phases.scoring = "ok";
  } catch (err) {
    await logError(supabase, park.fid, "scoring", err);
    phases.scoring = "error";
    return { fid: park.fid, ok: false, error: String(err), phases };
  }

  // e. Best windows
  const windows = selectBestWindowsDogPark(scored, config);
  applyBestWindowFlagsDogPark(scored, windows);

  // f. Upsert hourly
  const dates = [...new Set(scored.map((h) => h.localDate))].sort();
  try {
    const hourlyRows = scored.map((h) => buildHourlyRow(h, park, config, runAt, tz));
    if (hourlyRows.length > 0) {
      // Clear forecast window for this park (replace-on-rerun semantics)
      const minTs = hourlyRows[0].forecast_ts;
      const maxTs = hourlyRows[hourlyRows.length - 1].forecast_ts;
      const { error: delErr } = await supabase
        .from("dog_park_day_hourly_scores")
        .delete()
        .eq("dog_park_fid", park.fid)
        .gte("forecast_ts", minTs)
        .lte("forecast_ts", maxTs);
      if (delErr) throw new Error(`hourly clear failed: ${delErr.message}`);
    }
    for (let i = 0; i < hourlyRows.length; i += 100) {
      const { error } = await supabase
        .from("dog_park_day_hourly_scores")
        .upsert(hourlyRows.slice(i, i + 100), { onConflict: "dog_park_fid,forecast_ts" });
      if (error) throw new Error(error.message);
    }
    phases.upsert_hourly = "ok";
  } catch (err) {
    await logError(supabase, park.fid, "upsert_hourly", err);
    phases.upsert_hourly = "error";
    return { fid: park.fid, ok: false, error: String(err), phases };
  }

  // g. Upsert daily
  try {
    const dailyRows = dates.map((date) => {
      const dayHours = scored.filter((h) => h.localDate === date);
      const window = windows.get(date) ?? null;
      return buildDailyRow(park, date, dayHours, window, config, runAt, tz, mudCaution);
    });
    if (dailyRows.length > 0) {
      const { error: delErr } = await supabase
        .from("dog_park_day_recommendations")
        .delete()
        .eq("dog_park_fid", park.fid)
        .in("local_date", dates);
      if (delErr) throw new Error(`daily clear failed: ${delErr.message}`);
    }
    const { error } = await supabase
      .from("dog_park_day_recommendations")
      .upsert(dailyRows, { onConflict: "dog_park_fid,local_date" });
    if (error) throw new Error(error.message);
    phases.upsert_daily = "ok";
  } catch (err) {
    await logError(supabase, park.fid, "upsert_daily", err);
    phases.upsert_daily = "error";
    return { fid: park.fid, ok: false, error: String(err), phases };
  }

  // Phase v2-best-window: overwrite the v1 best_window_label/start/end on
  // each date with the v2-scored equivalent. Decoupled from the v1 status
  // pipeline so a bug here can't block the daily refresh — fail soft.
  try {
    for (const d of dates) {
      const { error: rpcErr } = await supabase.rpc(
        "apply_v2_best_window_to_recommendations",
        { p_fid: park.fid, p_date: d },
      );
      if (rpcErr) console.warn(`v2 best window apply failed fid=${park.fid} date=${d}: ${rpcErr.message}`);
    }
    phases.apply_v2_window = "ok";
  } catch (err) {
    console.warn(`v2 best window apply error fid=${park.fid}: ${err}`);
    phases.apply_v2_window = "soft_error";
  }

  return { fid: park.fid, ok: true, daysProcessed: dates.length, phases };
}

// ─── Weather grid lookup (W2.2 — replaces per-park Open-Meteo fetch) ─────────
//
// Reads from weather_grid_hourly via weather_for_point() RPC and shapes the
// result to match openmeteo.ts's OpenMeteoResult so buildRawHours stays
// unchanged. Returns {hours: [], days: []} if cell has no rows (caller falls
// back to direct fetch). Per [[weather-grid-reference-layer]].

async function fetchWeatherFromGrid(
  park: { fid: number; latitude: number; longitude: number },
  tz: string,
  supabase: ReturnType<typeof createClient>,
): Promise<Awaited<ReturnType<typeof fetchWeather>>> {
  const now = new Date();
  const startTs = new Date(now.getTime() - 3 * 86_400_000);
  const endTs   = new Date(now.getTime() + 7 * 86_400_000);

  const { data, error } = await supabase.rpc("weather_for_point", {
    p_lat:      park.latitude,
    p_lng:      park.longitude,
    p_start_ts: startTs.toISOString(),
    p_end_ts:   endTs.toISOString(),
  });

  if (error) {
    throw new Error(`weather_for_point RPC: ${error.message}`);
  }
  if (!Array.isArray(data) || data.length === 0) {
    return { hours: [], days: [] };
  }

  // Format forecast_ts as local-time "YYYY-MM-DDTHH:MM" matching the
  // direct Open-Meteo response shape (consumed by buildRawHours).
  const fmt = new Intl.DateTimeFormat("sv-SE", {
    timeZone: tz,
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", hour12: false,
  });

  const hours = (data as Array<Record<string, unknown>>).map((row) => {
    const ts = new Date(row.forecast_ts as string);
    const parts = Object.fromEntries(fmt.formatToParts(ts).map(p => [p.type, p.value]));
    const localTime = `${parts.year}-${parts.month}-${parts.day}T${parts.hour === "24" ? "00" : parts.hour}:${parts.minute}`;
    return {
      time:                       localTime,
      temperature_2m:             row.temp_air      as number,
      apparent_temperature:       row.feels_like    as number,
      precipitation_probability:  row.precip_chance as number,
      precipitation:              (row.precip_mm   as number) ?? 0,
      weathercode:                row.weather_code  as number,
      windspeed_10m:              row.wind_speed    as number,
      uv_index:                   row.uv_index      as number,
      cloud_cover:                row.cloud_cover   as number,
      is_day:                     row.is_day === true ? 1 : (row.is_day === false ? 0 : 0),
    };
  });

  return { hours, days: [] }; // sunrise/sunset unused (is_day fallback in buildRawHours)
}

// ─── Data merging ────────────────────────────────────────────────────────────

function buildRawHours(
  park: DogPark,
  weather: Awaited<ReturnType<typeof fetchWeather>>,
  sunriseByDate: Map<string, string>,
  sunsetByDate: Map<string, string>,
  tz: string,
): RawHourData[] {
  const openMin = timeToMinutes(park.hours_open_time);
  const closeMin = timeToMinutes(park.hours_close_time);

  return weather.hours.map((wh) => {
    const localDate = wh.time.slice(0, 10);
    const localHour = parseInt(wh.time.slice(11, 13), 10);

    // is_park_open: if cascade gave explicit hours, use them; else use
    // dawn-dusk window from Open-Meteo sunrise/sunset
    let isParkOpen: boolean;
    if (openMin !== null && closeMin !== null) {
      const hourMin = localHour * 60;
      isParkOpen = hourMin >= openMin && hourMin < closeMin;
    } else {
      // dawn-dusk fallback
      const sunrise = sunriseByDate.get(localDate);
      const sunset = sunsetByDate.get(localDate);
      if (sunrise && sunset) {
        const sunriseHour = parseInt(sunrise.slice(11, 13), 10);
        const sunsetHour = parseInt(sunset.slice(11, 13), 10);
        isParkOpen = localHour >= sunriseHour && localHour <= sunsetHour;
      } else {
        isParkOpen = wh.is_day === 1; // last-resort fallback
      }
    }

    return {
      forecastTs: toUtcIso(wh.time, tz),
      localDate,
      localHour,
      hourLabel: formatHour(localHour),
      isDaylight: wh.is_day === 1,
      weatherCode: wh.weathercode,
      tempAir: wh.temperature_2m,
      feelsLike: wh.apparent_temperature,
      windSpeed: wh.windspeed_10m,
      precipChance: wh.precipitation_probability,
      uvIndex: wh.uv_index,
      cloudCover: wh.cloud_cover ?? null,
      isParkOpen,
    };
  });
}

// ─── Row builders ────────────────────────────────────────────────────────────

function buildHourlyRow(
  h: ScoredHour,
  park: DogPark,
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
    is_in_best_window: h.isInBestWindow,
    is_now: false,
    weather_code: h.weatherCode,
    temp_air: h.tempAir,
    feels_like: h.feelsLike,
    wind_speed: h.windSpeed,
    precip_chance: h.precipChance,
    uv_index: h.uvIndex,
    asphalt_temp: h.asphaltTemp,
    hour_status: h.hourStatus,
    wind_status: h.metricStatuses.wind_status ?? null,
    rain_status: h.metricStatuses.rain_status ?? null,
    temp_status: h.metricStatuses.temp_status ?? null,
    temp_cold_status: h.metricStatuses.temp_cold_status ?? null,
    temp_hot_status: h.metricStatuses.temp_hot_status ?? null,
    uv_status: h.metricStatuses.uv_status ?? null,
    asphalt_status: h.metricStatuses.asphalt_status ?? null,
    surface_status: h.metricStatuses.surface_status ?? null,
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

function buildDailyRow(
  park: DogPark,
  date: string,
  dayHours: ScoredHour[],
  window: BestWindow | null,
  config: DogParkScoringConfig,
  runAt: Date,
  tz: string,
  mudCaution: boolean = false,
) {
  const daylightHours = dayHours.filter((h) => h.isDaylight);
  const dayStatus = deriveDayStatus(daylightHours);
  const counts = countStatuses(daylightHours);
  const avgTemp = average(daylightHours.map((h) => h.feelsLike).filter(nonNull));
  const avgWind = average(daylightHours.map((h) => h.windSpeed).filter(nonNull));
  const avgUv = average(daylightHours.map((h) => h.uvIndex).filter(nonNull));
  const midHour = daylightHours[Math.floor(daylightHours.length / 2)];
  const summaryWeather = midHour?.weatherCode !== null && midHour?.weatherCode !== undefined
    ? wmoToSummaryWeather(midHour.weatherCode, avgWind ?? 0) : null;

  // Aggregate reason codes
  const positiveSet = new Set<string>();
  const riskSet = new Set<string>();
  for (const h of dayHours) {
    h.positiveReasonCodes.forEach((c) => positiveSet.add(c));
    h.riskReasonCodes.forEach((c) => riskSet.add(c));
  }
  if (mudCaution) {
    riskSet.add("muddy_surface");
  }

  return {
    dog_park_fid: park.fid,
    local_date: date,
    day_status: dayStatus,
    best_window_start_ts: window?.startTs ?? null,
    best_window_end_ts: window?.endTs ?? null,
    best_window_label: window?.label ?? null,
    best_window_status: window?.status ?? null,
    summary_weather: summaryWeather,
    weather_code: midHour?.weatherCode ?? null,
    avg_temp: avgTemp !== null ? round1(avgTemp) : null,
    avg_feels_like: avgTemp !== null ? round1(avgTemp) : null,
    avg_wind: avgWind !== null ? round1(avgWind) : null,
    avg_uv: avgUv !== null ? round1(avgUv) : null,
    go_hours_count: counts.go,
    advisory_hours_count: counts.advisory,
    caution_hours_count: counts.caution,
    no_go_hours_count: counts.no_go,
    positive_reason_codes: [...positiveSet],
    risk_reason_codes: [...riskSet],
    day_text: buildDayText(dayStatus, counts),
    caution_text: null,
    no_go_text: null,
    best_window_text: window?.label ?? null,
    hourly_source_max_ts: dayHours[dayHours.length - 1]?.forecastTs ?? null,
    daily_source_date: date,
    timezone: tz,
    scoring_version: config.scoring_version,
    generated_at: runAt.toISOString(),
    updated_at: runAt.toISOString(),
  };
}

function deriveDayStatus(daylightHours: ScoredHour[]): DayStatus {
  if (daylightHours.length === 0) return "no_go";
  // Use majority of non-no_go hours
  const counts = countStatuses(daylightHours);
  const nonNogo = counts.go + counts.advisory + counts.caution;
  if (nonNogo === 0) return "no_go";
  if (counts.go >= nonNogo / 2) return "go";
  if (counts.advisory >= counts.caution) return "advisory";
  return "caution";
}

function countStatuses(hours: ScoredHour[]) {
  return {
    go: hours.filter((h) => h.hourStatus === "go").length,
    advisory: hours.filter((h) => h.hourStatus === "advisory").length,
    caution: hours.filter((h) => h.hourStatus === "caution").length,
    no_go: hours.filter((h) => h.hourStatus === "no_go").length,
  };
}

function buildDayText(status: DayStatus, counts: ReturnType<typeof countStatuses>): string {
  const total = counts.go + counts.advisory + counts.caution + counts.no_go;
  if (total === 0) return "No daylight data";
  return `${counts.go}h go · ${counts.advisory}h advisory · ${counts.caution}h caution · ${counts.no_go}h no-go`;
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function timeToMinutes(t: string | null): number | null {
  if (!t) return null;
  // Accept "HH:MM" or "HH:MM:SS"
  const m = t.match(/^(\d{1,2}):(\d{2})/);
  if (!m) return null;
  return parseInt(m[1], 10) * 60 + parseInt(m[2], 10);
}

function formatHour(h: number): string {
  if (h === 0 || h === 24) return "12am";
  if (h === 12) return "12pm";
  return h < 12 ? `${h}am` : `${h - 12}pm`;
}

function average(nums: number[]): number | null {
  if (nums.length === 0) return null;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

function round1(v: number): number { return Math.round(v * 10) / 10; }
function nonNull<T>(v: T | null | undefined): v is T { return v !== null && v !== undefined; }

// Convert a local datetime string (Open-Meteo gives `YYYY-MM-DDTHH:MM` in
// the requested tz) to a UTC ISO string. Uses Intl with the IANA tz to
// compute the offset for that specific instant (DST-correct).
function toUtcIso(localIso: string, tz: string): string {
  // Build a "wall clock" Date in UTC then shift by the tz offset for that
  // instant. We compute the offset by formatting the local wall time in
  // the IANA tz and back-deriving the deltas.
  const m = localIso.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$/);
  if (!m) return new Date(localIso).toISOString();
  const [_, Y, Mo, D, H, Mi] = m.map(Number) as unknown as number[];
  const asUtc = Date.UTC(Y as number, (Mo as number) - 1, D as number, H as number, Mi as number);

  // Find offset: render asUtc in tz, see what wall time it shows; diff = offset
  const dtf = new Intl.DateTimeFormat("en-US", {
    timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", hour12: false,
  });
  const parts = Object.fromEntries(
    dtf.formatToParts(new Date(asUtc)).map((p) => [p.type, p.value]),
  );
  const renderedUtc = Date.UTC(
    parseInt(parts.year, 10),
    parseInt(parts.month, 10) - 1,
    parseInt(parts.day, 10),
    parseInt(parts.hour === "24" ? "0" : parts.hour, 10),
    parseInt(parts.minute, 10),
  );
  const offsetMs = renderedUtc - asUtc;
  return new Date(asUtc - offsetMs).toISOString();
}

async function logError(
  supabase: ReturnType<typeof createClient>,
  fid: number,
  phase: string,
  err: unknown,
): Promise<void> {
  try {
    await supabase.from("refresh_errors").insert({
      location_id: `dog_park:${fid}`,
      phase,
      error_message: String(err),
    });
  } catch (_) {
    // best-effort
  }
}
