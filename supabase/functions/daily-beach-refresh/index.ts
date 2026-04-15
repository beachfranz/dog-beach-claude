// daily-beach-refresh/index.ts
// Supabase Edge Function — orchestrates the full daily data pipeline.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { fetchWeather, wmoToSummaryWeather }   from "./openmeteo.ts";
import { fetchTides }                           from "./noaa.ts";
import { fetchCrowds, jsDayToBestTimeDay }      from "./besttime.ts";
import {
  scoreHours,
  selectBestWindows,
  applyBestWindowFlags,
  deriveBusynessCategory,
  buildHourLabel,
  type RawHourData,
  type ScoredHour,
  type BestWindow,
} from "./scoring.ts";
import {
  generateDayNarrative,
  generateHourLabels,
  type NarrativeInput,
  type WindowHour,
} from "./narrative.ts";

// ─── Inlined types (replaces ../../src/lib/types.ts import) ──────────────────

type HourStatus       = "go" | "caution" | "no_go";
type BusynessCategory = "quiet" | "moderate" | "dog_party" | "too_crowded";
type DayStatus        = "go" | "caution" | "no_go";
type SummaryWeather   = "sunny" | "partly_cloudy" | "cloudy" | "foggy" | "rainy" | "windy";

interface Beach {
  location_id: string;
  display_name: string;
  latitude: number;
  longitude: number;
  noaa_station_id: string | null;
  besttime_venue_id: string | null;
  is_active: boolean;
  timezone: string;
  open_time: string | null;
  close_time: string | null;
  address: string | null;
  website: string | null;
  description: string | null;
  parking_text: string | null;
  location_numb: number | null;
  created_at: string;
}

interface ScoringConfig {
  id: string;
  scoring_version: string;
  effective_from: string;
  description: string | null;
  is_active: boolean;
  nogo_precip_chance: number;
  nogo_wind_speed: number;
  nogo_wmo_codes: number[];
  caution_precip_chance: number;
  caution_wind_speed: number;
  caution_tide_height: number;
  caution_uv_index: number;
  positive_low_tide: number;
  positive_very_low_tide: number;
  positive_low_precip: number;
  positive_calm_wind: number;
  positive_temp_min: number;
  positive_temp_max: number;
  positive_low_uv: number;
  busy_quiet_max: number;
  busy_moderate_max: number;
  busy_dog_party_max: number;
  weight_tide: number;
  weight_rain: number;
  weight_wind: number;
  weight_crowd: number;
  weight_temp: number;
  weight_uv: number;
  norm_tide_max: number;
  norm_wind_max: number;
  norm_temp_target: number;
  norm_temp_range: number;
  norm_uv_max: number;
  window_min_hours: number;
  window_max_hours: number;
  window_caution_penalty: number;
  created_at: string;
  updated_at: string;
}

// ─── Env ──────────────────────────────────────────────────────────────────────

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const BESTTIME_KEY_PRIVATE = Deno.env.get("besttime_api_key_private")!;
const BESTTIME_KEY_PUBLIC  = Deno.env.get("besttime_api_key_public")!;
const ANTHROPIC_API_KEY    = Deno.env.get("anthropic_api_key")!;
const SCORING_VERSION      = Deno.env.get("scoring_version") ?? "v1";

console.log("ENV CHECK — all keys present:", [
  "besttime_api_key_private",
  "besttime_api_key_public",
  "anthropic_api_key",
  "scoring_version",
].map(k => `${k}=${Deno.env.get(k) ? "SET" : "MISSING"}`).join(", "));

// ─── Entry point ──────────────────────────────────────────────────────────────

Deno.serve(async (req: Request) => {
  if (req.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }

  let targetLocationIds: string[] | null = null;
  try {
    const body = await req.json().catch(() => ({}));
    if (Array.isArray(body?.location_ids) && body.location_ids.length > 0) {
      targetLocationIds = body.location_ids;
    }
  } catch { /* no body — refresh all */ }

  console.log("Request received — targetLocationIds:", targetLocationIds);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  const runAt    = new Date();
  const results: RefreshResult[] = [];

  try {
    // 1. Load active beaches
    console.log("Loading beaches from DB...");
    const { data: beaches, error: beachErr } = await (
      targetLocationIds && targetLocationIds.length > 0
        ? supabase.from("beaches").select("*").eq("is_active", true).in("location_id", targetLocationIds)
        : supabase.from("beaches").select("*").eq("is_active", true)
    );

    console.log("Beach query result — data:", beaches?.length ?? "null", "error:", beachErr?.message ?? "none");

    if (beachErr) throw new Error(`Failed to load beaches: ${beachErr.message}`);
    if (!beaches || beaches.length === 0) {
      return json({ ok: true, message: "No active beaches found", results: [] });
    }

    // 2. Load scoring config
    console.log("Loading scoring config...");
    const config = await loadScoringConfig(supabase, SCORING_VERSION);
    console.log("Scoring config loaded — version:", config.scoring_version);

    // 3. Process each beach sequentially
    for (const beach of beaches as Beach[]) {
      const result = await processBeach(beach, config, supabase, runAt);
      results.push(result);
    }

    // 4. Trigger notification dispatch (non-fatal)
    // await triggerNotificationDispatch(supabase);

    return json({ ok: true, runAt: runAt.toISOString(), results });

  } catch (err) {
    console.error("Top-level error:", String(err));
    return json({ ok: false, error: String(err) }, 500);
  }
});

// ─── Per-beach pipeline ───────────────────────────────────────────────────────

interface RefreshResult {
  locationId: string;
  ok: boolean;
  daysProcessed?: number;
  error?: string;
  phases?: Record<string, "ok" | "error" | "skipped">;
}

async function processBeach(
  beach: Beach,
  config: ScoringConfig,
  supabase: ReturnType<typeof createClient>,
  runAt: Date,
): Promise<RefreshResult> {
  const phases: Record<string, "ok" | "error" | "skipped"> = {};
  console.log(`[${beach.location_id}] Starting refresh`);

  // a. Weather
  let weatherResult: Awaited<ReturnType<typeof fetchWeather>>;
  try {
    weatherResult = await fetchWeather(beach);
    phases.openmeteo = "ok";
    console.log(`[${beach.location_id}] Weather OK — ${weatherResult.hours.length} hours`);
  } catch (err) {
    await logError(supabase, beach.location_id, "openmeteo", err);
    phases.openmeteo = "error";
    return { locationId: beach.location_id, ok: false, error: String(err), phases };
  }

  // b. Tides
  let tideMap: Map<string, number>;
  try {
    tideMap = await fetchTides(beach, runAt);
    phases.noaa = "ok";
    console.log(`[${beach.location_id}] Tides OK — ${tideMap.size} hours`);
  } catch (err) {
    await logError(supabase, beach.location_id, "noaa", err);
    phases.noaa = "error";
    return { locationId: beach.location_id, ok: false, error: String(err), phases };
  }

  // c. Crowds (non-fatal)
  let crowdResult: Awaited<ReturnType<typeof fetchCrowds>>;
  try {
    crowdResult = await fetchCrowds(beach, BESTTIME_KEY_PRIVATE, BESTTIME_KEY_PUBLIC);
    phases.besttime = "ok";
    console.log(`[${beach.location_id}] Crowds OK — ${crowdResult.busynessMap.size} slots`);
    if (crowdResult.isNewVenue) {
      await supabase
        .from("beaches")
        .update({ besttime_venue_id: crowdResult.venueId })
        .eq("location_id", beach.location_id);
      console.log(`[${beach.location_id}] Persisted venue_id: ${crowdResult.venueId}`);
    }
  } catch (err) {
    await logError(supabase, beach.location_id, "besttime", err);
    phases.besttime = "error";
    crowdResult = { busynessMap: new Map(), venueId: "", isNewVenue: false };
    console.warn(`[${beach.location_id}] BestTime failed — proceeding without crowd data`);
  }

  // d. Merge raw hours
  const rawHours = buildRawHours(beach, weatherResult, tideMap, crowdResult.busynessMap);
  console.log(`[${beach.location_id}] Built ${rawHours.length} raw hours`);

  // e. Score hours
  let scoredHours: ScoredHour[];
  try {
    scoredHours = scoreHours(rawHours, config);
    phases.scoring = "ok";
    console.log(`[${beach.location_id}] Scoring OK`);
  } catch (err) {
    await logError(supabase, beach.location_id, "scoring", err);
    phases.scoring = "error";
    return { locationId: beach.location_id, ok: false, error: String(err), phases };
  }

  // f. Select best windows
  const windows = selectBestWindows(scoredHours, config);
  applyBestWindowFlags(scoredHours, windows);

  // g. Generate narratives
  const dates = [...new Set(scoredHours.map((h) => h.localDate))].sort();
  const narrativesByDate = new Map<string, Awaited<ReturnType<typeof generateDayNarrative>>>();
  const hourLabelsByTs   = new Map<string, string>();

  // Process all days in parallel — safe since we're only parallelizing within one beach
  await Promise.all(dates.map(async (date) => {
    const dayHours = scoredHours.filter((h) => h.localDate === date);
    const window   = windows.get(date) ?? null;
    const narInput = buildNarrativeInput(beach, date, dayHours, window);
    try {
      const [narrative, hourLabels] = await Promise.all([
        generateDayNarrative(narInput, ANTHROPIC_API_KEY),
        generateHourLabels(dayHours, beach.display_name, ANTHROPIC_API_KEY),
      ]);
      narrativesByDate.set(date, narrative);
      for (const [ts, label] of hourLabels) hourLabelsByTs.set(ts, label);
      phases.narrative = "ok";
    } catch (err) {
      await logError(supabase, beach.location_id, "narrative", err);
      phases.narrative = "error";
    }
  }));

  // h. Upsert hourly rows
  try {
    const hourlyRows = scoredHours.map((h) =>
      buildHourlyRow(h, beach, config, hourLabelsByTs.get(h.forecastTs) ?? h.hourText, runAt)
    );
    for (let i = 0; i < hourlyRows.length; i += 100) {
      const { error } = await supabase
        .from("beach_day_hourly_scores")
        .upsert(hourlyRows.slice(i, i + 100), { onConflict: "location_id,forecast_ts" });
      if (error) throw new Error(error.message);
    }
    phases.upsert_hourly = "ok";
    console.log(`[${beach.location_id}] Upserted ${hourlyRows.length} hourly rows`);
  } catch (err) {
    await logError(supabase, beach.location_id, "upsert", err);
    phases.upsert_hourly = "error";
    return { locationId: beach.location_id, ok: false, error: String(err), phases };
  }

  // i. Upsert daily rows
  try {
    const dailyRows = dates.map((date) => {
      const dayHours  = scoredHours.filter((h) => h.localDate === date);
      const window    = windows.get(date) ?? null;
      const narrative = narrativesByDate.get(date);
      return buildDailyRow(beach, date, dayHours, window, narrative, config, runAt);
    });
    const { error } = await supabase
      .from("beach_day_recommendations")
      .upsert(dailyRows, { onConflict: "location_id,local_date" });
    if (error) throw new Error(error.message);
    phases.upsert_daily = "ok";
    console.log(`[${beach.location_id}] Upserted ${dailyRows.length} daily rows`);
  } catch (err) {
    await logError(supabase, beach.location_id, "upsert", err);
    phases.upsert_daily = "error";
    return { locationId: beach.location_id, ok: false, error: String(err), phases };
  }

  console.log(`[${beach.location_id}] Refresh complete — ${dates.length} days`);
  return { locationId: beach.location_id, ok: true, daysProcessed: dates.length, phases };
}

// ─── Data merging ─────────────────────────────────────────────────────────────

function buildRawHours(
  beach: Beach,
  weather: Awaited<ReturnType<typeof fetchWeather>>,
  tideMap: Map<string, number>,
  busynessMap: Map<string, number>,
): RawHourData[] {
  const openMinutes  = timeToMinutes(beach.open_time  ?? "00:00");
  const closeMinutes = timeToMinutes(beach.close_time ?? "23:59");

  return weather.hours.map((wh) => {
    const localDate = wh.time.slice(0, 10);
    const localHour = parseInt(wh.time.slice(11, 13), 10);

    const tideKey    = `${localDate} ${String(localHour).padStart(2, "0")}`;
    const tideHeight = tideMap.get(tideKey) ?? null;

    const jsDate      = new Date(`${localDate}T${String(localHour).padStart(2, "0")}:00:00`);
    const btDay       = jsDayToBestTimeDay(jsDate.getDay());
    const busynessKey = `${btDay}:${localHour}`;
    const busyness    = busynessMap.get(busynessKey) ?? null;

    const hourMinutes = localHour * 60;
    const isBeachOpen = hourMinutes >= openMinutes && hourMinutes < closeMinutes;

    return {
      forecastTs:    toUtcIso(wh.time, beach.timezone),
      localDate,
      localHour,
      hourLabel:     buildHourLabel(localHour),
      isDaylight:    wh.is_day === 1,
      weatherCode:   wh.weathercode,
      tempAir:       wh.temperature_2m,
      windSpeed:     wh.windspeed_10m,
      precipChance:  wh.precipitation_probability,
      uvIndex:       wh.uv_index,
      tideHeight,
      busynessScore: busyness,
      isBeachOpen,
    };
  });
}

// ─── Row builders ─────────────────────────────────────────────────────────────

function buildHourlyRow(
  h: ScoredHour,
  beach: Beach,
  config: ScoringConfig,
  hourText: string,
  runAt: Date,
) {
  return {
    location_id:           beach.location_id,
    local_date:            h.localDate,
    forecast_ts:           h.forecastTs,
    local_hour:            h.localHour,
    hour_label:            h.hourLabel,
    is_daylight:           h.isDaylight,
    is_candidate_window:   h.isCandidateWindow,
    is_in_best_window:     h.isInBestWindow,
    weather_code:          h.weatherCode,
    temp_air:              h.tempAir,
    wind_speed:            h.windSpeed,
    precip_chance:         h.precipChance,
    uv_index:              h.uvIndex,
    tide_height:           h.tideHeight,
    busyness_score:        h.busynessScore,
    busyness_category:     h.busynessCategory,
    hour_status:           h.hourStatus,
    hour_score:            h.hourScore,
    passed_checks:         h.passedChecks,
    failed_checks:         h.failedChecks,
    positive_reason_codes: h.positiveReasonCodes,
    risk_reason_codes:     h.riskReasonCodes,
    explainability:        h.explainability,
    tide_score:            h.explainability.tide_score  ?? null,
    wind_score:            h.explainability.wind_score  ?? null,
    crowd_score:           h.explainability.crowd_score ?? null,
    rain_score:            h.explainability.rain_score  ?? null,
    temp_score:            h.explainability.temp_score  ?? null,
    uv_score:              h.explainability.uv_score    ?? null,
    hour_text:             hourText,
    timezone:              beach.timezone,
    scoring_version:       config.scoring_version,
    generated_at:          runAt.toISOString(),
  };
}

function buildDailyRow(
  beach: Beach,
  date: string,
  dayHours: ScoredHour[],
  window: BestWindow | null,
  narrative: Awaited<ReturnType<typeof generateDayNarrative>> | undefined,
  config: ScoringConfig,
  runAt: Date,
) {
  const goHours      = dayHours.filter((h) => h.hourStatus === "go");
  const cautionHours = dayHours.filter((h) => h.hourStatus === "caution");
  const noGoHours    = dayHours.filter((h) => h.hourStatus === "no_go");

  const dayStatus: DayStatus =
    goHours.length > 0       ? "go"
    : cautionHours.length > 0 ? "caution"
    : "no_go";

  const aggHours    = window?.hours ?? dayHours.filter((h) => h.isDaylight);
  const avgTemp     = average(aggHours.map((h) => h.tempAir).filter(nonNull));
  const avgWind     = average(aggHours.map((h) => h.windSpeed).filter(nonNull));
  const avgUv       = average(aggHours.map((h) => h.uvIndex).filter(nonNull));
  const avgTide     = average(aggHours.map((h) => h.tideHeight).filter(nonNull));
  const lowestTide  = Math.min(...aggHours.map((h) => h.tideHeight ?? Infinity));
  const avgBusyness = average(aggHours.map((h) => h.busynessScore).filter(nonNull));

  const dominantCode   = mostCommon(aggHours.map((h) => h.weatherCode).filter(nonNull));
  const summaryWeather: SummaryWeather | null = dominantCode !== null
    ? wmoToSummaryWeather(dominantCode, avgWind ?? 0)
    : null;

  const positiveSet = new Set<string>();
  const riskSet     = new Set<string>();
  for (const h of aggHours) {
    h.positiveReasonCodes.forEach((c) => positiveSet.add(c));
    h.riskReasonCodes.forEach((c) => riskSet.add(c));
  }

  const thresholdsUsed = {
    scoring_version:       config.scoring_version,
    nogo_precip_chance:    config.nogo_precip_chance,
    nogo_wind_speed:       config.nogo_wind_speed,
    caution_precip_chance: config.caution_precip_chance,
    caution_wind_speed:    config.caution_wind_speed,
    caution_tide_height:   config.caution_tide_height,
    weight_tide:           config.weight_tide,
    weight_rain:           config.weight_rain,
    weight_wind:           config.weight_wind,
    weight_crowd:          config.weight_crowd,
  };

  return {
    location_id:           beach.location_id,
    local_date:            date,
    day_status:            dayStatus,
    best_window_start_ts:  window?.startTs ?? null,
    best_window_end_ts:    window?.endTs   ?? null,
    best_window_label:     window?.label   ?? null,
    best_window_status:    window?.status  ?? null,
    summary_weather:       summaryWeather,
    weather_code:          dominantCode,
    avg_temp:              round1(avgTemp),
    avg_wind:              round1(avgWind),
    avg_uv:                round1(avgUv),
    avg_tide_height:       round1(avgTide),
    lowest_tide_height:    lowestTide === Infinity ? null : round1(lowestTide),
    avg_busyness_score:    round1(avgBusyness),
    busyness_category:     deriveBusynessCategory(avgBusyness, config),
    go_hours_count:        goHours.length,
    caution_hours_count:   cautionHours.length,
    no_go_hours_count:     noGoHours.length,
    positive_reason_codes: [...positiveSet],
    risk_reason_codes:     [...riskSet],
    explainability:        {},
    thresholds_used:       thresholdsUsed,
    day_text:              narrative?.dayText        ?? null,
    caution_text:          narrative?.cautionText    ?? null,
    no_go_text:            narrative?.noGoText       ?? null,
    best_window_text:      narrative?.bestWindowText ?? null,
    hourly_source_max_ts:  maxTs(dayHours.map((h) => h.forecastTs)),
    crowd_source_max_ts:   null,
    daily_source_date:     date,
    timezone:              beach.timezone,
    scoring_version:       config.scoring_version,
    generated_at:          runAt.toISOString(),
  };
}

// ─── Narrative input builder ──────────────────────────────────────────────────

function buildNarrativeInput(
  beach: Beach,
  date: string,
  dayHours: ScoredHour[],
  window: BestWindow | null,
): NarrativeInput {
  const jsDate    = new Date(`${date}T12:00:00`);
  const dayOfWeek = jsDate.toLocaleDateString("en-US", { weekday: "long" });
  const isWeekend = dayOfWeek === "Saturday" || dayOfWeek === "Sunday";

  const goHours      = dayHours.filter((h) => h.hourStatus === "go");
  const cautionHours = dayHours.filter((h) => h.hourStatus === "caution");
  const noGoHours    = dayHours.filter((h) => h.hourStatus === "no_go");

  const aggHours  = window?.hours ?? dayHours.filter((h) => h.isDaylight);
  const dayStatus: DayStatus =
    goHours.length > 0       ? "go"
    : cautionHours.length > 0 ? "caution"
    : "no_go";

  // Tide direction: compare first vs last tide in best window (or agg hours)
  const tideSeries = aggHours.map((h) => h.tideHeight).filter(nonNull);
  let tideDirection: "rising" | "falling" | "steady" = "steady";
  if (tideSeries.length >= 2) {
    const diff = tideSeries[tideSeries.length - 1] - tideSeries[0];
    if (diff > 0.15)       tideDirection = "rising";
    else if (diff < -0.15) tideDirection = "falling";
  }

  // Dominant weather code across agg hours
  const weatherCode = mostCommon(aggHours.map((h) => h.weatherCode).filter(nonNull));

  // Per-hour breakdown of best window
  const windowHourBreakdown: WindowHour[] = (window?.hours ?? [])
    .filter((h) => h.hourStatus === "go" || h.hourStatus === "caution")
    .map((h) => ({
      hour:   h.hourLabel,
      tide:   h.tideHeight !== null ? round1(h.tideHeight) : null,
      wind:   h.windSpeed  !== null ? Math.round(h.windSpeed) : null,
      temp:   h.tempAir    !== null ? Math.round(h.tempAir)   : null,
      rain:   h.precipChance !== null ? Math.round(h.precipChance) : null,
      crowd:  h.busynessCategory,
      status: h.hourStatus as "go" | "caution",
    }));

  const positiveSet = new Set<string>();
  const riskSet     = new Set<string>();
  for (const h of aggHours) {
    h.positiveReasonCodes.forEach((c) => positiveSet.add(c));
    h.riskReasonCodes.forEach((c) => riskSet.add(c));
  }

  return {
    beachName:            beach.display_name,
    localDate:            date,
    dayOfWeek,
    isWeekend,
    dayStatus,
    bestWindow:           window,
    weatherCode,
    tideDirection,
    windowHourBreakdown,
    avgTemp:              round1(average(aggHours.map((h) => h.tempAir).filter(nonNull))),
    avgWind:              round1(average(aggHours.map((h) => h.windSpeed).filter(nonNull))),
    avgPrecip:            round1(average(aggHours.map((h) => h.precipChance).filter(nonNull))),
    avgTide:              round1(average(aggHours.map((h) => h.tideHeight).filter(nonNull))),
    lowestTide:           round1(Math.min(...aggHours.map((h) => h.tideHeight ?? Infinity))),
    avgUv:                round1(average(aggHours.map((h) => h.uvIndex).filter(nonNull))),
    avgBusyness:          round1(average(aggHours.map((h) => h.busynessScore).filter(nonNull))),
    busynessCategory:     window?.hours[0]?.busynessCategory ?? null,
    positiveReasonCodes:  [...positiveSet],
    riskReasonCodes:      [...riskSet],
    goHoursCount:         goHours.length,
    cautionHoursCount:    cautionHours.length,
    noGoHoursCount:       noGoHours.length,
  };
}

// ─── DB helpers ───────────────────────────────────────────────────────────────

async function loadScoringConfig(
  supabase: ReturnType<typeof createClient>,
  version: string,
): Promise<ScoringConfig> {
  const today = new Date().toISOString().slice(0, 10);
  const { data, error } = await supabase
    .from("scoring_config")
    .select("*")
    .eq("scoring_version", version)
    .eq("is_active", true)
    .lte("effective_from", today)
    .order("effective_from", { ascending: false })
    .limit(1)
    .single();

  if (error || !data) {
    throw new Error(`Failed to load scoring config v${version}: ${error?.message ?? "not found"}`);
  }
  return data as ScoringConfig;
}

async function logError(
  supabase: ReturnType<typeof createClient>,
  locationId: string | null,
  phase: string,
  err: unknown,
): Promise<void> {
  const message = err instanceof Error ? err.message : String(err);
  console.error(`[${locationId ?? "global"}] ${phase} error: ${message}`);
  await supabase.from("refresh_errors").insert({
    location_id:   locationId,
    phase,
    error_message: message,
    error_detail:  err instanceof Error ? { stack: err.stack } : { raw: String(err) },
  });
}

async function triggerNotificationDispatch(
  supabase: ReturnType<typeof createClient>,
): Promise<void> {
  try {
    await supabase.functions.invoke("notification-dispatch", {
      body: { triggered_by: "daily-beach-refresh" },
    });
  } catch (err) {
    console.error("Failed to trigger notification-dispatch:", err);
  }
}

// ─── Utility helpers ──────────────────────────────────────────────────────────

function timeToMinutes(time: string): number {
  const [h, m] = time.split(":").map(Number);
  return h * 60 + (m ?? 0);
}

function toUtcIso(localIso: string, timezone: string): string {
  const [datePart, timePart] = localIso.split("T");
  const [year, month, day]   = datePart.split("-").map(Number);
  const [hour]               = timePart.split(":").map(Number);
  const utcGuess  = new Date(Date.UTC(year, month - 1, day, hour, 0, 0));
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", hour12: false,
  });
  const parts = Object.fromEntries(
    formatter.formatToParts(utcGuess).map((p) => [p.type, p.value])
  );
  const localFromUtc = new Date(Date.UTC(
    Number(parts.year), Number(parts.month) - 1, Number(parts.day),
    Number(parts.hour), Number(parts.minute),
  ));
  const offsetMs  = localFromUtc.getTime() - utcGuess.getTime();
  return new Date(utcGuess.getTime() - offsetMs).toISOString();
}

function average(nums: number[]): number | null {
  if (nums.length === 0) return null;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

function nonNull<T>(val: T | null | undefined): val is T {
  return val !== null && val !== undefined;
}

function round1(val: number | null | undefined): number | null {
  return val != null ? Math.round(val * 10) / 10 : null;
}

function mostCommon<T>(arr: T[]): T | null {
  if (arr.length === 0) return null;
  const freq = new Map<T, number>();
  for (const v of arr) freq.set(v, (freq.get(v) ?? 0) + 1);
  return [...freq.entries()].sort((a, b) => b[1] - a[1])[0][0];
}

function maxTs(timestamps: string[]): string | null {
  if (timestamps.length === 0) return null;
  return timestamps.reduce((a, b) => (a > b ? a : b));
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
