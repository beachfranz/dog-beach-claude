// daily-beach-refresh/index.ts
// Supabase Edge Function — orchestrates the full daily data pipeline.
//
// Execution order per beach:
//   1. Fetch active beaches from DB
//   2. For each beach (sequential to respect API rate limits):
//      a. Fetch weather        ← Open-Meteo
//      b. Fetch tides          ← NOAA CO-OPS
//      c. Fetch crowds         ← BestTime.app
//      d. Merge into RawHourData[]
//      e. Score all hours      ← scoring.ts
//      f. Select best windows  ← scoring.ts
//      g. Generate narratives  ← narrative.ts
//      h. Upsert hourly rows   ← beach_day_hourly_scores
//      i. Upsert daily rows    ← beach_day_recommendations
//      j. Persist venue_id     ← beaches (if new BestTime venue)
//   3. Invoke notification-dispatch function
//
// Invoke manually:
//   POST /functions/v1/daily-beach-refresh
//   Body (optional): { "location_ids": ["huntington-dog-beach"] }
//   Omit body to refresh all active beaches.

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
} from "./narrative.ts";
import type {
  Beach,
  ScoringConfig,
  DayStatus,
  SummaryWeather,
} from "../../src/lib/types.ts";

// ─── Env ──────────────────────────────────────────────────────────────────────

const SUPABASE_URL            = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY    = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const BESTTIME_KEY_PRIVATE    = Deno.env.get("besttime_api_key_private")!;
const BESTTIME_KEY_PUBLIC     = Deno.env.get("besttime_api_key_public")!;
const ANTHROPIC_API_KEY       = Deno.env.get("anthropic_api_key")!;
const SCORING_VERSION         = Deno.env.get("scoring_version") ?? "v1";

// ─── Entry point ──────────────────────────────────────────────────────────────

Deno.serve(async (req: Request) => {
  // Only allow POST
  if (req.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }

  // Optional body: { location_ids: string[] }
  let targetLocationIds: string[] | null = null;
  try {
    const body = await req.json().catch(() => ({}));
    if (Array.isArray(body?.location_ids) && body.location_ids.length > 0) {
      targetLocationIds = body.location_ids;
    }
  } catch { /* no body — refresh all */ }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  const runAt    = new Date();
  const results: RefreshResult[] = [];

  try {
    // 1. Load active beaches
    let beachQuery = supabase
      .from("beaches")
      .select("*")
      .eq("is_active", true);
    if (targetLocationIds) {
      beachQuery = beachQuery.in("location_id", targetLocationIds);
    }
    const { data: beaches, error: beachErr } = await beachQuery;
    if (beachErr) throw new Error(`Failed to load beaches: ${beachErr.message}`);
    if (!beaches || beaches.length === 0) {
      return json({ ok: true, message: "No active beaches found", results: [] });
    }

    // 2. Load active scoring config
    const config = await loadScoringConfig(supabase, SCORING_VERSION);

    // 3. Process each beach sequentially
    for (const beach of beaches as Beach[]) {
      const result = await processBeach(beach, config, supabase, runAt);
      results.push(result);
    }

    // 4. Trigger notification dispatch
    await triggerNotificationDispatch(supabase);

    return json({
      ok:      true,
      runAt:   runAt.toISOString(),
      results,
    });

  } catch (err) {
    console.error("daily-beach-refresh top-level error:", err);
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

  // ── a. Weather ──────────────────────────────────────────────────────────────
  let weatherResult: Awaited<ReturnType<typeof fetchWeather>>;
  try {
    weatherResult = await fetchWeather(beach);
    phases.openmeteo = "ok";
  } catch (err) {
    await logError(supabase, beach.location_id, "openmeteo", err);
    phases.openmeteo = "error";
    return { locationId: beach.location_id, ok: false, error: String(err), phases };
  }

  // ── b. Tides ────────────────────────────────────────────────────────────────
  let tideMap: Map<string, number>;
  try {
    tideMap = await fetchTides(beach, runAt);
    phases.noaa = "ok";
  } catch (err) {
    await logError(supabase, beach.location_id, "noaa", err);
    phases.noaa = "error";
    return { locationId: beach.location_id, ok: false, error: String(err), phases };
  }

  // ── c. Crowds ───────────────────────────────────────────────────────────────
  let crowdResult: Awaited<ReturnType<typeof fetchCrowds>>;
  try {
    crowdResult = await fetchCrowds(beach, BESTTIME_KEY_PRIVATE, BESTTIME_KEY_PUBLIC);
    phases.besttime = "ok";

    // Persist new venue_id if this was first registration
    if (crowdResult.isNewVenue) {
      await supabase
        .from("beaches")
        .update({ besttime_venue_id: crowdResult.venueId })
        .eq("location_id", beach.location_id);
      console.log(`[${beach.location_id}] Persisted BestTime venue_id: ${crowdResult.venueId}`);
    }
  } catch (err) {
    // Crowd data failure is non-fatal — continue with null busyness scores
    await logError(supabase, beach.location_id, "besttime", err);
    phases.besttime = "error";
    crowdResult = { busynessMap: new Map(), venueId: "", isNewVenue: false };
    console.warn(`[${beach.location_id}] BestTime failed — proceeding without crowd data`);
  }

  // ── d. Merge into RawHourData[] ─────────────────────────────────────────────
  const rawHours = buildRawHours(beach, weatherResult, tideMap, crowdResult.busynessMap);

  // ── e. Score hours ──────────────────────────────────────────────────────────
  let scoredHours: ScoredHour[];
  try {
    scoredHours = scoreHours(rawHours, config);
    phases.scoring = "ok";
  } catch (err) {
    await logError(supabase, beach.location_id, "scoring", err);
    phases.scoring = "error";
    return { locationId: beach.location_id, ok: false, error: String(err), phases };
  }

  // ── f. Select best windows ──────────────────────────────────────────────────
  const windows = selectBestWindows(scoredHours, config);
  applyBestWindowFlags(scoredHours, windows);

  // ── g. Generate narratives (one API call per day) ───────────────────────────
  const dates = [...new Set(scoredHours.map((h) => h.localDate))].sort();
  const narrativesByDate = new Map<string, Awaited<ReturnType<typeof generateDayNarrative>>>();
  const hourLabelsByTs   = new Map<string, string>();

  for (const date of dates) {
    const dayHours  = scoredHours.filter((h) => h.localDate === date);
    const window    = windows.get(date) ?? null;
    const narInput  = buildNarrativeInput(beach, date, dayHours, window);

    try {
      const narrative = await generateDayNarrative(narInput, ANTHROPIC_API_KEY);
      narrativesByDate.set(date, narrative);

      const hourLabels = await generateHourLabels(dayHours, beach.display_name, ANTHROPIC_API_KEY);
      for (const [ts, label] of hourLabels) {
        hourLabelsByTs.set(ts, label);
      }
      phases.narrative = "ok";
    } catch (err) {
      await logError(supabase, beach.location_id, "narrative", err);
      phases.narrative = "error";
      // Non-fatal — narrative fields will be null
    }
  }

  // ── h. Upsert hourly rows ───────────────────────────────────────────────────
  try {
    const hourlyRows = scoredHours.map((h) =>
      buildHourlyRow(h, beach, config, hourLabelsByTs.get(h.forecastTs) ?? h.hourText, runAt)
    );

    // Upsert in batches of 100 to stay within Supabase payload limits
    for (let i = 0; i < hourlyRows.length; i += 100) {
      const batch = hourlyRows.slice(i, i + 100);
      const { error } = await supabase
        .from("beach_day_hourly_scores")
        .upsert(batch, { onConflict: "location_id,forecast_ts" });
      if (error) throw new Error(error.message);
    }
    phases.upsert_hourly = "ok";
  } catch (err) {
    await logError(supabase, beach.location_id, "upsert", err);
    phases.upsert_hourly = "error";
    return { locationId: beach.location_id, ok: false, error: String(err), phases };
  }

  // ── i. Upsert daily recommendation rows ────────────────────────────────────
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
  } catch (err) {
    await logError(supabase, beach.location_id, "upsert", err);
    phases.upsert_daily = "error";
    return { locationId: beach.location_id, ok: false, error: String(err), phases };
  }

  console.log(`[${beach.location_id}] Refresh complete — ${dates.length} days processed`);
  return {
    locationId:    beach.location_id,
    ok:            true,
    daysProcessed: dates.length,
    phases,
  };
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
    // wh.time = "YYYY-MM-DDTHH:00" (local, no TZ suffix from Open-Meteo)
    const localDate = wh.time.slice(0, 10);
    const localHour = parseInt(wh.time.slice(11, 13), 10);

    // Tide: match on "YYYY-MM-DD HH" key
    const tideKey    = `${localDate} ${String(localHour).padStart(2, "0")}`;
    const tideHeight = tideMap.get(tideKey) ?? null;

    // Crowd: match on BestTime day index + hour
    const jsDate      = new Date(`${localDate}T${String(localHour).padStart(2, "0")}:00:00`);
    const btDay       = jsDayToBestTimeDay(jsDate.getDay());
    const busynessKey = `${btDay}:${localHour}`;
    const busyness    = busynessMap.get(busynessKey) ?? null;

    // Beach open check
    const hourMinutes  = localHour * 60;
    const isBeachOpen  = hourMinutes >= openMinutes && hourMinutes < closeMinutes;

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

  // Day status: go if any go hours, caution if only caution, no_go if neither
  const dayStatus: DayStatus =
    goHours.length > 0      ? "go"
    : cautionHours.length > 0 ? "caution"
    : "no_go";

  // Aggregate over window hours if available, else all daylight hours
  const aggregateHours = window?.hours ?? dayHours.filter((h) => h.isDaylight);

  const avgTemp      = average(aggregateHours.map((h) => h.tempAir).filter(nonNull));
  const avgWind      = average(aggregateHours.map((h) => h.windSpeed).filter(nonNull));
  const avgUv        = average(aggregateHours.map((h) => h.uvIndex).filter(nonNull));
  const avgTide      = average(aggregateHours.map((h) => h.tideHeight).filter(nonNull));
  const lowestTide   = Math.min(...aggregateHours.map((h) => h.tideHeight ?? Infinity));
  const avgBusyness  = average(aggregateHours.map((h) => h.busynessScore).filter(nonNull));

  // Summary weather: use most common weather code from window/daylight hours
  const dominantCode = mostCommon(aggregateHours.map((h) => h.weatherCode).filter(nonNull));
  const avgWindVal   = avgWind ?? 0;
  const summaryWeather: SummaryWeather | null = dominantCode !== null
    ? wmoToSummaryWeather(dominantCode, avgWindVal)
    : null;

  // Positive/risk codes: union across all window hours
  const positiveSet = new Set<string>();
  const riskSet     = new Set<string>();
  for (const h of aggregateHours) {
    h.positiveReasonCodes.forEach((c) => positiveSet.add(c));
    h.riskReasonCodes.forEach((c) => riskSet.add(c));
  }

  // thresholds_used: snapshot of config for reproducibility
  const thresholdsUsed = {
    scoring_version:      config.scoring_version,
    nogo_precip_chance:   config.nogo_precip_chance,
    nogo_wind_speed:      config.nogo_wind_speed,
    caution_precip_chance:config.caution_precip_chance,
    caution_wind_speed:   config.caution_wind_speed,
    caution_tide_height:  config.caution_tide_height,
    weight_tide:          config.weight_tide,
    weight_rain:          config.weight_rain,
    weight_wind:          config.weight_wind,
    weight_crowd:         config.weight_crowd,
  };

  return {
    location_id:              beach.location_id,
    local_date:               date,
    day_status:               dayStatus,
    best_window_start_ts:     window?.startTs ?? null,
    best_window_end_ts:       window?.endTs   ?? null,
    best_window_label:        window?.label   ?? null,
    best_window_status:       window?.status  ?? null,
    summary_weather:          summaryWeather,
    weather_code:             dominantCode,
    avg_temp:                 round1(avgTemp),
    avg_wind:                 round1(avgWind),
    avg_uv:                   round1(avgUv),
    avg_tide_height:          round1(avgTide),
    lowest_tide_height:       lowestTide === Infinity ? null : round1(lowestTide),
    avg_busyness_score:       round1(avgBusyness),
    busyness_category:        deriveBusynessCategory(avgBusyness, config),
    go_hours_count:           goHours.length,
    caution_hours_count:      cautionHours.length,
    no_go_hours_count:        noGoHours.length,
    positive_reason_codes:    [...positiveSet],
    risk_reason_codes:        [...riskSet],
    explainability:           {},
    thresholds_used:          thresholdsUsed,
    day_text:                 narrative?.dayText        ?? null,
    caution_text:             narrative?.cautionText    ?? null,
    no_go_text:               narrative?.noGoText       ?? null,
    best_window_text:         narrative?.bestWindowText ?? null,
    hourly_source_max_ts:     maxTs(dayHours.map((h) => h.forecastTs)),
    crowd_source_max_ts:      null,   // BestTime is weekly pattern, no live ts
    daily_source_date:        date,
    timezone:                 beach.timezone,
    scoring_version:          config.scoring_version,
    generated_at:             runAt.toISOString(),
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

  const goHours      = dayHours.filter((h) => h.hourStatus === "go");
  const cautionHours = dayHours.filter((h) => h.hourStatus === "caution");
  const noGoHours    = dayHours.filter((h) => h.hourStatus === "no_go");

  const aggHours  = window?.hours ?? dayHours.filter((h) => h.isDaylight);
  const dayStatus: DayStatus =
    goHours.length > 0       ? "go"
    : cautionHours.length > 0 ? "caution"
    : "no_go";

  const positiveSet = new Set<string>();
  const riskSet     = new Set<string>();
  for (const h of aggHours) {
    h.positiveReasonCodes.forEach((c) => positiveSet.add(c));
    h.riskReasonCodes.forEach((c) => riskSet.add(c));
  }

  return {
    beachName:           beach.display_name,
    localDate:           date,
    dayOfWeek,
    dayStatus,
    bestWindow:          window,
    avgTemp:             round1(average(aggHours.map((h) => h.tempAir).filter(nonNull))),
    avgWind:             round1(average(aggHours.map((h) => h.windSpeed).filter(nonNull))),
    avgPrecip:           round1(average(aggHours.map((h) => h.precipChance).filter(nonNull))),
    avgTide:             round1(average(aggHours.map((h) => h.tideHeight).filter(nonNull))),
    lowestTide:          round1(Math.min(...aggHours.map((h) => h.tideHeight ?? Infinity))),
    avgUv:               round1(average(aggHours.map((h) => h.uvIndex).filter(nonNull))),
    avgBusyness:         round1(average(aggHours.map((h) => h.busynessScore).filter(nonNull))),
    busynessCategory:    window?.hours[0]?.busynessCategory ?? null,
    positiveReasonCodes: [...positiveSet],
    riskReasonCodes:     [...riskSet],
    goHoursCount:        goHours.length,
    cautionHoursCount:   cautionHours.length,
    noGoHoursCount:      noGoHours.length,
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
    console.log("notification-dispatch triggered");
  } catch (err) {
    // Non-fatal — log but don't fail the refresh
    console.error("Failed to trigger notification-dispatch:", err);
    await logError(supabase, null, "notification", err);
  }
}

// ─── Utility helpers ──────────────────────────────────────────────────────────

function timeToMinutes(time: string): number {
  const [h, m] = time.split(":").map(Number);
  return h * 60 + (m ?? 0);
}

// Convert Open-Meteo local ISO string to UTC ISO string.
// Open-Meteo returns "YYYY-MM-DDTHH:00" without TZ info — it IS local time
// for the requested timezone. We reconstruct a proper UTC timestamp.
function toUtcIso(localIso: string, timezone: string): string {
  // Use Intl to determine the UTC offset for this instant in the given timezone.
  // We parse the local time as if it's in that timezone.
  const [datePart, timePart] = localIso.split("T");
  const [year, month, day]   = datePart.split("-").map(Number);
  const [hour]               = timePart.split(":").map(Number);

  // Build a Date by finding the UTC time that corresponds to localIso in timezone.
  // Strategy: try UTC, compute offset, adjust.
  const utcGuess = new Date(Date.UTC(year, month - 1, day, hour, 0, 0));
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", hour12: false,
  });
  const parts    = Object.fromEntries(
    formatter.formatToParts(utcGuess).map((p) => [p.type, p.value])
  );
  const localFromUtc = new Date(Date.UTC(
    Number(parts.year), Number(parts.month) - 1, Number(parts.day),
    Number(parts.hour), Number(parts.minute),
  ));
  const offsetMs = localFromUtc.getTime() - utcGuess.getTime();
  const utcActual = new Date(utcGuess.getTime() - offsetMs);
  return utcActual.toISOString();
}

function average(nums: number[]): number | null {
  if (nums.length === 0) return null;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

function nonNull<T>(val: T | null | undefined): val is T {
  return val !== null && val !== undefined;
}

function round1(val: number | null): number | null {
  return val !== null ? Math.round(val * 10) / 10 : null;
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
