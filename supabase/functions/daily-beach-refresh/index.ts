// daily-beach-refresh/index.ts
// Supabase Edge Function — orchestrates the full daily data pipeline.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }  from "../_shared/cors.ts";
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

// ─── Inlined types (replaces ../../src/lib/types.ts import) ──────────────────

type HourStatus       = "go" | "advisory" | "caution" | "no_go";
type BusynessCategory = "quiet" | "moderate" | "dog_party" | "too_crowded";
type DayStatus        = "go" | "advisory" | "caution" | "no_go";
type SummaryWeather   = "sunny" | "partly_cloudy" | "cloudy" | "foggy" | "rainy" | "windy";
type BacteriaRisk     = "none" | "low" | "moderate" | "high";

interface Beach {
  location_id: string;
  arena_group_id: number | null;   // path 3a dual-key bridge to beaches_gold
  display_name: string;
  latitude: number;
  longitude: number;
  noaa_station_id: string | null;
  besttime_venue_id: string | null;
  is_active: boolean;
  timezone: string;
  open_time: string | null;
  close_time: string | null;
  dogs_prohibited_start: string | null;
  dogs_prohibited_end:   string | null;
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
  // No-go thresholds
  nogo_precip_chance: number;
  nogo_wind_speed: number;
  nogo_wmo_codes: number[];
  nogo_uv_index: number;
  nogo_temp_hot_max: number;
  // Caution thresholds
  caution_precip_chance: number;
  caution_wind_speed: number;
  caution_tide_height: number;
  caution_uv_index: number;
  caution_wmo_codes: number[];
  caution_temp_cold_min: number;
  caution_temp_hot_max: number;
  advisory_crowd_max: number;
  // Advisory thresholds
  advisory_precip_chance: number;
  advisory_wind_speed: number;
  advisory_tide_height: number;
  advisory_uv_index: number;
  advisory_temp_cold_min: number;
  go_temp_cold_min: number;
  advisory_temp_hot_max: number;
  advisory_crowd_min: number;
  // Surface temp thresholds
  advisory_sand_temp: number;
  caution_sand_temp: number;
  nogo_sand_temp: number;
  advisory_asphalt_temp: number;
  caution_asphalt_temp: number;
  // Positive signals
  positive_low_tide: number;
  positive_very_low_tide: number;
  positive_low_precip: number;
  positive_calm_wind: number;
  positive_temp_min: number;
  positive_temp_max: number;
  positive_low_uv: number;
  // Busyness categories
  busy_quiet_max: number;
  busy_moderate_max: number;
  busy_dog_party_max: number;
  // Weights
  weight_tide: number;
  weight_rain: number;
  weight_wind: number;
  weight_crowd: number;
  weight_temp: number;
  weight_uv: number;
  weight_weather_code: number;
  // Normalisation
  norm_tide_max: number;
  norm_wind_max: number;
  norm_temp_target: number;
  norm_temp_range: number;
  norm_uv_max: number;
  // Window selection
  window_min_hours: number;
  window_max_hours: number;
  window_caution_penalty: number;
  window_score_threshold: number;
  // Bacteria thresholds
  bacteria_caution_mm?: number;
  bacteria_nogo_mm?: number;
  created_at: string;
  updated_at: string;
}

// ─── Env ──────────────────────────────────────────────────────────────────────

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const BESTTIME_KEY_PRIVATE = Deno.env.get("besttime_api_key_private")!;
const BESTTIME_KEY_PUBLIC  = Deno.env.get("besttime_api_key_public")!;
const SCORING_VERSION      = Deno.env.get("scoring_version") ?? "v1";

console.log("ENV CHECK — all keys present:", [
  "besttime_api_key_private",
  "besttime_api_key_public",
  "anthropic_api_key",
  "scoring_version",
].map(k => `${k}=${Deno.env.get(k) ? "SET" : "MISSING"}`).join(", "));

// ─── Entry point ──────────────────────────────────────────────────────────────

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "POST, OPTIONS"), "Content-Type": "application/json" };
  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });
  if (req.method !== "POST") {
    return new Response("Method not allowed", { status: 405, headers: cors });
  }

  // Auth gate. Deployed with --no-verify-jwt so the admin editor can
  // proxy a call through admin-refresh-beach (the gateway JWT check
  // rejects our sb_secret_-format service-role key). requireAdmin()
  // recreates that gate at the function level.
  const { requireAdmin } = await import("../_shared/admin-auth.ts");
  const authFail = await requireAdmin(req, cors);
  if (authFail) return authFail;

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
    // 1. Load scoreable beaches from the spine.
    //
    // Path 3b-3.1: source of truth is beaches_gold + the is_scoreable gate.
    // INNER JOIN to public.beaches because the scoring tables still PK on
    // location_id (NOT NULL) until the next migration retires that column.
    // beach_dog_policy supplies dogs_prohibited_start/end (overlay table).
    //
    // The big swap was 15 → 15: today's curated set is exactly the rows
    // where beaches_gold.is_scoreable=true. Future seeds opt in via
    // seed_arena_beach.py --score (sets is_scoreable=true).
    console.log("Loading beaches from beaches_gold (is_scoreable=true)...");
    let beachQuery = supabase.from("beaches_gold")
      .select(`
        fid,
        name,
        display_name_override,
        lat,
        lon,
        noaa_station_id,
        besttime_venue_id,
        timezone,
        open_time,
        close_time,
        is_scoreable,
        is_active,
        beaches!inner(location_id, address, website, description, parking_text, location_numb, created_at, is_active),
        beach_dog_policy(dogs_prohibited_start, dogs_prohibited_end)
      `)
      .eq("is_scoreable", true)
      .eq("is_active", true);
    if (targetLocationIds && targetLocationIds.length > 0) {
      // Caller pinned specific location_ids — filter via the joined beaches row.
      // Note: PostgREST doesn't support .in() on a foreign-table column directly,
      // so we post-filter in JS instead of complicating the query.
    }
    const { data: goldRows, error: beachErr } = await beachQuery;

    // Reshape gold rows into the existing Beach interface so the rest of
    // the function doesn't need touching. PostgREST returns the joined
    // beaches row as either an object or array depending on cardinality;
    // since we INNER JOIN there's exactly one.
    type GoldJoinedRow = {
      fid: number; name: string; display_name_override: string | null;
      lat: number; lon: number;
      noaa_station_id: string | null; besttime_venue_id: string | null;
      timezone: string; open_time: string | null; close_time: string | null;
      is_scoreable: boolean; is_active: boolean;
      beaches: { location_id: string; address: string | null; website: string | null;
                 description: string | null; parking_text: string | null;
                 location_numb: number | null; created_at: string;
                 is_active: boolean } | { location_id: string }[];
      beach_dog_policy: { dogs_prohibited_start: string | null; dogs_prohibited_end: string | null }
                        | null
                        | { dogs_prohibited_start: string | null; dogs_prohibited_end: string | null }[];
    };
    const flatten = (g: GoldJoinedRow): Beach => {
      const pb = Array.isArray(g.beaches) ? g.beaches[0] : g.beaches;
      const dp = Array.isArray(g.beach_dog_policy) ? g.beach_dog_policy[0]
               : g.beach_dog_policy;
      return {
        location_id:    (pb as { location_id: string }).location_id,
        arena_group_id: g.fid,
        display_name:   g.display_name_override ?? g.name,
        latitude:       g.lat,
        longitude:      g.lon,
        noaa_station_id: g.noaa_station_id,
        besttime_venue_id: g.besttime_venue_id,
        is_active:      g.is_active,
        timezone:       g.timezone ?? "America/Los_Angeles",
        open_time:      g.open_time,
        close_time:     g.close_time,
        dogs_prohibited_start: dp?.dogs_prohibited_start ?? null,
        dogs_prohibited_end:   dp?.dogs_prohibited_end   ?? null,
        address:        (pb as { address?: string }).address ?? null,
        website:        (pb as { website?: string }).website ?? null,
        description:    (pb as { description?: string }).description ?? null,
        parking_text:   (pb as { parking_text?: string }).parking_text ?? null,
        location_numb:  (pb as { location_numb?: number }).location_numb ?? null,
        created_at:     (pb as { created_at?: string }).created_at ?? "",
      };
    };
    let beaches: Beach[] | null = goldRows ? (goldRows as GoldJoinedRow[]).map(flatten) : null;
    if (beaches && targetLocationIds && targetLocationIds.length > 0) {
      const want = new Set(targetLocationIds);
      beaches = beaches.filter(b => want.has(b.location_id));
    }

    console.log("Beach query result — data:", beaches?.length ?? "null", "error:", beachErr?.message ?? "none");

    if (beachErr) throw new Error(`Failed to load beaches: ${beachErr.message}`);
    if (!beaches || beaches.length === 0) {
      return json({ ok: true, message: "No active beaches found", results: [] }, 200, cors);
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

    return json({ ok: true, runAt: runAt.toISOString(), results }, 200, cors);

  } catch (err) {
    console.error("Top-level error:", String(err));
    return json({ ok: false, error: String(err) }, 500, cors);
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
      // Write the discovered venue back to the spine (beaches_gold). The
      // legacy public.beaches column is now derived/optional and gets
      // mirrored on next sync rather than directly written here.
      await supabase
        .from("beaches_gold")
        .update({ besttime_venue_id: crowdResult.venueId })
        .eq("fid", beach.arena_group_id);
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

  const cautionMm = config.bacteria_caution_mm ?? 2.5;
  const nogoMm    = config.bacteria_nogo_mm    ?? 25.0;

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

  // g. Upsert hourly rows
  const dates = [...new Set(scoredHours.map((h) => h.localDate))].sort();
  try {
    const hourlyRows = scoredHours.map((h) =>
      buildHourlyRow(h, beach, config, h.hourText, runAt)
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

  // h. Upsert daily rows
  try {
    const dailyRows = dates.map((date) => {
      const dayHours     = scoredHours.filter((h) => h.localDate === date);
      const window       = windows.get(date) ?? null;
      const recentPrecip = computePrecipForDay(rawHours, weatherResult.hours, date);
      const bacteriaRisk = deriveBacteriaRisk(recentPrecip.precip72hMm, cautionMm, nogoMm);
      console.log(`[${beach.location_id}] ${date}: 72h=${recentPrecip.precip72hMm}mm → ${bacteriaRisk}`);
      return buildDailyRow(beach, date, dayHours, window, config, runAt, recentPrecip, bacteriaRisk);
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

// ─── Bacteria risk helpers ────────────────────────────────────────────────────

/**
 * Sum precipitation (mm) in the 24h and 72h windows ending at the start of
 * the given local day. For day 1 (today) the window is entirely observed
 * past-rain; for day 7 it's entirely forecast. Open-Meteo past_days=3 +
 * forecast_days=7 covers every window we ever need.
 *
 * Rationale: bacteria risk is a function of recent runoff accumulating at
 * the beach. Anchoring at start-of-day gives a clean, per-day rolling
 * total that matches how SoCal advisories are issued.
 */
function computePrecipForDay(
  rawHours: RawHourData[],
  weatherHours: Awaited<ReturnType<typeof fetchWeather>>["hours"],
  localDate: string,
): { precip24hMm: number; precip72hMm: number } {
  const firstIdx = rawHours.findIndex((h) => h.localDate === localDate);
  if (firstIdx < 0) return { precip24hMm: 0, precip72hMm: 0 };

  const anchorMs = new Date(rawHours[firstIdx].forecastTs).getTime();
  const ms24h    = 24 * 3_600_000;
  const ms72h    = 72 * 3_600_000;
  let precip24h  = 0;
  let precip72h  = 0;

  const len = Math.min(rawHours.length, weatherHours.length);
  for (let i = 0; i < len; i++) {
    const tsMs = new Date(rawHours[i].forecastTs).getTime();
    if (tsMs >= anchorMs) continue;
    const ageMs = anchorMs - tsMs;
    const mm    = weatherHours[i].precipitation ?? 0;
    if (ageMs <= ms72h) precip72h += mm;
    if (ageMs <= ms24h) precip24h += mm;
  }

  return {
    precip24hMm: Math.round(precip24h * 10) / 10,
    precip72hMm: Math.round(precip72h * 10) / 10,
  };
}

function deriveBacteriaRisk(
  precip72hMm: number,
  cautionMm: number,
  nogoMm: number,
): BacteriaRisk {
  if (precip72hMm >= nogoMm)    return "high";
  if (precip72hMm >= cautionMm) return "moderate";
  if (precip72hMm > 0)          return "low";
  return "none";
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
  const prohibStart  = beach.dogs_prohibited_start ? timeToMinutes(beach.dogs_prohibited_start) : null;
  const prohibEnd    = beach.dogs_prohibited_end   ? timeToMinutes(beach.dogs_prohibited_end)   : null;

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
    const isProhibited = prohibStart !== null && prohibEnd !== null &&
                         hourMinutes >= prohibStart && hourMinutes < prohibEnd;

    return {
      forecastTs:    toUtcIso(wh.time, beach.timezone),
      localDate,
      localHour,
      hourLabel:     buildHourLabel(localHour),
      isDaylight:    wh.is_day === 1,
      weatherCode:   wh.weathercode,
      tempAir:       wh.temperature_2m,
      feelsLike:     wh.apparent_temperature,
      windSpeed:     wh.windspeed_10m,
      precipChance:  wh.precipitation_probability,
      uvIndex:       wh.uv_index,
      tideHeight,
      busynessScore: busyness,
      isBeachOpen,
      isProhibited,
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
    arena_group_id:        beach.arena_group_id ?? null,
    local_date:            h.localDate,
    forecast_ts:           h.forecastTs,
    local_hour:            h.localHour,
    hour_label:            h.hourLabel,
    is_daylight:           h.isDaylight,
    is_candidate_window:   h.isCandidateWindow,
    is_in_best_window:     h.isInBestWindow,
    weather_code:          h.weatherCode,
    temp_air:              h.tempAir,
    feels_like:            h.feelsLike,
    wind_speed:            h.windSpeed,
    precip_chance:         h.precipChance,
    uv_index:              h.uvIndex,
    tide_height:           h.tideHeight,
    busyness_score:        h.busynessScore,
    busyness_category:     h.busynessCategory,
    hour_status:           h.hourStatus,
    hour_score:            h.hourScore,
    positive_reason_codes: h.positiveReasonCodes,
    risk_reason_codes:     h.riskReasonCodes,
    explainability:        h.explainability,
    tide_score:            h.explainability.tide_score    ?? null,
    wind_score:            h.explainability.wind_score    ?? null,
    crowd_score:           h.explainability.crowd_score   ?? null,
    rain_score:            h.explainability.rain_score    ?? null,
    temp_score:            h.explainability.temp_score    ?? null,
    uv_score:              h.explainability.uv_score      ?? null,
    weather_score:         h.explainability.weather_score ?? null,
    tide_status:           h.metricStatuses.tide_status     ?? null,
    wind_status:           h.metricStatuses.wind_status     ?? null,
    crowd_status:          h.metricStatuses.crowd_status    ?? null,
    rain_status:           h.metricStatuses.rain_status     ?? null,
    temp_status:           h.metricStatuses.temp_status     ?? null,
    temp_cold_status:      h.metricStatuses.temp_cold_status ?? null,
    temp_hot_status:       h.metricStatuses.temp_hot_status  ?? null,
    uv_status:             h.metricStatuses.uv_status       ?? null,
    sand_temp:             h.sandTemp,
    asphalt_temp:          h.asphaltTemp,
    sand_status:           h.metricStatuses.sand_status     ?? null,
    asphalt_status:        h.metricStatuses.asphalt_status  ?? null,
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
  config: ScoringConfig,
  runAt: Date,
  recentPrecip: { precip24hMm: number; precip72hMm: number },
  bacteriaRisk: BacteriaRisk,
) {
  const goHours       = dayHours.filter((h) => h.hourStatus === "go");
  const advisoryHours = dayHours.filter((h) => h.hourStatus === "advisory");
  const cautionHours  = dayHours.filter((h) => h.hourStatus === "caution");
  const noGoHours     = dayHours.filter((h) => h.hourStatus === "no_go");

  // day_status = best achievable status (go > advisory > caution > no_go)
  // Bacteria risk forces day_status up to at least caution
  const weatherStatus: DayStatus =
    goHours.length > 0        ? "go"
    : advisoryHours.length > 0 ? "advisory"
    : cautionHours.length > 0  ? "caution"
    : "no_go";
  const dayStatus: DayStatus =
    (bacteriaRisk === "moderate" || bacteriaRisk === "high") &&
    (weatherStatus === "go" || weatherStatus === "advisory")
      ? "caution"
      : weatherStatus;

  const aggHours    = window?.hours ?? dayHours.filter((h) => h.isDaylight);
  const avgTemp       = average(aggHours.map((h) => h.tempAir).filter(nonNull));
  const avgFeelsLike  = average(aggHours.map((h) => h.feelsLike).filter(nonNull));
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
  if (bacteriaRisk === "none")                                  positiveSet.add("clean_water");
  if (bacteriaRisk === "moderate" || bacteriaRisk === "high")   riskSet.add("bacteria_risk");

  return {
    location_id:           beach.location_id,
    arena_group_id:        beach.arena_group_id ?? null,
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
    advisory_hours_count:  advisoryHours.length,
    caution_hours_count:   cautionHours.length,
    no_go_hours_count:     noGoHours.length,
    avg_feels_like:        round1(avgFeelsLike),
    positive_reason_codes: [...positiveSet],
    risk_reason_codes:     [...riskSet],
    day_text:              null,
    caution_text:          null,
    best_window_text:      null,
    no_go_text:            dayStatus === "no_go" ? buildNoGoText([...riskSet]) : null,
    hourly_source_max_ts:  maxTs(dayHours.map((h) => h.forecastTs)),
    daily_source_date:     date,
    timezone:              beach.timezone,
    scoring_version:       config.scoring_version,
    generated_at:          runAt.toISOString(),
    precip_24h_mm:         recentPrecip.precip24hMm,
    precip_72h_mm:         recentPrecip.precip72hMm,
    bacteria_risk:         bacteriaRisk,
  };
}

// ─── Rule-based no_go text ────────────────────────────────────────────────────

function buildNoGoText(riskCodes: string[]): string {
  if (riskCodes.includes("severe_weather"))   return "Severe weather makes today unsafe for a beach visit.";
  if (riskCodes.includes("dangerous_wind"))   return "Dangerous wind speeds make today a no-go.";
  if (riskCodes.includes("bacteria_risk"))    return "Recent rainfall has elevated bacteria risk — not a good day to visit.";
  if (riskCodes.includes("extreme_temp"))     return "Extreme temperatures make today unsafe for dogs.";
  if (riskCodes.includes("extreme_uv"))       return "Extreme UV index makes today a no-go.";
  if (riskCodes.includes("hot_sand"))         return "Sand temperatures are dangerously hot for paws today.";
  return "Conditions today make for a poor beach experience — try another day.";
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

function json(body: unknown, status = 200, cors?: Record<string, string>): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: cors ?? { "Content-Type": "application/json" },
  });
}
