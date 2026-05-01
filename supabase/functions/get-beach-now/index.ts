// get-beach-now/index.ts
// Fetches actual current-hour conditions, runs them through the full
// scoring engine, and writes the result to beach_day_hourly_scores
// with is_now = true — overwriting the forecast row for that hour.
//
// GET  ?location_id=X              → refresh single beach, return its NOW row
// POST { location_ids?: string[] } → refresh all (or listed) beaches (used by cron)

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";
import {
  scoreHours,
  buildHourLabel,
  type RawHourData,
  type ScoredHour,
} from "../_shared/scoring.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "GET, POST, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  const runAt    = new Date();

  // ── Which beaches to refresh ─────────────────────────────────────────────────
  // Accepts either location_id (text slug, legacy) or arena_group_id (bigint,
  // new spine). Path 3b dual-input — both work; 3c will drop location_id.
  let locationIds:    string[] | null = null;
  let arenaGroupIds:  number[] | null = null;

  if (req.method === "GET") {
    const params = new URL(req.url).searchParams;
    const loc = params.get("location_id");
    const fid = params.get("arena_group_id") ?? params.get("fid");
    if (loc) locationIds = [loc];
    if (fid) arenaGroupIds = [parseInt(fid, 10)].filter(Number.isFinite);
  } else if (req.method === "POST") {
    const body = await req.json().catch(() => ({}));
    if (Array.isArray(body?.location_ids) && body.location_ids.length > 0) {
      locationIds = body.location_ids;
    }
    if (Array.isArray(body?.arena_group_ids) && body.arena_group_ids.length > 0) {
      arenaGroupIds = body.arena_group_ids
        .map((x: unknown) => typeof x === "number" ? x : parseInt(String(x), 10))
        .filter(Number.isFinite);
    }
  }

  // ── Load beaches + scoring config in parallel ────────────────────────────────
  let beachQuery = supabase.from("beaches").select("*").eq("is_active", true);
  if (arenaGroupIds?.length && locationIds?.length) {
    // Both keys provided — OR them. Rare; keeps the BC layer permissive.
    beachQuery = beachQuery.or(
      `location_id.in.(${locationIds.join(",")}),arena_group_id.in.(${arenaGroupIds.join(",")})`
    );
  } else if (arenaGroupIds?.length) {
    beachQuery = beachQuery.in("arena_group_id", arenaGroupIds);
  } else if (locationIds?.length) {
    beachQuery = beachQuery.in("location_id", locationIds);
  }

  const [beachRes, configRes] = await Promise.all([
    beachQuery,
    supabase.from("scoring_config")
      .select("*")
      .eq("is_active", true)
      .order("effective_from", { ascending: false })
      .limit(1)
      .single(),
  ]);

  if (beachRes.error || !beachRes.data?.length) return json({ error: "No beaches found" }, 404);
  if (configRes.error || !configRes.data)        return json({ error: "Scoring config not found" }, 500);

  const beaches = beachRes.data;
  const config  = configRes.data;

  // ── Process each beach ───────────────────────────────────────────────────────
  const results = await Promise.all(
    beaches.map(beach => refreshNow(beach, config, supabase, runAt))
  );

  // Single-beach GET: return the NOW row directly (frontend call)
  if (req.method === "GET" && results.length === 1) {
    const r = results[0];
    if (!r.ok) return json({ error: r.error }, 500);
    return json(r.row);
  }

  // Batch POST: return summary (cron call)
  return json({
    ok:      true,
    runAt:   runAt.toISOString(),
    results: results.map(r => ({ locationId: r.locationId, ok: r.ok, error: r.error })),
  });
});

// ─── Per-beach NOW refresh ────────────────────────────────────────────────────

interface Beach {
  location_id: string;
  display_name: string;
  latitude: number;
  longitude: number;
  noaa_station_id: string | null;
  timezone: string;
  open_time: string | null;
  close_time: string | null;
  dogs_prohibited_start: string | null;
  dogs_prohibited_end:   string | null;
}

async function refreshNow(
  beach: Beach,
  config: Record<string, unknown>,
  supabase: ReturnType<typeof createClient>,
  runAt: Date,
): Promise<{ locationId: string; ok: boolean; row?: unknown; error?: string }> {
  try {
    // ── Current local date + hour for this beach ────────────────────────────
    const localParts = new Intl.DateTimeFormat("en-US", {
      timeZone: beach.timezone,
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", hour12: false,
    }).formatToParts(runAt);
    const get = (t: string) => localParts.find(p => p.type === t)?.value ?? "";
    const localDate = `${get("year")}-${get("month")}-${get("day")}`;
    const localHour = parseInt(get("hour")) % 24;

    // ── Fetch weather + tide + crowd in parallel ────────────────────────────
    const [weather, tide, crowdRow] = await Promise.all([
      fetchCurrentWeather(beach.latitude, beach.longitude, beach.timezone),
      fetchCurrentTide(beach.noaa_station_id, localHour),
      supabase.from("beach_day_hourly_scores")
        .select("busyness_score, busyness_category")
        .eq("location_id", beach.location_id)
        .eq("local_date", localDate)
        .eq("local_hour", localHour)
        .maybeSingle(),
    ]);

    // ── Build RawHourData ───────────────────────────────────────────────────
    const openMinutes  = timeToMinutes(beach.open_time  ?? "00:00");
    const closeMinutes = timeToMinutes(beach.close_time ?? "23:59");
    const isBeachOpen  = (localHour * 60) >= openMinutes && (localHour * 60) < closeMinutes;
    const prohibStart  = beach.dogs_prohibited_start ? timeToMinutes(beach.dogs_prohibited_start) : null;
    const prohibEnd    = beach.dogs_prohibited_end   ? timeToMinutes(beach.dogs_prohibited_end)   : null;
    const isProhibited = prohibStart !== null && prohibEnd !== null &&
                         (localHour * 60) >= prohibStart && (localHour * 60) < prohibEnd;

    const rawHour: RawHourData = {
      forecastTs:    localToUtcIso(localDate, localHour, beach.timezone),
      localDate,
      localHour,
      hourLabel:     buildHourLabel(localHour),
      isDaylight:    weather.is_day,
      weatherCode:   weather.weather_code,
      tempAir:       weather.temperature_2m,
      feelsLike:     weather.apparent_temperature,
      windSpeed:     weather.wind_speed_10m,
      precipChance:  weather.precip_chance,
      uvIndex:       weather.uv_index,
      tideHeight:    tide.height,
      busynessScore: crowdRow.data?.busyness_score ?? null,
      isBeachOpen,
      isProhibited,
    };

    // ── Score through shared engine ─────────────────────────────────────────
    const [scored] = scoreHours([rawHour], config as Parameters<typeof scoreHours>[1]);

    // ── Build DB row ────────────────────────────────────────────────────────
    const row = buildNowRow(scored, beach, config as { scoring_version: string }, runAt);

    // ── Clear old is_now flag for this beach, then upsert ──────────────────
    await supabase
      .from("beach_day_hourly_scores")
      .update({ is_now: false })
      .eq("location_id", beach.location_id)
      .eq("is_now", true);

    const { error: upsertErr } = await supabase
      .from("beach_day_hourly_scores")
      .upsert(row, { onConflict: "location_id,forecast_ts" });

    if (upsertErr) throw new Error(upsertErr.message);

    // Return row with tide direction for frontend display
    return {
      locationId: beach.location_id,
      ok:         true,
      row:        { ...row, tide_direction: tide.direction },
    };

  } catch (err) {
    console.error(`[${beach.location_id}] NOW refresh error:`, String(err));
    return { locationId: beach.location_id, ok: false, error: String(err) };
  }
}

// ─── Row builder ─────────────────────────────────────────────────────────────

function buildNowRow(
  h: ScoredHour,
  beach: { location_id: string; timezone: string; arena_group_id?: number | null },
  config: { scoring_version: string },
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
    is_in_best_window:     false,
    is_now:                true,
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
    tide_status:           h.metricStatuses.tide_status      ?? null,
    wind_status:           h.metricStatuses.wind_status      ?? null,
    crowd_status:          h.metricStatuses.crowd_status     ?? null,
    rain_status:           h.metricStatuses.rain_status      ?? null,
    temp_status:           h.metricStatuses.temp_status      ?? null,
    temp_cold_status:      h.metricStatuses.temp_cold_status ?? null,
    temp_hot_status:       h.metricStatuses.temp_hot_status  ?? null,
    uv_status:             h.metricStatuses.uv_status        ?? null,
    sand_temp:             h.sandTemp,
    asphalt_temp:          h.asphaltTemp,
    sand_status:           h.metricStatuses.sand_status      ?? null,
    asphalt_status:        h.metricStatuses.asphalt_status   ?? null,
    hour_text:             h.hourText,
    timezone:              beach.timezone,
    scoring_version:       config.scoring_version,
    generated_at:          runAt.toISOString(),
  };
}

// ─── Weather fetch ────────────────────────────────────────────────────────────

interface CurrentWeather {
  temperature_2m:      number;
  apparent_temperature: number;
  wind_speed_10m:      number;
  weather_code:        number;
  uv_index:            number;
  precip_chance:       number;
  is_day:              boolean;
}

async function fetchCurrentWeather(
  lat: number,
  lng: number,
  timezone: string,
): Promise<CurrentWeather> {
  const params = new URLSearchParams({
    latitude:           String(lat),
    longitude:          String(lng),
    current:            "temperature_2m,apparent_temperature,wind_speed_10m,weather_code,uv_index,is_day",
    hourly:             "precipitation_probability",
    forecast_days:      "1",
    temperature_unit:   "fahrenheit",
    windspeed_unit:     "mph",
    timezone,
  });

  const res = await fetch(`https://api.open-meteo.com/v1/forecast?${params}`);
  if (!res.ok) throw new Error(`Open-Meteo error ${res.status}`);
  const data = await res.json();
  const cur  = data.current;

  // precipitation_probability only available in hourly; take current hour's value
  const nowHour  = new Date().getHours();
  const precipArr = data.hourly?.precipitation_probability ?? [];
  const precip    = precipArr[nowHour] ?? 0;

  return {
    temperature_2m:       cur.temperature_2m,
    apparent_temperature: cur.apparent_temperature ?? cur.temperature_2m,
    wind_speed_10m:       cur.wind_speed_10m,
    weather_code:         cur.weather_code,
    uv_index:             cur.uv_index ?? 0,
    precip_chance:        precip,
    is_day:               cur.is_day === 1,
  };
}

// ─── Tide fetch ───────────────────────────────────────────────────────────────

interface CurrentTide {
  height:    number | null;
  direction: "rising" | "falling" | "steady";
}

async function fetchCurrentTide(
  stationId: string | null,
  localHour: number,
): Promise<CurrentTide> {
  if (!stationId) return { height: null, direction: "steady" };

  const now   = new Date();
  const today = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, "0")}${String(now.getDate()).padStart(2, "0")}`;

  const params = new URLSearchParams({
    station:    stationId,
    product:    "predictions",
    datum:      "MLLW",
    units:      "english",
    time_zone:  "lst_ldt",
    interval:   "h",
    format:     "json",
    begin_date: today,
    end_date:   today,
  });

  try {
    const res  = await fetch(`https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?${params}`);
    if (!res.ok) return { height: null, direction: "steady" };
    const data = await res.json();
    if (!Array.isArray(data.predictions)) return { height: null, direction: "steady" };

    const tides     = data.predictions as Array<{ t: string; v: string }>;
    const curEntry  = tides.find(p => parseInt(p.t.slice(11, 13)) === localHour);
    const nextEntry = tides.find(p => parseInt(p.t.slice(11, 13)) === (localHour + 1) % 24);

    const height = curEntry  ? parseFloat(curEntry.v)  : null;
    const next   = nextEntry ? parseFloat(nextEntry.v) : null;

    let direction: "rising" | "falling" | "steady" = "steady";
    if (height !== null && next !== null) {
      if (next - height > 0.1)      direction = "rising";
      else if (height - next > 0.1) direction = "falling";
    }

    return { height, direction };
  } catch {
    return { height: null, direction: "steady" };
  }
}

// ─── Utilities ────────────────────────────────────────────────────────────────

function timeToMinutes(time: string): number {
  const [h, m] = time.split(":").map(Number);
  return h * 60 + (m ?? 0);
}

function localToUtcIso(localDate: string, localHour: number, timezone: string): string {
  const [year, month, day] = localDate.split("-").map(Number);
  const utcGuess  = new Date(Date.UTC(year, month - 1, day, localHour, 0, 0));
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", hour12: false,
  });
  const parts     = Object.fromEntries(
    formatter.formatToParts(utcGuess).map(p => [p.type, p.value])
  );
  const localFromUtc = new Date(Date.UTC(
    Number(parts.year), Number(parts.month) - 1, Number(parts.day),
    Number(parts.hour), Number(parts.minute),
  ));
  const offsetMs = localFromUtc.getTime() - utcGuess.getTime();
  return new Date(utcGuess.getTime() - offsetMs).toISOString();
}
