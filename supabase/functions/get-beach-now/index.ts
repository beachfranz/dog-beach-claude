// get-beach-now/index.ts
// Returns current real-time conditions for a beach.
// Fetches live weather from Open-Meteo + tide from NOAA.
// Borrows crowd score for the current hour from beach_day_hourly_scores
// (BestTime forecasts don't change intra-day, so the pipeline value is valid).
// Computes a full 6-metric composite score identical to the day cards.
//
// GET ?location_id=huntington-dog-beach
// Returns: { location_id, display_name, as_of, is_day, temp, wind_speed,
//            weather_code, summary_weather, uv_index, precip_chance,
//            tide_height, tide_direction, hour_score, busyness_category }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "GET, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });

  const url        = new URL(req.url);
  const locationId = url.searchParams.get("location_id") ?? "huntington-dog-beach";

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // ── Load beach + scoring config in parallel ───────────────────────────────
  const [beachRes, configRes] = await Promise.all([
    supabase.from("beaches")
      .select("location_id, display_name, latitude, longitude, timezone, noaa_station_id")
      .eq("location_id", locationId)
      .single(),
    supabase.from("scoring_config")
      .select("*")
      .eq("is_active", true)
      .order("effective_from", { ascending: false })
      .limit(1)
      .single(),
  ]);

  if (beachRes.error || !beachRes.data) return json({ error: "Beach not found" }, 404);
  if (configRes.error || !configRes.data) return json({ error: "Scoring config not found" }, 500);

  const beach = beachRes.data;
  const cfg   = configRes.data;

  // ── Current date/hour in beach timezone ──────────────────────────────────
  const now       = new Date();
  const localParts = new Intl.DateTimeFormat("en-US", {
    timeZone: beach.timezone,
    year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit",
    hour12: false,
  }).formatToParts(now);
  const get = (t: string) => localParts.find(p => p.type === t)?.value ?? "";
  const localDate = `${get("year")}-${get("month")}-${get("day")}`;
  const localHour = parseInt(get("hour")) % 24;

  // ── Fetch weather, tide, and crowd score in parallel ──────────────────────
  const [weather, tide, hourRow] = await Promise.all([
    fetchCurrentWeather(beach.latitude, beach.longitude, beach.timezone),
    fetchCurrentTide(beach.noaa_station_id),
    supabase.from("beach_day_hourly_scores")
      .select("busyness_score, busyness_category, hour_status")
      .eq("location_id", locationId)
      .eq("local_date", localDate)
      .eq("local_hour", localHour)
      .maybeSingle(),
  ]);

  const busynessScore    = hourRow.data?.busyness_score    ?? null;
  const busynessCategory = hourRow.data?.busyness_category ?? null;

  // ── Compute full 6-metric score ───────────────────────────────────────────
  const score = computeScore(weather, tide, busynessScore, cfg);

  return json({
    location_id:       beach.location_id,
    display_name:      beach.display_name,
    as_of:             now.toISOString(),
    is_day:            weather.is_day,
    temp:              weather.temp,
    wind_speed:        weather.wind_speed,
    weather_code:      weather.weather_code,
    summary_weather:   wmoToSummaryWeather(weather.weather_code, weather.wind_speed),
    uv_index:          weather.uv_index,
    precip_chance:     weather.precip_chance,
    tide_height:       tide.height,
    tide_direction:    tide.direction,
    hour_score:        score,
    busyness_category: busynessCategory,
  });
});

// ─── Weather fetch ────────────────────────────────────────────────────────────

interface CurrentWeather {
  temp: number;
  wind_speed: number;
  weather_code: number;
  uv_index: number;
  precip_chance: number;
  is_day: boolean;
}

async function fetchCurrentWeather(
  lat: number,
  lng: number,
  timezone: string,
): Promise<CurrentWeather> {
  const params = new URLSearchParams({
    latitude:         String(lat),
    longitude:        String(lng),
    current:          "temperature_2m,wind_speed_10m,weather_code,uv_index,is_day",
    hourly:           "precipitation_probability",
    forecast_days:    "1",
    temperature_unit: "fahrenheit",
    windspeed_unit:   "mph",
    timezone,
  });

  const res  = await fetch(`https://api.open-meteo.com/v1/forecast?${params}`);
  if (!res.ok) throw new Error(`Open-Meteo error ${res.status}`);
  const data = await res.json();

  const cur = data.current;

  // Current hour's precipitation probability from hourly forecast
  const now       = new Date();
  const currentH  = now.getHours();
  const precipArr = data.hourly?.precipitation_probability ?? [];
  const precip    = precipArr[currentH] ?? 0;

  return {
    temp:          cur.temperature_2m,
    wind_speed:    cur.wind_speed_10m,
    weather_code:  cur.weather_code,
    uv_index:      cur.uv_index ?? 0,
    precip_chance: precip,
    is_day:        cur.is_day === 1,
  };
}

// ─── Tide fetch ───────────────────────────────────────────────────────────────

interface CurrentTide {
  height: number | null;
  direction: "rising" | "falling" | "steady";
}

async function fetchCurrentTide(stationId: string | null): Promise<CurrentTide> {
  if (!stationId) return { height: null, direction: "steady" };

  const now   = new Date();
  const today = formatNoaaDate(now);

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

  const res = await fetch(`https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?${params}`);
  if (!res.ok) return { height: null, direction: "steady" };

  const data = await res.json();
  if (!Array.isArray(data.predictions)) return { height: null, direction: "steady" };

  const nowHour   = now.getHours();
  const tides     = data.predictions as Array<{ t: string; v: string }>;
  const curEntry  = tides.find(p => parseInt(p.t.slice(11, 13)) === nowHour);
  const nextEntry = tides.find(p => parseInt(p.t.slice(11, 13)) === (nowHour + 1) % 24);

  const height = curEntry  ? parseFloat(curEntry.v)  : null;
  const next   = nextEntry ? parseFloat(nextEntry.v) : null;

  let direction: "rising" | "falling" | "steady" = "steady";
  if (height !== null && next !== null) {
    if (next - height > 0.1)      direction = "rising";
    else if (height - next > 0.1) direction = "falling";
  }

  return { height, direction };
}

// ─── Scoring ──────────────────────────────────────────────────────────────────

function computeScore(
  weather: CurrentWeather,
  tide: CurrentTide,
  busynessScore: number | null,
  cfg: Record<string, number>,
): number | null {
  if (!weather.is_day) return null;

  const tideScore  = tide.height !== null
    ? clamp(1 - tide.height / cfg.norm_tide_max) : 0.5;
  const rainScore  = clamp(1 - weather.precip_chance / 100);
  const windScore  = clamp(1 - weather.wind_speed / cfg.norm_wind_max);
  const crowdScore = busynessScore !== null
    ? clamp(1 - busynessScore / 100) : 0.5;
  const tempScore  = clamp(1 - Math.abs(weather.temp - cfg.norm_temp_target) / cfg.norm_temp_range);
  const uvScore    = clamp(1 - weather.uv_index / cfg.norm_uv_max);

  const score =
    tideScore  * (cfg.weight_tide  ?? 0.30) +
    rainScore  * (cfg.weight_rain  ?? 0.25) +
    windScore  * (cfg.weight_wind  ?? 0.20) +
    crowdScore * (cfg.weight_crowd ?? 0.15) +
    tempScore  * (cfg.weight_temp  ?? 0.05) +
    uvScore    * (cfg.weight_uv    ?? 0.05);

  return Math.round(score * 100);
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function clamp(val: number): number {
  return Math.max(0, Math.min(1, val));
}

function formatNoaaDate(date: Date): string {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}${m}${d}`;
}

function wmoToSummaryWeather(code: number, windSpeed: number): string {
  if (windSpeed >= 20) return "windy";
  if (code === 0)      return "sunny";
  if (code <= 2)       return "partly_cloudy";
  if (code === 3)      return "cloudy";
  if (code >= 45 && code <= 48) return "foggy";
  if (code >= 51)      return "rainy";
  return "partly_cloudy";
}
