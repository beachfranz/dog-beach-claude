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
  // skip beaches whose is_now row was updated within the cutoff. Fractional
  // hours OK (e.g. 0.9 = 54 min — pairs well with hourly pg_cron + a 6 min
  // grace buffer). Capped at 24h.
  let skipRecentHours: number | null = null;

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
    if (typeof body?.skip_recent_hours === "number" && body.skip_recent_hours > 0) {
      skipRecentHours = Math.min(body.skip_recent_hours, 24);
    }
  }

  // ── Resolve all input keys to a single set of arena_group_ids ───────────────
  // Path 3b-3.x: beaches_gold is the spine; the legacy location_id slug
  // lives on it now too. Any location_id input maps to arena_group_id
  // via beaches_gold.
  const fidSet = new Set<number>(arenaGroupIds ?? []);
  if (locationIds?.length) {
    const { data: rows } = await supabase
      .from("beaches_gold")
      .select("fid")
      .in("location_id", locationIds);
    for (const r of rows ?? []) {
      if (r.fid) fidSet.add(r.fid);
    }
  }

  let beachQuery = supabase.from("beaches_gold")
    .select(`
      fid,
      location_id,
      name,
      display_name_override,
      lat,
      lon,
      noaa_station_id,
      besttime_venue_id,
      timezone,
      open_time,
      close_time,
      is_active,
      address,
      website,
      description,
      parking_text,
      beach_dog_policy(dogs_prohibited_start, dogs_prohibited_end)
    `)
    .eq("is_active", true);
  if (fidSet.size > 0) {
    beachQuery = beachQuery.in("fid", [...fidSet]);
  }
  // If neither key was provided, fall through to "all active scoreable
  // beaches" — same semantic as the hourly cron call (POST {} → batch).
  // 2026-05-13: scoring_tier replaces is_scoreable. Hourly tier only
  // — daily tier doesn't need NOW refreshes (rolled-up by daily-beach-refresh).
  if (!locationIds?.length && !arenaGroupIds?.length) {
    beachQuery = beachQuery.eq("scoring_tier", "hourly");
  }

  const [goldRes, configRes] = await Promise.all([
    beachQuery,
    supabase.from("scoring_config")
      .select("*")
      .eq("is_active", true)
      .order("effective_from", { ascending: false })
      .limit(1)
      .single(),
  ]);

  if (goldRes.error || !goldRes.data?.length) return json({ error: "No beaches found" }, 404);
  if (configRes.error || !configRes.data)     return json({ error: "Scoring config not found" }, 500);

  // Reshape gold rows into the existing beach shape so refreshNow doesn't change.
  type GoldRow = {
    fid: number; location_id: string | null;
    name: string; display_name_override: string | null;
    lat: number; lon: number;
    noaa_station_id: string | null; besttime_venue_id: string | null;
    timezone: string; open_time: string | null; close_time: string | null;
    is_active: boolean;
    address: string | null; website: string | null;
    description: string | null; parking_text: string | null;
    beach_dog_policy: { dogs_prohibited_start: string | null; dogs_prohibited_end: string | null }
                      | null
                      | { dogs_prohibited_start: string | null; dogs_prohibited_end: string | null }[];
  };
  let beaches = (goldRes.data as GoldRow[]).map(g => {
    const dp = Array.isArray(g.beach_dog_policy) ? g.beach_dog_policy[0] : g.beach_dog_policy;
    return {
      location_id:    g.location_id,
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
      address:        g.address,
      website:        g.website,
      description:    g.description,
      parking_text:   g.parking_text,
    };
  });

  // ── Optional skip_recent_hours filter ────────────────────────────────────
  // Drop beaches whose is_now row was updated within the cutoff. Mirrors
  // the same flag on daily-beach-refresh; idempotent for cron retries and
  // ad-hoc fires. For hourly cron pass ~0.9 (= 54min, gives 6min slack
  // before the next firing).
  let skippedRecent = 0;
  if (skipRecentHours !== null && beaches.length > 0) {
    const cutoffMs = runAt.getTime() - skipRecentHours * 3600 * 1000;
    const cutoffIso = new Date(cutoffMs).toISOString();
    const beachFids = beaches.map(b => b.arena_group_id);
    const { data: recentRows, error: recentErr } = await supabase
      .from("beach_day_hourly_scores")
      .select("arena_group_id, updated_at")
      .eq("is_now", true)
      .in("arena_group_id", beachFids)
      .gte("updated_at", cutoffIso);
    if (recentErr) {
      console.warn(`skip_recent_hours query failed: ${recentErr.message} — proceeding without skip`);
    } else {
      const recentSet = new Set((recentRows ?? []).map((r: { arena_group_id: number }) => r.arena_group_id));
      const before = beaches.length;
      beaches = beaches.filter(b => !recentSet.has(b.arena_group_id));
      skippedRecent = before - beaches.length;
      console.log(`skip_recent_hours=${skipRecentHours} → skipped ${skippedRecent}/${before}; ${beaches.length} remaining`);
    }
  }

  const config = configRes.data;

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
    skipped_recent: skippedRecent,
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
    // W2.3 cutover: weather reads from weather_grid_hourly (current hour
    // row) via weather_for_point RPC instead of live Open-Meteo. Falls
    // back to direct fetch if grid cell is unloaded. Per [[weather-grid-reference-layer]].
    const [weather, tide, crowdRow] = await Promise.all([
      fetchCurrentWeatherFromGrid(beach.latitude, beach.longitude, beach.timezone, supabase)
        .catch(() => null)
        .then(w => w ?? fetchCurrentWeather(beach.latitude, beach.longitude, beach.timezone)),
      fetchCurrentTide(beach.noaa_station_id, localHour),
      supabase.from("beach_day_hourly_scores")
        .select("busyness_score, busyness_category")
        .eq("arena_group_id", beach.arena_group_id)
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
      cloudCover:    weather.cloud_cover ?? null,
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
      .eq("arena_group_id", beach.arena_group_id)
      .eq("is_now", true);

    const { error: upsertErr } = await supabase
      .from("beach_day_hourly_scores")
      .upsert(row, { onConflict: "arena_group_id,forecast_ts" });

    if (upsertErr) throw new Error(upsertErr.message);

    // Re-apply v2 best window so the recommendation row stays pegged to the
    // freshly-updated hourly_scores. Without this, the stored
    // best_window_label drifts every hour as NOW overwrites individual hours
    // — Scout blurb (reads stored value) and chart headline (reads live RPC)
    // disagree. Soft-fail: never block the NOW write on the rec update.
    // Per Franz 2026-05-30.
    let hourScoreV2: number | null = null;
    let hourStatusV2: string | null = null;
    if (beach.arena_group_id != null) {
      const { error: bwErr } = await supabase.rpc(
        "apply_v2_best_window_to_beach_recommendations",
        { p_fid: beach.arena_group_id, p_date: localDate },
      );
      if (bwErr) console.warn(`[${beach.location_id}] apply_v2 window soft-fail:`, bwErr.message);

      // Read back the v2 fields that apply_v2 just populated, plus derive
      // hour_status_v2 inline via the v2_compute_hour_status helper. find.html
      // NOW mode (nowToDay) prefers these over v1 hour_score / hour_status
      // when present. Per Franz 2026-05-30 task #5.
      const { data: v2row } = await supabase
        .from("beach_day_hourly_scores")
        .select("hour_score_v2")
        .eq("arena_group_id", beach.arena_group_id)
        .eq("forecast_ts", row.forecast_ts)
        .maybeSingle();
      hourScoreV2 = (v2row?.hour_score_v2 as number | null) ?? null;

      const { data: stRow } = await supabase.rpc("v2_compute_hour_status", {
        p_uv:         row.uv_index ?? null,
        p_asphalt:    row.asphalt_temp ?? null,
        p_sand:       row.sand_temp ?? null,
        p_tide:       row.tide_height ?? null,
        p_wind:       row.wind_speed ?? null,
        p_crowd:      row.busyness_score ?? null,
        p_precip:     row.precip_chance ?? null,
        p_feels_like: row.feels_like ?? null,
        p_is_closed:  false,
      });
      hourStatusV2 = (stRow as unknown as string | null) ?? null;
    }

    // Return row with tide direction + v2 fields for frontend display
    return {
      locationId: beach.location_id,
      ok:         true,
      row:        {
        ...row,
        tide_direction: tide.direction,
        hour_score_v2:  hourScoreV2,
        hour_status_v2: hourStatusV2,
      },
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
    // hour_status + per-metric *_status columns retired per Franz
    // 2026-05-30 v1-retirement task #11. apply_v2 derives v2 statuses
    // inline. Columns dropped by task #12.
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
    sand_temp:             h.sandTemp,
    asphalt_temp:          h.asphaltTemp,
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
  cloud_cover:         number;
  precip_chance:       number;
  is_day:              boolean;
}

// W2.3 — read current-hour weather from weather_grid_hourly via RPC.
// Returns null if the grid cell isn't loaded (caller falls back to direct fetch).
async function fetchCurrentWeatherFromGrid(
  lat: number,
  lng: number,
  _timezone: string,
  supabase: ReturnType<typeof createClient>,
): Promise<CurrentWeather | null> {
  // Window: current hour ± 1h, take the closest to now()
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

  // Pick row closest to now()
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

async function fetchCurrentWeather(
  lat: number,
  lng: number,
  timezone: string,
): Promise<CurrentWeather> {
  const params = new URLSearchParams({
    latitude:           String(lat),
    longitude:          String(lng),
    current:            "temperature_2m,apparent_temperature,wind_speed_10m,weather_code,uv_index,cloud_cover,is_day",
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
    cloud_cover:          cur.cloud_cover ?? 0,
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
