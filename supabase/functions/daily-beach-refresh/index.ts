// daily-beach-refresh/index.ts
// Supabase Edge Function — orchestrates the full daily data pipeline.

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders }  from "../_shared/cors.ts";
import { ensureNotTruncated } from "../_shared/safeSelect.ts";
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
  let tideWindowDays = 7;       // default: refresh fetches up to 7 days
  let forceTideRefresh = false; // default: skip NOAA when buffer is fresh
  let skipRecentHours: number | null = null;  // skip beaches refreshed within N hours
  let limitBeaches: number | null = null;     // cap beaches processed per call
  // BestTime soft-removed 2026-05-25: human-presence signal clashes with
  // dog-centric brand, 95% null coverage was inconsistent, paid API with
  // marginal value. Crowd scoring falls back to 0.5 neutral via scoring.ts.
  // Override with body { skip_besttime: false } if ever needed (emergency).
  let skipBesttime = true;
  try {
    const body = await req.json().catch(() => ({}));
    if (Array.isArray(body?.location_ids) && body.location_ids.length > 0) {
      targetLocationIds = body.location_ids;
    }
    if (typeof body?.tide_window_days === "number" && body.tide_window_days >= 7) {
      tideWindowDays = Math.min(body.tide_window_days, 30);
    }
    if (body?.force_tide_refresh === true) forceTideRefresh = true;
    if (typeof body?.skip_besttime === "boolean") skipBesttime = body.skip_besttime;
    if (typeof body?.skip_recent_hours === "number" && body.skip_recent_hours > 0) {
      skipRecentHours = Math.min(body.skip_recent_hours, 168);  // cap at 1 week
    }
    if (typeof body?.limit === "number" && body.limit > 0) {
      limitBeaches = Math.min(body.limit, 500);  // hard cap to keep one call safe
    }
  } catch { /* no body — refresh all with defaults */ }

  console.log("Request received —",
    "targetLocationIds:", targetLocationIds,
    "tideWindowDays:",   tideWindowDays,
    "forceTideRefresh:", forceTideRefresh,
    "skipRecentHours:",  skipRecentHours);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  const runAt    = new Date();
  const results: RefreshResult[] = [];

  try {
    // 1. Select beaches due for refresh via the all-in-one RPC.
    //
    // 2026-06-05 cutover (from PostgREST page-truncated SELECT): the prior
    // approach hit PostgREST's db-max-rows=1000 cap on beaches_gold, so
    // beaches with high fids (OR/WA/NH/MI/OH/RI/VA) were silently invisible
    // to the function — fid 6947591 stuck 14 days stale. The RPC does
    // the entire selection DB-side and returns just the ≤p_limit oldest
    // fids to process. Result is always ≤ limit so the cap never engages.
    //
    // Filters applied DB-side: is_active + scoring_tier IN (daily, hourly)
    // + optional target_location_ids + optional skip_recent_hours
    // (against beach_day_recommendations.generated_at, NOT updated_at — see
    // 552d757). Ordered by max(generated_at) ASC with NULLS FIRST so
    // never-written beaches surface immediately.
    console.log("Selecting due beaches via beaches_due_for_refresh RPC...");
    const { data: dueRows, error: dueErr } = await supabase.rpc(
      "beaches_due_for_refresh",
      {
        p_target_fids:         null,
        p_target_location_ids: targetLocationIds ?? null,
        p_skip_recent_hours:   skipRecentHours,
        p_limit:               limitBeaches,
      },
    );
    if (dueErr) throw new Error(`beaches_due_for_refresh failed: ${dueErr.message}`);
    const dueFids = (dueRows ?? []).map((r: { fid: number }) => r.fid);
    console.log(`RPC returned ${dueFids.length} beaches due for refresh`);
    if (dueFids.length === 0) {
      return json({
        ok: true,
        message: skipRecentHours !== null
          ? `All beaches refreshed within skip_recent_hours=${skipRecentHours} — nothing to do`
          : "No active beaches found",
        results: [], skipped_recent: 0,
      }, 200, cors);
    }

    // 2. Load full detail for just those fids (≤ limit rows; well below
    //    the PostgREST cap, no truncation risk). ensureNotTruncated()
    //    catches scope drift if limit ever grows past the cap.
    const detailResult = await supabase.from("beaches_gold")
      .select(`
        fid, location_id, name, display_name_override, lat, lon,
        noaa_station_id, besttime_venue_id, timezone, open_time, close_time,
        scoring_tier, is_active, address, website, description, parking_text,
        beach_dog_policy(dogs_prohibited_start, dogs_prohibited_end)
      `, { count: "exact" })
      .in("fid", dueFids);
    ensureNotTruncated(detailResult, "daily-beach-refresh: detail SELECT");
    const { data: goldRows, error: beachErr } = detailResult;
    if (beachErr) throw new Error(`Failed to load beach detail: ${beachErr.message}`);

    type GoldRow = {
      fid: number; location_id: string | null;
      name: string; display_name_override: string | null;
      lat: number; lon: number;
      noaa_station_id: string | null; besttime_venue_id: string | null;
      timezone: string; open_time: string | null; close_time: string | null;
      scoring_tier: string | null; is_active: boolean;
      address: string | null; website: string | null;
      description: string | null; parking_text: string | null;
      beach_dog_policy: { dogs_prohibited_start: string | null; dogs_prohibited_end: string | null }
                        | null
                        | { dogs_prohibited_start: string | null; dogs_prohibited_end: string | null }[];
    };
    const flatten = (g: GoldRow): Beach => {
      const dp = Array.isArray(g.beach_dog_policy) ? g.beach_dog_policy[0]
               : g.beach_dog_policy;
      return {
        location_id:    g.location_id ?? "",
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
        location_numb:  null,
        created_at:     "",
      };
    };
    let beaches: Beach[] = goldRows
      ? (goldRows as GoldRow[]).map(flatten)
      : [];
    const skippedRecent = 0;  // RPC applies skip_recent_hours DB-side; not counted client-side

    // 2. Load scoring config
    console.log("Loading scoring config...");
    const config = await loadScoringConfig(supabase, SCORING_VERSION);
    console.log("Scoring config loaded — version:", config.scoring_version);

    // 3. Process each beach sequentially
    for (const beach of beaches as Beach[]) {
      const result = await processBeach(
        beach, config, supabase, runAt,
        { tideWindowDays, forceTideRefresh, skipBesttime },
      );
      results.push(result);
    }

    // 4. Trigger notification dispatch (non-fatal)
    // await triggerNotificationDispatch(supabase);

    return json({
      ok: true, runAt: runAt.toISOString(),
      results,
      skipped_recent: skippedRecent,
    }, 200, cors);

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

interface RefreshOpts {
  tideWindowDays: number;
  forceTideRefresh: boolean;
  skipBesttime?: boolean;
}

async function processBeach(
  beach: Beach,
  config: ScoringConfig,
  supabase: ReturnType<typeof createClient>,
  runAt: Date,
  opts: RefreshOpts = { tideWindowDays: 7, forceTideRefresh: false },
): Promise<RefreshResult> {
  const phases: Record<string, "ok" | "error" | "skipped"> = {};
  console.log(`[${beach.location_id}] Starting refresh`);

  // a. Weather (W2.1 cutover — reads from weather_grid_hourly reference layer
  // via supabase RPC instead of fetching Open-Meteo per beach. Drops per-beach
  // cost from ~700ms to ~50-100ms; fixes WORKER_RESOURCE_LIMIT. See pin
  // [[weather-grid-reference-layer]]. Falls back to direct Open-Meteo fetch
  // ONLY if the grid lookup returns nothing — covers cells loaded mid-refresh.)
  let weatherResult: Awaited<ReturnType<typeof fetchWeather>>;
  let weatherSource: "grid" | "direct" = "grid";
  try {
    weatherResult = await fetchWeatherFromGrid(beach, supabase);
    if (weatherResult.hours.length === 0) {
      console.warn(`[${beach.location_id}] Grid empty for cell; falling back to direct Open-Meteo fetch`);
      weatherResult = await fetchWeather(beach);
      weatherSource = "direct";
    }
    phases.openmeteo = "ok";
    console.log(`[${beach.location_id}] Weather OK — ${weatherResult.hours.length} hours (source=${weatherSource})`);
  } catch (err) {
    await logError(supabase, beach.location_id, "openmeteo", err);
    phases.openmeteo = "error";
    return { locationId: beach.location_id, ok: false, error: String(err), phases };
  }

  // b. Tides — try cache first (rolling 7-day buffer), fetch only on miss.
  // Weekly cron passes forceTideRefresh + tideWindowDays=14 to refill the
  // buffer; daily cron uses defaults (no force, 7-day window) which means
  // we skip NOAA whenever stored rows cover the next 7 days at <=7d age.
  //
  // Inland/lake beaches without a NOAA station: skip tide fetch entirely
  // and proceed with empty tideMap. Scoring math defaults tideHeight=null
  // → tideScore=0.5 (neutral), same pattern as null crowd/busyness data.
  // Don't fail the whole beach for missing tide data.
  // b. Tides — cache-only by default per Franz directive 2026-05-25 LATE.
  // NOAA fetches happen ONLY when forceTideRefresh=true (i.e., the weekly
  // Sunday cron at 08:00 UTC fires with force=true to refresh the 14-day
  // buffer). Daily refresh just reads cache; cache misses → empty tideMap
  // → scoring treats as neutral (0.5). Tide harmonics don't drift between
  // weekly refreshes, so accuracy cost is zero.
  let tideMap: Map<string, number> = new Map();
  let tideFromCache = false;
  if (!beach.noaa_station_id) {
    phases.noaa = "skipped";
    console.log(`[${beach.location_id}] No NOAA station (inland) — scoring without tide`);
  } else if (opts.forceTideRefresh) {
    // Weekly cron path only — actually fetch from NOAA.
    try {
      tideMap = await fetchTides(beach, runAt, opts.tideWindowDays);
      phases.noaa = "ok";
      console.log(`[${beach.location_id}] Tides fetched — ${tideMap.size} hours (${opts.tideWindowDays}d window, weekly force-refresh)`);
    } catch (err) {
      await logError(supabase, beach.location_id, "noaa", err);
      phases.noaa = "error";
      tideMap = new Map();
      console.warn(`[${beach.location_id}] NOAA failed — proceeding without tide data`);
    }
  } else {
    // Daily path — cache only, never call NOAA.
    try {
      const cached = await tryReadTideCache(supabase, beach, runAt);
      if (cached) {
        tideMap = cached;
        tideFromCache = true;
        phases.noaa = "skipped";
        console.log(`[${beach.location_id}] Tides cached — ${tideMap.size} hours (no NOAA call)`);
      } else {
        phases.noaa = "skipped";
        console.log(`[${beach.location_id}] No tide cache — proceeding without tides (neutral score)`);
      }
    } catch (err) {
      phases.noaa = "skipped";
      tideMap = new Map();
      console.warn(`[${beach.location_id}] Tide cache read failed — proceeding without tides: ${err}`);
    }
  }

  // c. Crowds (non-fatal). Skipped via opts.skipBesttime (BestTime to be
  // phased out / replaced; default off as of 2026-05-22).
  let crowdResult: Awaited<ReturnType<typeof fetchCrowds>>;
  if (opts.skipBesttime) {
    phases.besttime = "skipped";
    crowdResult = { busynessMap: new Map(), venueId: "", isNewVenue: false };
  } else {
    try {
      crowdResult = await fetchCrowds(beach, BESTTIME_KEY_PRIVATE, BESTTIME_KEY_PUBLIC);
      phases.besttime = "ok";
      console.log(`[${beach.location_id}] Crowds OK — ${crowdResult.busynessMap.size} slots`);
      if (crowdResult.isNewVenue) {
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

  // g. Upsert hourly rows.
  // tideFromCache=true means we used cached tides; don't bump tide_refreshed_at
  // on these rows (preserves the original fetch timestamp).
  //
  // Self-healing fid-drift handling (gap #37 / #185, 2026-05-23): the
  // scoring tables have a redundant partial unique on (location_id,
  // forecast_ts) alongside the PK on (arena_group_id, forecast_ts).
  // When the pipeline's promote phase re-fids a beach (e.g. GNIS
  // ingestion creates a new fid for an existing location), old rows
  // under the OLD arena_group_id but SAME location_id strand. ON
  // CONFLICT (arena_group_id, forecast_ts) misses them; the upsert
  // falls through to INSERT; the partial unique fires; refresh fails.
  // Fix: delete by location_id first within this forecast window, so
  // any orphans from prior fids get cleared before the fresh insert.
  // Idempotent and safe — every row deleted is about to be re-inserted.
  const dates = [...new Set(scoredHours.map((h) => h.localDate))].sort();
  try {
    const hourlyRows = scoredHours.map((h) =>
      buildHourlyRow(h, beach, config, h.hourText, runAt, tideFromCache)
    );
    // Clear any pre-existing rows for this location in the forecast window
    // (handles fid-drift orphans + simple replace-on-rerun semantics).
    if (hourlyRows.length > 0) {
      const minTs = hourlyRows[0].forecast_ts;
      const maxTs = hourlyRows[hourlyRows.length - 1].forecast_ts;
      const { error: delErr } = await supabase
        .from("beach_day_hourly_scores")
        .delete()
        .eq("location_id", beach.location_id)
        .gte("forecast_ts", minTs)
        .lte("forecast_ts", maxTs);
      if (delErr) throw new Error(`hourly clear failed: ${delErr.message}`);
    }
    for (let i = 0; i < hourlyRows.length; i += 100) {
      const { error } = await supabase
        .from("beach_day_hourly_scores")
        .upsert(hourlyRows.slice(i, i + 100), { onConflict: "arena_group_id,forecast_ts" });
      if (error) throw new Error(error.message);
    }
    phases.upsert_hourly = "ok";
    console.log(`[${beach.location_id}] Upserted ${hourlyRows.length} hourly rows`);
  } catch (err) {
    await logError(supabase, beach.location_id, "upsert", err);
    phases.upsert_hourly = "error";
    return { locationId: beach.location_id, ok: false, error: String(err), phases };
  }

  // h. Upsert daily rows — same fid-drift handling as hourly.
  try {
    const dailyRows = await Promise.all(dates.map(async (date) => {
      const dayHours     = scoredHours.filter((h) => h.localDate === date);
      const window       = windows.get(date) ?? null;
      const recentPrecip = await computePrecipForDay(supabase, beach, rawHours, weatherResult.hours, date);
      const bacteriaRisk = deriveBacteriaRisk(recentPrecip.precip72hMm, cautionMm, nogoMm);
      console.log(`[${beach.location_id}] ${date}: 72h=${recentPrecip.precip72hMm}mm → ${bacteriaRisk}`);
      return buildDailyRow(beach, date, dayHours, window, config, runAt, recentPrecip, bacteriaRisk);
    }));
    if (dailyRows.length > 0) {
      const { error: delErr } = await supabase
        .from("beach_day_recommendations")
        .delete()
        .eq("location_id", beach.location_id)
        .in("local_date", dates);
      if (delErr) throw new Error(`daily clear failed: ${delErr.message}`);
    }
    const { error } = await supabase
      .from("beach_day_recommendations")
      .upsert(dailyRows, { onConflict: "arena_group_id,local_date" });
    if (error) throw new Error(error.message);
    phases.upsert_daily = "ok";
    console.log(`[${beach.location_id}] Upserted ${dailyRows.length} daily rows`);
  } catch (err) {
    await logError(supabase, beach.location_id, "upsert", err);
    phases.upsert_daily = "error";
    return { locationId: beach.location_id, ok: false, error: String(err), phases };
  }

  // Phase v2-best-window: overwrite v1 best_window_label/start/end on
  // each date with the v2-scored equivalent. Soft-fail so a bug here
  // can't block the v1 daily pipeline. Mirrors the dog-park hook.
  // beaches_gold.fid == beach_dog_policy.arena_group_id, so use that.
  if (beach.arena_group_id != null) {
    try {
      for (const d of dates) {
        const { error: rpcErr } = await supabase.rpc(
          "apply_v2_best_window_to_beach_recommendations",
          { p_fid: beach.arena_group_id, p_date: d },
        );
        if (rpcErr) console.warn(`[${beach.location_id}] v2 best window apply ${d}: ${rpcErr.message}`);
      }
      phases.apply_v2_window = "ok";
    } catch (err) {
      console.warn(`[${beach.location_id}] v2 best window apply error: ${err}`);
      phases.apply_v2_window = "soft_error";
    }
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
// precip_72h reads from precipitation_history via precip_72h_for_point() RPC.
// precip_24h stays inline because daily-beach-refresh's fetched weather hours
// already cover the 24h trailing window — no need to round-trip the DB.
async function computePrecipForDay(
  supabase: ReturnType<typeof createClient>,
  beach: Beach,
  rawHours: RawHourData[],
  weatherHours: Awaited<ReturnType<typeof fetchWeather>>["hours"],
  localDate: string,
): Promise<{ precip24hMm: number; precip72hMm: number }> {
  // 24h: inline from fetched weather hours (forecast window covers this).
  let precip24h = 0;
  const firstIdx = rawHours.findIndex((h) => h.localDate === localDate);
  if (firstIdx >= 0) {
    const anchorMs = new Date(rawHours[firstIdx].forecastTs).getTime();
    const ms24h    = 24 * 3_600_000;
    const len      = Math.min(rawHours.length, weatherHours.length);
    for (let i = 0; i < len; i++) {
      const tsMs = new Date(rawHours[i].forecastTs).getTime();
      if (tsMs >= anchorMs) continue;
      if (anchorMs - tsMs <= ms24h) {
        precip24h += weatherHours[i].precipitation ?? 0;
      }
    }
  }

  // 72h: via precip_72h_for_point() against precipitation_history (3-day
  // rollup, maintained by _refresh_precipitation_history_from_grid()).
  let precip72h = 0;
  const { data: rpcVal, error: rpcErr } = await supabase.rpc("precip_72h_for_point", {
    p_lat:         beach.latitude,
    p_lng:         beach.longitude,
    p_anchor_date: localDate,
  });
  if (rpcErr) {
    console.warn(`[${beach.location_id}] precip_72h_for_point failed: ${rpcErr.message}`);
  } else if (typeof rpcVal === "number") {
    precip72h = rpcVal;
  } else if (typeof rpcVal === "string") {
    precip72h = parseFloat(rpcVal) || 0;
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

// ─── Weather grid lookup (W2.1 — replaces per-beach Open-Meteo fetch) ────────
//
// Reads from weather_grid_hourly via the weather_for_point() RPC and shapes
// the result to OpenMeteoResult so buildRawHours stays unchanged. Returns
// {hours: [], days: []} if the cell has no rows (caller falls back to direct
// fetch). Per [[weather-grid-reference-layer]].

async function fetchWeatherFromGrid(
  beach: { location_id: string; latitude: number; longitude: number; timezone: string },
  supabase: ReturnType<typeof createClient>,
): Promise<Awaited<ReturnType<typeof fetchWeather>>> {
  // Window: past_3d → +7d, matches the existing fetchWeather range.
  const now = new Date();
  const startTs = new Date(now.getTime() - 3 * 86_400_000);
  const endTs   = new Date(now.getTime() + 7 * 86_400_000);

  const { data, error } = await supabase.rpc("weather_for_point", {
    p_lat:      beach.latitude,
    p_lng:      beach.longitude,
    p_start_ts: startTs.toISOString(),
    p_end_ts:   endTs.toISOString(),
  });

  if (error) {
    throw new Error(`weather_for_point RPC: ${error.message}`);
  }
  if (!Array.isArray(data) || data.length === 0) {
    return { hours: [], days: [] };
  }

  // Format each row's forecast_ts as local-time string ("YYYY-MM-DDTHH:MM")
  // in the beach's timezone — matches Open-Meteo's native response shape
  // so buildRawHours's localDate/localHour slicing works unchanged.
  const fmt = new Intl.DateTimeFormat("sv-SE", {
    timeZone: beach.timezone,
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

  return { hours, days: [] }; // sunrise/sunset unused downstream
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
      cloudCover:    wh.cloud_cover ?? null,
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
  tideFromCache: boolean = false,
) {
  return {
    location_id:           beach.location_id,
    arena_group_id:        beach.arena_group_id ?? null,
    local_date:            h.localDate,
    forecast_ts:           h.forecastTs,
    local_hour:            h.localHour,
    hour_label:            h.hourLabel,
    is_daylight:           h.isDaylight,
    // Freshness markers — only bump tide_refreshed_at when we actually
    // hit NOAA. weather_refreshed_at always bumps for now (Step 3 will
    // add per-day tier logic).
    ...(tideFromCache ? {} : { tide_refreshed_at: runAt.toISOString() }),
    weather_refreshed_at:  runAt.toISOString(),
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
    // hour_status + per-metric *_status columns retired per Franz
    // 2026-05-30 v1-retirement task #11. Raw values + hour_score are
    // kept (downstream apply_v2 derives v2 statuses inline). Columns
    // will be dropped from the schema by task #12.
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
    // day_status retired per Franz 2026-05-30 v1-retirement task #11.
    // day_status_v2 is the consumer-facing field; written by
    // apply_v2_best_window_to_beach_recommendations after this upsert.
    // Column dropped by task #12.
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

// Read tide values from beach_day_hourly_scores when fresh enough that
// we don't need a NOAA fetch. Returns Map<"YYYY-MM-DD HH", height_ft>
// matching what fetchTides returns, or null if the cache misses.
//
// Cache hit requires:
//   * All 7 forecast days (today + next 6) present in the table
//   * Every row's tide_refreshed_at is within the last 7 days
//   * Every row has a non-null tide_height
async function tryReadTideCache(
  supabase: ReturnType<typeof createClient>,
  beach: Beach,
  runAt: Date,
): Promise<Map<string, number> | null> {
  const startDate = runAt.toISOString().slice(0, 10);
  const endDate   = new Date(runAt);
  endDate.setUTCDate(endDate.getUTCDate() + 6);
  const endDateStr = endDate.toISOString().slice(0, 10);
  const cutoff = new Date(runAt);
  cutoff.setUTCDate(cutoff.getUTCDate() - 7);

  const { data, error } = await supabase
    .from("beach_day_hourly_scores")
    .select("local_date, local_hour, tide_height, tide_refreshed_at")
    .eq("arena_group_id", beach.arena_group_id)
    .gte("local_date", startDate)
    .lte("local_date", endDateStr);
  if (error || !data || data.length === 0) return null;

  // Need at least one row per day for all 7 days in the window.
  const daysCovered = new Set<string>();
  for (const r of data) daysCovered.add(r.local_date);
  for (let i = 0; i < 7; i++) {
    const d = new Date(runAt);
    d.setUTCDate(d.getUTCDate() + i);
    if (!daysCovered.has(d.toISOString().slice(0, 10))) return null;
  }

  // Every row must be fresh + have tide data.
  const tideMap = new Map<string, number>();
  for (const r of data) {
    if (r.tide_height == null) return null;
    if (!r.tide_refreshed_at) return null;
    if (new Date(r.tide_refreshed_at) < cutoff) return null;
    const hourKey = `${r.local_date} ${String(r.local_hour).padStart(2, "0")}`;
    tideMap.set(hourKey, Number(r.tide_height));
  }
  return tideMap;
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
