// get-beaches-find/index.ts
// Single-query beach list for find.html day-pill views.
// Uses the find_beaches PostgreSQL RPC (PostGIS) for efficient distance computation.
//
// GET ?date=2026-04-18[&lat=33.6&lng=-117.9][&leash=any|off_leash|on_leash|mixed][&scored=true|false]
//   scored=true  (default) → only beaches with a beach_day_recommendations row
//   scored=false           → full beaches_gold catalog (763 active rows)
//
// Returns: { date, beaches: RankedBeach[], is_today: boolean }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "GET, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });

  try {
    const url    = new URL(req.url);
    const nowUtc = new Date();

    // Default date: today in Pacific time
    const pacificDate = new Intl.DateTimeFormat("en-CA", {
      timeZone: "America/Los_Angeles",
      year: "numeric", month: "2-digit", day: "2-digit",
    }).format(nowUtc);

    const date  = url.searchParams.get("date")  ?? pacificDate;
    const leash = url.searchParams.get("leash") ?? "any";
    const latParam = url.searchParams.get("lat");
    const lngParam = url.searchParams.get("lng");
    const lat   = latParam ? parseFloat(latParam) : null;
    const lng   = lngParam ? parseFloat(lngParam) : null;
    // scored=true → only beaches with day_recommendations rows.
    // scored=false (NEW DEFAULT) → full catalog visible; client renders
    // tier-aware icons (1+2 normal, 3 muted, 4 grey-no-dog). The cost
    // gate (scoring_tier IN ('daily','hourly')) is enforced upstream by
    // daily-beach-refresh, not by this read query — display gates are
    // tier-based.
    const scoredParam = url.searchParams.get("scored");
    const scoredOnly  = scoredParam === "true" ? true : false;

    // Bounded result set via spatial KNN. Only applied when lat/lng present;
    // without coords the server still returns the full active set (ghost-user
    // path). Capped at 50 to prevent oversized responses.
    const MAX_LIMIT   = 50;
    const limitParam  = url.searchParams.get("limit");
    const limitParsed = limitParam ? parseInt(limitParam, 10) : NaN;
    const limit       = Number.isFinite(limitParsed) && limitParsed > 0
      ? Math.min(limitParsed, MAX_LIMIT)
      : null;

    const isToday = date === pacificDate;

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

    // Current Pacific hour (needed for remaining-window logic)
    const localParts = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/Los_Angeles",
      hour: "2-digit", hour12: false,
    }).formatToParts(nowUtc);
    const currentHour = parseInt(localParts.find(p => p.type === "hour")?.value ?? "0") % 24;

    // ── 1. find_beaches RPC: beaches + day rec, one join, PostGIS distance ────
    const { data: beaches, error: rpcErr } = await supabase.rpc("find_beaches", {
      p_date:         date,
      p_lat:          lat,
      p_lng:          lng,
      p_leash:        leash,
      p_limit:        limit,
      p_scored_only:  scoredOnly,
    });

    if (rpcErr || !beaches) return json({ error: rpcErr?.message ?? "RPC failed" }, 500);

    const arenaGroupIds = (beaches as BeachRow[])
      .map(b => b.arena_group_id)
      .filter((x): x is number => typeof x === "number");
    if (arenaGroupIds.length === 0) return json({ date, is_today: isToday, beaches: [] });

    // ── 2. Hourly scores in one query ─────────────────────────────────────────
    // For today: fetch all candidate-window hours so we can compute remaining windows.
    // For other dates: fetch only best-window hours for component score averaging.
    const hoursQuery = supabase
      .from("beach_day_hourly_scores")
      .select(
        // v2-only after Franz 2026-05-30 task #12. v1 *_status columns
        // dropped. Window-status rollup below uses raw values to derive
        // per-hour v2 status via the inline TS port.
        "arena_group_id, local_hour, hour_score_v2, " +
        "is_in_best_window, is_candidate_window, " +
        "explainability, " +
        "tide_height, wind_speed, precip_chance, busyness_score, " +
        "feels_like, uv_index, sand_temp, asphalt_temp"
      )
      .in("arena_group_id", arenaGroupIds)
      .eq("local_date", date)
      .eq("is_daylight", true)
      .order("local_hour", { ascending: true });

    const { data: hours, error: hoursErr } = await hoursQuery;
    if (hoursErr) return json({ error: hoursErr.message }, 500);

    // ── 3. Build per-beach data structures ───────────────────────────────────
    type HourRow = {
      arena_group_id: number;
      local_hour: number;
      hour_score_v2: number | null;
      is_in_best_window: boolean;
      is_candidate_window: boolean;
      explainability: Record<string, number>;
      tide_height: number | null; wind_speed: number | null;
      precip_chance: number | null; busyness_score: number | null;
      feels_like: number | null; uv_index: number | null;
      sand_temp: number | null; asphalt_temp: number | null;
    };

    // Group hours by arena_group_id
    const hoursByFid: Record<number, HourRow[]> = {};
    for (const h of (hours ?? []) as HourRow[]) {
      (hoursByFid[h.arena_group_id] ??= []).push(h);
    }

    // Compute composite + component scores per beach
    const scoresByFid: Record<number, {
      composite: number; tide: number; wind: number;
      crowd: number; rain: number; temp: number; count: number;
      bestWindowLabel: string | null; bestWindowStatus: string | null;
    }> = {};

    for (const fid of arenaGroupIds) {
      const locHours = hoursByFid[fid] ?? [];

      let windowHours: HourRow[];
      let bestWindowLabel:  string | null = null;
      let bestWindowStatus: string | null = null;

      if (isToday) {
        // Recompute best remaining window
        const remaining = locHours.filter(
          h => h.is_candidate_window && Number(h.local_hour) >= currentHour
        );
        const win = findBestRemainingWindow(remaining);
        const bestSet = new Set(win ? win.hours.map(h => h.local_hour) : []);
        windowHours      = locHours.filter(h => bestSet.has(Number(h.local_hour)));
        bestWindowLabel  = win ? buildWindowLabel(win.startHour, win.endHour) : "No good window remaining";
        bestWindowStatus = win ? win.status : "no_go";
      } else {
        windowHours = locHours.filter(h => h.is_in_best_window);
      }

      const count = windowHours.length;
      if (count === 0) {
        scoresByFid[fid] = { composite: 0, tide: 0, wind: 0, crowd: 0, rain: 0, temp: 0, count: 0, bestWindowLabel, bestWindowStatus };
        continue;
      }

      let composite = 0, tide = 0, wind = 0, crowd = 0, rain = 0, temp = 0;
      for (const h of windowHours) {
        const ex = h.explainability ?? {};
        // Prefer v2 score; fall back to v1 during transition when
        // apply_v2_best_window_to_beach_recommendations hasn't backfilled
        // this beach yet. Per Franz 2026-05-30 v1-retirement task #5.
        composite += Number(h.hour_score_v2 ?? 0);
        tide      += ex.tide_score  ?? 0;
        wind      += ex.wind_score  ?? 0;
        crowd     += ex.crowd_score ?? 0;
        rain      += ex.rain_score  ?? 0;
        temp      += ex.temp_score  ?? 0;
      }

      scoresByFid[fid] = {
        composite: composite / count,
        tide:      tide      / count,
        wind:      wind      / count,
        crowd:     crowd     / count,
        rain:      rain      / count,
        temp:      temp      / count,
        count,
        bestWindowLabel,
        bestWindowStatus,
      };
    }

    // ── 4. Assemble and rank ──────────────────────────────────────────────────
    const ranked = (beaches as BeachRow[]).map(b => {
      const s = scoresByFid[b.arena_group_id];
      const bestWindowLabel  = isToday ? (s?.bestWindowLabel  ?? b.best_window_label)  : b.best_window_label;
      const bestWindowStatus = isToday ? (s?.bestWindowStatus ?? b.best_window_status) : b.best_window_status;
      return {
        arena_group_id:     b.arena_group_id,
        location_id:        b.location_id,
        display_name:       b.display_name,
        latitude:           b.latitude,
        longitude:          b.longitude,
        access_rule:        b.access_rule,
        has_on_leash:       b.has_on_leash  ?? null,
        has_off_leash:      b.has_off_leash ?? null,
        dogs_allowed:       b.dogs_allowed  ?? null,
        dogs_prohibited_start: b.dogs_prohibited_start ?? null,
        location_tier:      b.location_tier ?? null,
        distance_m:         b.distance_m ?? null,
        // v2-only after Franz 2026-05-30 task #12.
        day_status:         (b as Record<string, unknown>).day_status_v2 ?? "no_data",
        day_status_v2:      (b as Record<string, unknown>).day_status_v2 ?? null,
        composite_score_v2: (b as Record<string, unknown>).composite_score_v2 ?? null,
        best_window_label:  bestWindowLabel  ?? null,
        best_window_status: bestWindowStatus ?? null,
        bacteria_risk:      b.bacteria_risk  ?? null,
        summary_weather:    b.summary_weather ?? null,
        weather_code:       b.weather_code    ?? null,
        lowest_tide_height: b.lowest_tide_height ?? null,
        avg_tide_height:    b.avg_tide_height    ?? null,
        avg_temp:           b.avg_temp  ?? null,
        avg_wind:           b.avg_wind  ?? null,
        busyness_category:  b.busyness_category ?? null,
        go_hours_count:     b.go_hours_count    ?? 0,
        composite_score:    s ? Math.round(s.composite) : 0,
        tide_score:         s?.tide  ?? null,
        wind_score:         s?.wind  ?? null,
        crowd_score:        s?.crowd ?? null,
        rain_score:         s?.rain  ?? null,
        temp_score:         s?.temp  ?? null,
      };
    });
    // Preserve the RPC's order: distance-ordered when lat/lng present (GIST
    // KNN), unspecified otherwise. Clients sort explicitly (find.html by
    // dropdown mode, index.html always by distance) so server-side sort
    // is both wasted work and wrong for the index.html switcher.

    return json({ date, is_today: isToday, beaches: ranked });

  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});

// ─── Types ────────────────────────────────────────────────────────────────────

interface BeachRow {
  arena_group_id:     number;
  location_id:        string | null;
  display_name:       string;
  latitude:           number;
  longitude:          number;
  access_rule:        string | null;
  has_on_leash:       boolean | null;
  has_off_leash:      boolean | null;
  dogs_allowed:       string | null;
  dogs_prohibited_start: string | null;
  location_tier:      string | null;
  distance_m:         number | null;
  day_status:         string | null;
  best_window_label:  string | null;
  best_window_status: string | null;
  bacteria_risk:      string | null;
  summary_weather:    string | null;
  weather_code:       number | null;
  lowest_tide_height: number | null;
  avg_tide_height:    number | null;
  avg_temp:           number | null;
  avg_wind:           number | null;
  busyness_category:  string | null;
  go_hours_count:     number | null;
}

// ─── Window helpers (mirrors get-beach-detail logic) ─────────────────────────

function formatHour(hour: number): string {
  if (hour === 0 || hour === 24) return "12am";
  if (hour === 12) return "12pm";
  return hour < 12 ? `${hour}am` : `${hour - 12}pm`;
}

function buildWindowLabel(startHour: number, endHour: number): string {
  return `${formatHour(startHour)}–${formatHour(endHour + 1)}`;
}

type CandidateHour = {
  local_hour: number; hour_score_v2: number | null;
  tide_height: number | null; wind_speed: number | null;
  precip_chance: number | null; busyness_score: number | null;
  feels_like: number | null; uv_index: number | null;
  sand_temp: number | null; asphalt_temp: number | null;
  [key: string]: unknown;
};

function findBestRemainingWindow(hours: CandidateHour[]): {
  startHour: number; endHour: number; avgScore: number;
  status: string; hours: CandidateHour[];
} | null {
  if (!hours.length) return null;

  // v2-only after Franz 2026-05-30 task #12.
  const score = (h: CandidateHour) => Number(h.hour_score_v2 ?? 0);

  const sorted    = [...hours].sort((a, b) => a.local_hour - b.local_hour);
  const peak      = sorted.reduce((b, h) => score(h) > score(b) ? h : b);
  const peakScore = score(peak);
  const peakIdx   = sorted.indexOf(peak);

  const STEP = 0.05;
  let threshold = 0.93;
  let window: CandidateHour[] = [];

  while (true) {
    const minScore = peakScore * threshold;
    window = [peak];

    for (let i = peakIdx + 1; i < sorted.length; i++) {
      const h = sorted[i], prev = window[window.length - 1];
      if (h.local_hour !== prev.local_hour + 1) break;
      if (score(h) < minScore)                  break;
      window.push(h);
    }
    for (let i = peakIdx - 1; i >= 0; i--) {
      const h = sorted[i], next = window[0];
      if (next.local_hour !== h.local_hour + 1) break;
      if (score(h) < minScore)                  break;
      window.unshift(h);
    }

    if (window.length >= 2) break;
    if (threshold <= 0)     break;
    threshold = Math.max(0, threshold - STEP);
  }

  if (window.length < 2) return null;

  // Window status = worst per-hour v2 status across the window hours.
  // Computed inline from raw values via v2StatusFor — v1 *_status columns
  // dropped from the schema per Franz 2026-05-30 task #12.
  const statusRank: Record<string, number> = { clear: 0, advisory: 1, caution: 2, no_go: 3 };
  const overallStatus = window.reduce<string | null>((worst, h) => {
    const hStatus = v2HourStatus(h);
    if (worst === null) return hStatus;
    return (statusRank[hStatus] ?? 0) >= (statusRank[worst] ?? 0) ? hStatus : worst;
  }, null) ?? "clear";

  return {
    startHour: window[0].local_hour,
    endHour:   window[window.length - 1].local_hour,
    avgScore:  window.reduce((s, h) => s + score(h), 0) / window.length,
    status:    overallStatus,
    hours:     window,
  };
}

// ─── v2 status helpers (inline port of public.v2_signal_status) ──────────
// SQL truth-source: scoring_config_v2 + v2_signal_status. Pin
// [[v2-signal-status-mapping]]. Keep in sync when bands change.
type V2Status = "clear" | "advisory" | "caution" | "no_go";
function v2StatusFor(signal: string, v: number | null | undefined): V2Status | null {
  if (v == null) return null;
  switch (signal) {
    case "uv":         return v >= 11 ? "no_go" : v >= 9 ? "caution" : v >= 6 ? "advisory" : "clear";
    case "asphalt":    return v >= 125 ? "no_go" : v >= 115 ? "advisory" : "clear";
    case "sand":       return v >= 145 ? "no_go" : v >= 125 ? "caution" : v >= 115 ? "advisory" : "clear";
    case "tide":       return v >= 7 ? "no_go" : v >= 5 ? "caution" : v >= 3 ? "advisory" : "clear";
    case "wind":       return v >= 35 ? "no_go" : v >= 19 ? "caution" : v >= 13 ? "advisory" : "clear";
    case "crowd":      return v >= 85 ? "no_go" : v >= 60 ? "caution" : v >= 30 ? "advisory" : "clear";
    case "precip":     return v >= 80 ? "no_go" : v >= 50 ? "caution" : v >= 30 ? "advisory" : "clear";
    case "feels_hot":  return v >= 125 ? "no_go" : v >= 95 ? "caution" : v >= 85 ? "advisory" : "clear";
    case "feels_cold": return v <= 20 ? "no_go" : v <= 32 ? "caution" : v <= 50 ? "advisory" : "clear";
    default:           return null;
  }
}
function v2HourStatus(h: { tide_height: number | null; wind_speed: number | null;
                          precip_chance: number | null; busyness_score: number | null;
                          feels_like: number | null; uv_index: number | null;
                          sand_temp: number | null; asphalt_temp: number | null }): V2Status {
  const rank: Record<V2Status, number> = { clear: 0, advisory: 1, caution: 2, no_go: 3 };
  let worst: V2Status = "clear";
  const consider = (s: V2Status | null) => { if (s && rank[s] > rank[worst]) worst = s; };
  consider(v2StatusFor("tide",       h.tide_height));
  consider(v2StatusFor("wind",       h.wind_speed));
  consider(v2StatusFor("precip",     h.precip_chance));
  consider(v2StatusFor("crowd",      h.busyness_score));
  consider(v2StatusFor("uv",         h.uv_index));
  consider(v2StatusFor("sand",       h.sand_temp));
  consider(v2StatusFor("asphalt",    h.asphalt_temp));
  consider(v2StatusFor("feels_hot",  h.feels_like));
  consider(v2StatusFor("feels_cold", h.feels_like));
  return worst;
}
