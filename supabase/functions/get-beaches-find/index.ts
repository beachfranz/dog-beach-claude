// get-beaches-find/index.ts
// Single-query beach list for find.html day-pill views.
// Uses the find_beaches PostgreSQL RPC (PostGIS) for efficient distance computation.
//
// GET ?date=2026-04-18[&lat=33.6&lng=-117.9][&leash=any|off_leash|on_leash|mixed]
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
      p_date:  date,
      p_lat:   lat,
      p_lng:   lng,
      p_leash: leash,
      p_limit: limit,
    });

    if (rpcErr || !beaches) return json({ error: rpcErr?.message ?? "RPC failed" }, 500);

    const locationIds = (beaches as BeachRow[]).map(b => b.location_id);
    if (locationIds.length === 0) return json({ date, is_today: isToday, beaches: [] });

    // ── 2. Hourly scores in one query ─────────────────────────────────────────
    // For today: fetch all candidate-window hours so we can compute remaining windows.
    // For other dates: fetch only best-window hours for component score averaging.
    const hoursQuery = supabase
      .from("beach_day_hourly_scores")
      .select(
        "location_id, local_hour, hour_score, is_in_best_window, is_candidate_window, " +
        "explainability, hour_status, " +
        "tide_status, wind_status, crowd_status, rain_status, temp_status, uv_status"
      )
      .in("location_id", locationIds)
      .eq("local_date", date)
      .eq("is_daylight", true)
      .order("local_hour", { ascending: true });

    const { data: hours, error: hoursErr } = await hoursQuery;
    if (hoursErr) return json({ error: hoursErr.message }, 500);

    // ── 3. Build per-beach data structures ───────────────────────────────────
    type HourRow = {
      location_id: string;
      local_hour: number;
      hour_score: number;
      is_in_best_window: boolean;
      is_candidate_window: boolean;
      explainability: Record<string, number>;
      hour_status: string | null;
      tide_status: string | null; wind_status: string | null;
      crowd_status: string | null; rain_status: string | null;
      temp_status: string | null; uv_status: string | null;
    };

    // Group hours by location
    const hoursByLocation: Record<string, HourRow[]> = {};
    for (const h of (hours ?? []) as HourRow[]) {
      (hoursByLocation[h.location_id] ??= []).push(h);
    }

    // Compute composite + component scores per beach
    const scoresByLocation: Record<string, {
      composite: number; tide: number; wind: number;
      crowd: number; rain: number; temp: number; count: number;
      bestWindowLabel: string | null; bestWindowStatus: string | null;
    }> = {};

    for (const locId of locationIds) {
      const locHours = hoursByLocation[locId] ?? [];

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
        scoresByLocation[locId] = { composite: 0, tide: 0, wind: 0, crowd: 0, rain: 0, temp: 0, count: 0, bestWindowLabel, bestWindowStatus };
        continue;
      }

      let composite = 0, tide = 0, wind = 0, crowd = 0, rain = 0, temp = 0;
      for (const h of windowHours) {
        const ex = h.explainability ?? {};
        composite += Number(h.hour_score ?? 0);
        tide      += ex.tide_score  ?? 0;
        wind      += ex.wind_score  ?? 0;
        crowd     += ex.crowd_score ?? 0;
        rain      += ex.rain_score  ?? 0;
        temp      += ex.temp_score  ?? 0;
      }

      scoresByLocation[locId] = {
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
      const s = scoresByLocation[b.location_id];
      const bestWindowLabel  = isToday ? (s?.bestWindowLabel  ?? b.best_window_label)  : b.best_window_label;
      const bestWindowStatus = isToday ? (s?.bestWindowStatus ?? b.best_window_status) : b.best_window_status;
      return {
        location_id:        b.location_id,
        display_name:       b.display_name,
        latitude:           b.latitude,
        longitude:          b.longitude,
        access_rule:        b.access_rule,
        distance_m:         b.distance_m ?? null,
        day_status:         b.day_status  ?? "no_data",
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
    }).sort((a, b) => b.composite_score - a.composite_score);

    return json({ date, is_today: isToday, beaches: ranked });

  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});

// ─── Types ────────────────────────────────────────────────────────────────────

interface BeachRow {
  location_id:        string;
  display_name:       string;
  latitude:           number;
  longitude:          number;
  access_rule:        string | null;
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
  local_hour: number; hour_score: number;
  tide_status: string | null; wind_status: string | null;
  rain_status: string | null; crowd_status: string | null;
  temp_status: string | null; uv_status: string | null;
  [key: string]: unknown;
};

function findBestRemainingWindow(hours: CandidateHour[]): {
  startHour: number; endHour: number; avgScore: number;
  status: string; hours: CandidateHour[];
} | null {
  if (!hours.length) return null;

  const sorted    = [...hours].sort((a, b) => a.local_hour - b.local_hour);
  const peak      = sorted.reduce((b, h) => Number(h.hour_score) > Number(b.hour_score) ? h : b);
  const peakScore = Number(peak.hour_score);
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
      if (Number(h.hour_score) < minScore)      break;
      window.push(h);
    }
    for (let i = peakIdx - 1; i >= 0; i--) {
      const h = sorted[i], next = window[0];
      if (next.local_hour !== h.local_hour + 1) break;
      if (Number(h.hour_score) < minScore)       break;
      window.unshift(h);
    }

    if (window.length >= 2) break;
    if (threshold <= 0)     break;
    threshold = Math.max(0, threshold - STEP);
  }

  if (window.length < 2) return null;

  const statusRank: Record<string, number> = { go: 1, advisory: 2, caution: 3, no_go: 4 };
  const worst = (a: string | null, b: string | null) => {
    if (!a) return b; if (!b) return a;
    return (statusRank[a] ?? 0) >= (statusRank[b] ?? 0) ? a : b;
  };
  const overallStatus = window.reduce(
    (s, h) => worst(s, worst(worst(worst(h.tide_status, h.wind_status), worst(h.rain_status, h.crowd_status)), worst(h.temp_status, h.uv_status))),
    null as string | null
  ) ?? "go";

  return {
    startHour: window[0].local_hour,
    endHour:   window[window.length - 1].local_hour,
    avgScore:  window.reduce((s, h) => s + Number(h.hour_score), 0) / window.length,
    status:    overallStatus,
    hours:     window,
  };
}
