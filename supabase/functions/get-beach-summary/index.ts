// get-beach-summary/index.ts 
// Serves 7-day beach recommendations for the summary screen.
// 
// GET /functions/v1/get-beach-summary?location_id=huntington-dog-beach
// Returns: { beach, days: DayRecommendation[] }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "GET, OPTIONS"), "Content-Type": "application/json" };
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: cors });
  }

  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  try {
    const url               = new URL(req.url);
    const locationIdParam   = url.searchParams.get("location_id");
    const arenaGroupIdParam = url.searchParams.get("arena_group_id") ?? url.searchParams.get("fid");

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
    const nowUtc   = new Date();

    // Resolve to fid via either input. Legacy slug lives on beaches_gold now.
    let fid: number | null = null;
    if (arenaGroupIdParam) {
      const parsed = parseInt(arenaGroupIdParam, 10);
      if (!Number.isFinite(parsed)) return json({ error: "Invalid arena_group_id" }, 400);
      fid = parsed;
    } else {
      const slug = locationIdParam ?? "huntington-dog-beach";
      const { data: row } = await supabase
        .from("beaches_gold")
        .select("fid")
        .eq("location_id", slug)
        .limit(1);
      fid = row?.[0]?.fid ?? null;
      if (!fid) return json({ error: "Beach not found (slug not in spine)" }, 404);
    }

    // Beach metadata — all on the spine now.
    const { data: goldRows, error: goldErr } = await supabase
      .from("beaches_gold")
      .select("fid, location_id, name, display_name_override, timezone, address, website")
      .eq("fid", fid)
      .limit(1);
    const gold = goldRows?.[0];
    if (goldErr || !gold) return json({ error: "Beach not found" }, 404);
    const beach = {
      location_id:     gold.location_id ?? null,
      arena_group_id:  gold.fid,
      display_name:    gold.display_name_override ?? gold.name,
      timezone:        gold.timezone ?? "America/Los_Angeles",
      address:         gold.address ?? null,
      website:         gold.website ?? null,
    };
    const locationId = beach.location_id;  // may be null for non-curated

    // Current local date + hour for this beach (must use beach timezone, not UTC)
    const localParts = new Intl.DateTimeFormat("en-US", {
      timeZone: beach.timezone,
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", hour12: false,
    }).formatToParts(nowUtc);
    const getPart = (t: string) => localParts.find(p => p.type === t)?.value ?? "";
    const today         = `${getPart("year")}-${getPart("month")}-${getPart("day")}`;
    const currentLocalHour = parseInt(getPart("hour")) % 24;

    // Fetch 7 days of recommendations starting today (keyed on arena_group_id)
    const { data: days, error: daysErr } = await supabase
      .from("beach_day_recommendations")
      .select("*")
      .eq("arena_group_id", fid)
      .gte("local_date", today)
      .order("local_date", { ascending: true })
      .limit(7);

    if (daysErr) {
      return json({ error: daysErr.message }, 500);
    }

    const dates        = (days ?? []).map(d => d.local_date);
    const futureDates  = dates.filter(d => d !== today);

    // Query 1: best-window hours for future days (keyed on arena_group_id)
    const { data: futureHours } = futureDates.length ? await supabase
      .from("beach_day_hourly_scores")
      .select("local_date, local_hour, hour_score, tide_status, wind_status, rain_status, crowd_status, temp_status, uv_status")
      .eq("arena_group_id", fid)
      .in("local_date", futureDates)
      .eq("is_in_best_window", true) : { data: [] };

    // Query 2: remaining candidate hours for today (keyed on arena_group_id)
    const { data: todayHours } = await supabase
      .from("beach_day_hourly_scores")
      .select("local_hour, hour_score, tide_status, wind_status, rain_status, crowd_status, temp_status, uv_status")
      .eq("arena_group_id", fid)
      .eq("local_date", today)
      .eq("is_candidate_window", true)
      .gte("local_hour", currentLocalHour)
      .order("local_hour", { ascending: true });

    // Status priority
    const statusRank: Record<string, number> = { go: 1, advisory: 2, caution: 3, no_go: 4 };
    const worstStatus = (a: string | null, b: string | null): string | null => {
      if (!a) return b;
      if (!b) return a;
      return (statusRank[a] ?? 0) >= (statusRank[b] ?? 0) ? a : b;
    };

    // Aggregate future days from is_in_best_window hours
    type DateAgg = {
      sum: number; count: number;
      tide: string | null; wind: string | null; rain: string | null;
      crowd: string | null; temp: string | null; uv: string | null;
    };
    const byDate: Record<string, DateAgg> = {};
    for (const h of futureHours ?? []) {
      if (!byDate[h.local_date]) byDate[h.local_date] = {
        sum: 0, count: 0, tide: null, wind: null, rain: null, crowd: null, temp: null, uv: null,
      };
      const agg = byDate[h.local_date];
      agg.sum   += Number(h.hour_score ?? 0);
      agg.count += 1;
      agg.tide  = worstStatus(agg.tide,  h.tide_status);
      agg.wind  = worstStatus(agg.wind,  h.wind_status);
      agg.rain  = worstStatus(agg.rain,  h.rain_status);
      agg.crowd = worstStatus(agg.crowd, h.crowd_status);
      agg.temp  = worstStatus(agg.temp,  h.temp_status);
      agg.uv    = worstStatus(agg.uv,    h.uv_status);
    }

    // Find best remaining window for today from candidate hours
    const todayWindow = findBestRemainingWindow(todayHours ?? []);

    const daysWithScore = (days ?? []).map(d => {
      const isToday = d.local_date === today;

      if (isToday) {
        if (todayWindow) {
          return {
            ...d,
            best_window_label:  buildWindowLabel(todayWindow.startHour, todayWindow.endHour),
            best_window_status: todayWindow.status,
            composite_score:    Math.round(todayWindow.avgScore),
            metric_statuses:    todayWindow.metricStatuses,
          };
        } else {
          return {
            ...d,
            best_window_label:  "No good window remaining",
            best_window_status: "no_go",
            composite_score:    null,
            metric_statuses:    null,
          };
        }
      }

      const agg = byDate[d.local_date];
      return {
        ...d,
        composite_score: agg ? Math.round(agg.sum / agg.count) : null,
        metric_statuses: agg ? {
          tide: agg.tide, wind: agg.wind, rain: agg.rain,
          crowd: agg.crowd, temp: agg.temp, uv: agg.uv,
        } : null,
      };
    });

    // Fetch all active beaches for the location switcher dropdown.
    // All on the spine now — no JOIN needed.
    const { data: goldList } = await supabase
      .from("beaches_gold")
      .select("fid, location_id, name, display_name_override")
      .eq("is_active", true)
      .order("name");
    const allBeaches = (goldList ?? []).map(g => ({
      location_id:    g.location_id ?? null,
      arena_group_id: g.fid,
      display_name:   g.display_name_override ?? g.name,
    }));

    return json({ beach, days: daysWithScore, allBeaches: allBeaches ?? [] });

  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});

function formatHour(hour: number): string {
  if (hour === 0 || hour === 24) return "12am";
  if (hour === 12) return "12pm";
  return hour < 12 ? `${hour}am` : `${hour - 12}pm`;
}

function buildWindowLabel(startHour: number, endHour: number): string {
  return `${formatHour(startHour)}–${formatHour(endHour + 1)}`;
}

type HourRow = {
  local_hour: number; hour_score: number;
  tide_status: string | null; wind_status: string | null;
  rain_status: string | null; crowd_status: string | null;
  temp_status: string | null; uv_status: string | null;
};

function findBestRemainingWindow(hours: HourRow[]): {
  startHour: number; endHour: number; avgScore: number; status: string;
  metricStatuses: Record<string, string | null>;
} | null {
  if (!hours.length) return null;

  const sorted    = [...hours].sort((a, b) => a.local_hour - b.local_hour);
  const peak      = sorted.reduce((b, h) => Number(h.hour_score) > Number(b.hour_score) ? h : b);
  const peakScore = Number(peak.hour_score);
  const peakIdx   = sorted.indexOf(peak);

  // Mirror scoring.ts findBestWindow: expand from peak using score threshold
  const STEP = 0.05;
  let threshold = 0.93;
  let window: HourRow[] = [];

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
  const ms = window.reduce((acc, h) => ({
    tide:  worst(acc.tide,  h.tide_status),
    wind:  worst(acc.wind,  h.wind_status),
    rain:  worst(acc.rain,  h.rain_status),
    crowd: worst(acc.crowd, h.crowd_status),
    temp:  worst(acc.temp,  h.temp_status),
    uv:    worst(acc.uv,    h.uv_status),
  }), { tide: null, wind: null, rain: null, crowd: null, temp: null, uv: null } as Record<string, string | null>);

  const avgScore      = window.reduce((s, h) => s + Number(h.hour_score), 0) / window.length;
  const overallStatus = Object.values(ms).reduce(worst, null) ?? "go";

  return {
    startHour:      window[0].local_hour,
    endHour:        window[window.length - 1].local_hour,
    avgScore,
    status:         overallStatus,
    metricStatuses: ms,
  };
}
