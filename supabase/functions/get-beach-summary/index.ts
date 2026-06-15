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

    // Query 1: best-window hours for future days (keyed on arena_group_id).
    // Raw-values-only SELECT after Franz 2026-05-30 task #12 — v1 *_status
    // columns dropped. Per-metric worst-status computed below via the
    // inline v2 port. hour_score_v2 powers the composite.
    const { data: futureHours } = futureDates.length ? await supabase
      .from("beach_day_hourly_scores")
      .select("local_date, local_hour, hour_score_v2, hour_score_v3, tide_height, wind_speed, precip_chance, busyness_score, feels_like, uv_index, sand_temp, asphalt_temp")
      .eq("arena_group_id", fid)
      .in("local_date", futureDates)
      .eq("is_in_best_window", true) : { data: [] };

    // Query 2: remaining candidate hours for today (keyed on arena_group_id)
    const { data: todayHours } = await supabase
      .from("beach_day_hourly_scores")
      .select("local_hour, hour_score_v2, hour_score_v3, tide_height, wind_speed, precip_chance, busyness_score, feels_like, uv_index, sand_temp, asphalt_temp")
      .eq("arena_group_id", fid)
      .eq("local_date", today)
      .eq("is_candidate_window", true)
      .gte("local_hour", currentLocalHour)
      .order("local_hour", { ascending: true });

    // v2 status priority (clear < advisory < caution < no_go)
    const statusRank: Record<string, number> = { clear: 0, advisory: 1, caution: 2, no_go: 3 };
    const worstStatus = (a: string | null, b: string | null): string | null => {
      if (!a) return b;
      if (!b) return a;
      return (statusRank[a] ?? 0) >= (statusRank[b] ?? 0) ? a : b;
    };

    // Aggregate future days from is_in_best_window hours. Per-metric
    // status now derived from raw values via the inline v2 port.
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
      agg.sum   += Number(h.hour_score_v3 ?? h.hour_score_v2 ?? 0);
      agg.count += 1;
      const sTemp = v2Worst(
        v2StatusFor("feels_hot",  h.feels_like as number | null),
        v2StatusFor("feels_cold", h.feels_like as number | null),
      );
      agg.tide  = worstStatus(agg.tide,  v2StatusFor("tide",    h.tide_height as number | null));
      agg.wind  = worstStatus(agg.wind,  v2StatusFor("wind",    h.wind_speed as number | null));
      agg.rain  = worstStatus(agg.rain,  v2StatusFor("precip",  h.precip_chance as number | null));
      agg.crowd = worstStatus(agg.crowd, v2StatusFor("crowd",   h.busyness_score as number | null));
      agg.temp  = worstStatus(agg.temp,  sTemp);
      agg.uv    = worstStatus(agg.uv,    v2StatusFor("uv",      h.uv_index as number | null));
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
    //
    // 2026-06-05 LATE: switched to all_active_beaches_for_dropdown RPC
    // (returns a single jsonb-array row) to bypass PostgREST's
    // db-max-rows=1000 cap. The prior `.from("beaches_gold").eq("is_active")`
    // SELECT was truncating to ~27% of the 3,709 active beaches —
    // ~2,709 silently invisible to the dropdown. See 20260605i.
    const { data: dropdownData, error: dropdownErr } = await supabase
      .rpc("all_active_beaches_for_dropdown");
    if (dropdownErr) console.warn(`allBeaches RPC failed: ${dropdownErr.message}`);
    const allBeaches = (dropdownData as Array<{
      arena_group_id: number;
      location_id: string | null;
      display_name: string;
    }>) ?? [];

    // Fire-and-forget page-view log. Feeds the long-tail tier in the
    // daily-refresh strategy: recently-viewed beaches get full nightly
    // refresh, never-viewed ones get on-demand.
    const clientId = req.headers.get("x-client-id");
    supabase.from("beach_views").insert({
      arena_group_id: fid, source: "index",
      client_id: clientId,
    }).then(() => {}).catch(() => {});

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
  local_hour: number; hour_score_v2: number | null; hour_score_v3: number | null;
  tide_height: number | null; wind_speed: number | null;
  precip_chance: number | null; busyness_score: number | null;
  feels_like: number | null; uv_index: number | null;
  sand_temp: number | null; asphalt_temp: number | null;
};

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
function v2Worst(a: V2Status | null, b: V2Status | null): V2Status | null {
  const rank = (s: V2Status | null) =>
    s === "no_go" ? 3 : s === "caution" ? 2 : s === "advisory" ? 1 : s === "clear" ? 0 : -1;
  return rank(a) >= rank(b) ? a : b;
}

function findBestRemainingWindow(hours: HourRow[]): {
  startHour: number; endHour: number; avgScore: number; status: string;
  metricStatuses: Record<string, string | null>;
} | null {
  if (!hours.length) return null;

  // v3-first with v2 fallback (cutover 2026-06-15). Pin
  // [[v3-advisory-driven-scoring]]. Drop v2 fallback once v3 has 100%
  // coverage across all scoring-tier beaches.
  const score = (h: HourRow) => Number(h.hour_score_v3 ?? h.hour_score_v2 ?? 0);

  const sorted    = [...hours].sort((a, b) => a.local_hour - b.local_hour);
  const peak      = sorted.reduce((b, h) => score(h) > score(b) ? h : b);
  const peakScore = score(peak);
  const peakIdx   = sorted.indexOf(peak);

  const STEP = 0.05;
  let threshold = 0.93;
  let window: HourRow[] = [];

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

  const statusRank: Record<string, number> = { clear: 0, advisory: 1, caution: 2, no_go: 3 };
  const worst = (a: string | null, b: string | null) => {
    if (!a) return b; if (!b) return a;
    return (statusRank[a] ?? 0) >= (statusRank[b] ?? 0) ? a : b;
  };
  const ms = window.reduce((acc, h) => ({
    tide:  worst(acc.tide,  v2StatusFor("tide",   h.tide_height)),
    wind:  worst(acc.wind,  v2StatusFor("wind",   h.wind_speed)),
    rain:  worst(acc.rain,  v2StatusFor("precip", h.precip_chance)),
    crowd: worst(acc.crowd, v2StatusFor("crowd",  h.busyness_score)),
    temp:  worst(acc.temp,  v2Worst(v2StatusFor("feels_hot", h.feels_like), v2StatusFor("feels_cold", h.feels_like))),
    uv:    worst(acc.uv,    v2StatusFor("uv",     h.uv_index)),
  }), { tide: null, wind: null, rain: null, crowd: null, temp: null, uv: null } as Record<string, string | null>);

  const avgScore      = window.reduce((s, h) => s + score(h), 0) / window.length;
  const overallStatus = Object.values(ms).reduce(worst, null) ?? "clear";

  return {
    startHour:      window[0].local_hour,
    endHour:        window[window.length - 1].local_hour,
    avgScore,
    status:         overallStatus,
    metricStatuses: ms,
  };
}
