// get-beach-detail/index.ts
// Serves hourly scores + daily recommendation for the detail drawer.
// 
// GET /functions/v1/get-beach-detail?location_id=huntington-dog-beach&date=2026-04-09 
// Returns: { beach, day: DayRecommendation, hours: HourlyScore[] } 

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
    const url        = new URL(req.url);
    const locationId = url.searchParams.get("location_id") ?? "huntington-dog-beach";
    const date       = url.searchParams.get("date");

    if (!date) {
      return json({ error: "date parameter required (YYYY-MM-DD)" }, 400);
    }

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
    const nowUtc   = new Date();

    // Beach metadata
    const { data: beach, error: beachErr } = await supabase
      .from("beaches")
      .select("location_id, display_name, timezone, address, website, arena_group_id")
      .eq("location_id", locationId)
      .single();

    if (beachErr || !beach) {
      return json({ error: "Beach not found" }, 404);
    }

    // Derive today's local date + hour from beach timezone (never use UTC date)
    const localParts = new Intl.DateTimeFormat("en-US", {
      timeZone: beach.timezone,
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", hour12: false,
    }).formatToParts(nowUtc);
    const getPart = (t: string) => localParts.find(p => p.type === t)?.value ?? "";
    const today          = `${getPart("year")}-${getPart("month")}-${getPart("day")}`;
    const isToday        = date === today;
    const currentLocalHour = isToday ? parseInt(getPart("hour")) % 24 : 0;

    // Daily recommendation + hourly scores in parallel
    const [{ data: day, error: dayErr }, { data: hours, error: hoursErr }] = await Promise.all([
      supabase
        .from("beach_day_recommendations")
        .select("*")
        .eq("location_id", locationId)
        .eq("local_date", date)
        .single(),
      supabase
        .from("beach_day_hourly_scores")
        .select(
          "local_hour, hour_label, hour_status, is_in_best_window, is_candidate_window, " +
          "tide_height, wind_speed, temp_air, feels_like, precip_chance, busyness_score, " +
          "uv_index, weather_code, hour_text, is_daylight, hour_score, " +
          "tide_score, wind_score, crowd_score, rain_score, temp_score, uv_score, weather_score, " +
          "tide_status, wind_status, crowd_status, rain_status, temp_status, uv_status, " +
          "temp_cold_status, temp_hot_status, sand_temp, asphalt_temp, sand_status, asphalt_status"
        )
        .eq("location_id", locationId)
        .eq("local_date", date)
        .eq("is_daylight", true)
        .order("local_hour", { ascending: true }),
    ]);

    if (dayErr || !day) return json({ error: "No recommendation found for this date" }, 404);
    if (hoursErr)        return json({ error: hoursErr.message }, 500);

    // LLM-extracted policy/amenity metadata via the arena bridge.
    // Best-effort: if no arena_group_id (e.g., OR beach), or no extraction
    // yet, fields are simply null.
    let metadata: Record<string, unknown> | null = null;
    if (beach.arena_group_id) {
      const { data: meta } = await supabase
        .from("arena_beach_metadata")
        .select(
          "dogs_allowed, dogs_leash_required, dogs_off_leash_area, " +
          "dogs_seasonal_restrictions, dogs_time_restrictions, " +
          "dogs_policy_notes, dogs_allowed_areas, " +
          "hours_text, public_access, access_text, " +
          "parking_type, " +
          "extracted_address, best_address"
        )
        .eq("arena_group_id", beach.arena_group_id)
        .maybeSingle();
      metadata = meta ?? null;
    }

    // For today: find best remaining window and override is_in_best_window + day label
    let finalDay   = day;
    let finalHours = hours ?? [];

    if (isToday) {
      const remaining = finalHours.filter(
        h => h.is_candidate_window && Number(h.local_hour) >= currentLocalHour
      );
      const win = findBestRemainingWindow(remaining);
      const bestHours = new Set(win ? win.hours.map(h => h.local_hour) : []);

      finalHours = finalHours.map(h => ({
        ...h,
        is_in_best_window: bestHours.has(Number(h.local_hour)),
      }));

      finalDay = {
        ...day,
        best_window_label:  win
          ? buildWindowLabel(win.startHour, win.endHour)
          : "No good window remaining",
        best_window_status: win ? win.status : "no_go",
      };
    }

    return json({ beach, day: finalDay, hours: finalHours, metadata });

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

