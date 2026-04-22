// get-beaches-nearest/index.ts
// Returns the N active beaches closest to the user's coordinates, each
// with today's day-level recommendation and averaged component scores
// from best-window hourly rows. Distance-ordered.
//
// The bounded-set pattern: server returns a small fixed N (default 10);
// the client filters/sorts within that set for dropdown modes on find.html
// and "show more" on index.html. The GIST index on beaches.location keeps
// this query O(log N) regardless of total beach count.
//
// GET ?lat=<float>&lon=<float>&limit=<int>&date=<YYYY-MM-DD>
//
// Query params:
//   lat, lon (required) — user coordinates
//   limit   (optional)  — default 10, server-capped at 50
//   date    (optional)  — defaults to today (America/Los_Angeles)
//
// Returns: { date, lat, lon, beaches: NearbyBeach[] }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const DEFAULT_LIMIT = 10;
const MAX_LIMIT     = 50;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "GET, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });

  const url = new URL(req.url);
  const latStr = url.searchParams.get("lat");
  const lonStr = url.searchParams.get("lon");
  const lat = latStr === null ? NaN : Number(latStr);
  const lon = lonStr === null ? NaN : Number(lonStr);
  if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180) {
    return json({ error: "Missing or invalid lat/lon query params" }, 400);
  }

  const limitRaw = Number(url.searchParams.get("limit") ?? DEFAULT_LIMIT);
  const limit = Number.isFinite(limitRaw) && limitRaw > 0
    ? Math.min(Math.floor(limitRaw), MAX_LIMIT)
    : DEFAULT_LIMIT;

  const date = url.searchParams.get("date") ?? new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Los_Angeles",
    year: "numeric", month: "2-digit", day: "2-digit",
  }).format(new Date());

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // 1. KNN — pull the N nearest active beaches via the SQL function.
  const { data: nearest, error: nearestErr } = await supabase.rpc("beaches_nearest", {
    p_lat: lat, p_lon: lon, p_limit: limit,
  });
  if (nearestErr) return json({ error: nearestErr.message }, 500);
  if (!nearest || nearest.length === 0) {
    return json({ date, lat, lon, beaches: [] });
  }

  const locationIds = nearest.map((b: { location_id: string }) => b.location_id);

  // 2. Day recommendations for those beaches on `date`.
  const { data: days, error: daysErr } = await supabase
    .from("beach_day_recommendations")
    .select("location_id, day_status, best_window_label, best_window_status, go_hours_count, avg_wind, avg_tide_height, busyness_category, summary_weather")
    .in("location_id", locationIds)
    .eq("local_date", date);
  if (daysErr) return json({ error: daysErr.message }, 500);

  // 3. Best-window hourly rows — used to average component scores, matching
  // the shape get-beach-compare returns.
  const { data: hours, error: hoursErr } = await supabase
    .from("beach_day_hourly_scores")
    .select("location_id, hour_score, explainability, hour_status")
    .in("location_id", locationIds)
    .eq("local_date", date)
    .eq("is_in_best_window", true);
  if (hoursErr) return json({ error: hoursErr.message }, 500);

  const scoresByLocation: Record<string, {
    hours: number; tide: number; wind: number; crowd: number; rain: number; temp: number; composite: number;
  }> = {};

  for (const h of hours ?? []) {
    const ex = h.explainability as Record<string, number> ?? {};
    const s = scoresByLocation[h.location_id] ??= {
      hours: 0, tide: 0, wind: 0, crowd: 0, rain: 0, temp: 0, composite: 0,
    };
    s.hours++;
    s.tide      += ex.tide_score  ?? 0;
    s.wind      += ex.wind_score  ?? 0;
    s.crowd     += ex.crowd_score ?? 0;
    s.rain      += ex.rain_score  ?? 0;
    s.temp      += ex.temp_score  ?? 0;
    s.composite += Number(h.hour_score ?? 0);
  }

  for (const loc in scoresByLocation) {
    const s = scoresByLocation[loc];
    if (s.hours > 0) {
      s.tide      /= s.hours;
      s.wind      /= s.hours;
      s.crowd     /= s.hours;
      s.rain      /= s.hours;
      s.temp      /= s.hours;
      s.composite /= s.hours;
    }
  }

  const dayMap = Object.fromEntries((days ?? []).map(d => [d.location_id, d]));

  // 4. Assemble. Preserve distance-order from step 1.
  const beaches = nearest.map((b: { location_id: string; display_name: string; latitude: number; longitude: number; distance_m: number }) => {
    const day    = dayMap[b.location_id];
    const scores = scoresByLocation[b.location_id];
    return {
      location_id:        b.location_id,
      display_name:       b.display_name,
      latitude:           b.latitude,
      longitude:          b.longitude,
      distance_m:         Math.round(b.distance_m),
      day_status:         day?.day_status ?? "no_data",
      best_window_label:  day?.best_window_label ?? null,
      best_window_status: day?.best_window_status ?? null,
      go_hours_count:     day?.go_hours_count ?? 0,
      busyness_category:  day?.busyness_category ?? null,
      summary_weather:    day?.summary_weather ?? null,
      avg_wind:           day?.avg_wind ?? null,
      avg_tide_height:    day?.avg_tide_height ?? null,
      composite_score:    scores ? Math.round(scores.composite) : 0,
      tide_score:         scores ? scores.tide  : null,
      wind_score:         scores ? scores.wind  : null,
      crowd_score:        scores ? scores.crowd : null,
      rain_score:         scores ? scores.rain  : null,
      temp_score:         scores ? scores.temp  : null,
    };
  });

  return json({ date, lat, lon, limit, beaches });
});
