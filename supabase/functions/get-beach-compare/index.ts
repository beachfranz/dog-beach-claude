// get-beach-compare/index.ts
// Returns all active beaches ranked by score for a given date.
// GET ?date=2026-04-15
// Returns: { date, beaches: RankedBeach[] }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL         = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "GET, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });

  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });

  const url  = new URL(req.url);
  const date = url.searchParams.get("date") ?? new Date().toISOString().slice(0, 10);

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // All active beaches
  const { data: beaches, error: beachErr } = await supabase
    .from("beaches")
    .select("location_id, display_name")
    .eq("is_active", true)
    .order("display_name");

  if (beachErr || !beaches) return json({ error: "Failed to load beaches" }, 500);

  // Day recommendations for all beaches on this date
  const { data: days, error: daysErr } = await supabase
    .from("beach_day_recommendations")
    .select("location_id, day_status, best_window_label, go_hours_count, avg_wind, avg_tide_height, busyness_category, summary_weather")
    .in("location_id", beaches.map(b => b.location_id))
    .eq("local_date", date);

  if (daysErr) return json({ error: daysErr.message }, 500);

  // Hourly scores for best window hours only — for component score averaging
  const { data: hours, error: hoursErr } = await supabase
    .from("beach_day_hourly_scores")
    .select("location_id, hour_score, explainability, hour_status")
    .in("location_id", beaches.map(b => b.location_id))
    .eq("local_date", date)
    .eq("is_in_best_window", true);

  if (hoursErr) return json({ error: hoursErr.message }, 500);

  // Build per-beach averaged component scores from best window hours
  const scoresByLocation: Record<string, {
    hours: number; tide: number; wind: number; crowd: number; rain: number; temp: number; composite: number;
  }> = {};

  for (const h of hours ?? []) {
    const ex = h.explainability as Record<string, number> ?? {};
    if (!scoresByLocation[h.location_id]) {
      scoresByLocation[h.location_id] = { hours: 0, tide: 0, wind: 0, crowd: 0, rain: 0, temp: 0, composite: 0 };
    }
    const s = scoresByLocation[h.location_id];
    s.hours++;
    s.tide      += ex.tide_score  ?? 0;
    s.wind      += ex.wind_score  ?? 0;
    s.crowd     += ex.crowd_score ?? 0;
    s.rain      += ex.rain_score  ?? 0;
    s.temp      += ex.temp_score  ?? 0;
    s.composite += Number(h.hour_score ?? 0);
  }

  // Average them
  for (const loc in scoresByLocation) {
    const s = scoresByLocation[loc];
    if (s.hours > 0) {
      s.tide      = s.tide      / s.hours;
      s.wind      = s.wind      / s.hours;
      s.crowd     = s.crowd     / s.hours;
      s.rain      = s.rain      / s.hours;
      s.temp      = s.temp      / s.hours;
      s.composite = s.composite / s.hours;
    }
  }

  // Assemble ranked results
  const dayMap = Object.fromEntries((days ?? []).map(d => [d.location_id, d]));

  const ranked = beaches.map(beach => {
    const day    = dayMap[beach.location_id];
    const scores = scoresByLocation[beach.location_id];
    return {
      location_id:       beach.location_id,
      display_name:      beach.display_name,
      day_status:        day?.day_status ?? "no_data",
      best_window_label: day?.best_window_label ?? null,
      go_hours_count:    day?.go_hours_count ?? 0,
      busyness_category: day?.busyness_category ?? null,
      summary_weather:   day?.summary_weather ?? null,
      avg_wind:          day?.avg_wind ?? null,
      avg_tide_height:   day?.avg_tide_height ?? null,
      composite_score:   scores ? Math.round(scores.composite) : 0,
      tide_score:        scores ? scores.tide  : null,
      wind_score:        scores ? scores.wind  : null,
      crowd_score:       scores ? scores.crowd : null,
      rain_score:        scores ? scores.rain  : null,
      temp_score:        scores ? scores.temp  : null,
    };
  }).sort((a, b) => b.composite_score - a.composite_score);

  return json({ date, beaches: ranked });
});

