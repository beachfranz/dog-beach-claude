// get-beach-compare/index.ts
// Returns all active beaches ranked by score for a given date.
// GET ?date=2026-04-15
// Returns: { date, beaches: RankedBeach[] }
//
// Path 3b-3: spine swap — reads from beaches_gold (758+ rows) instead
// of public.beaches (14 rows). Beaches without scoring data render as
// "no_data" cards on the frontend; the catalog is the source of truth
// for what exists.

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
  const date = url.searchParams.get("date") ?? new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Los_Angeles",
    year: "numeric", month: "2-digit", day: "2-digit",
  }).format(new Date());

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

  // All active beaches from the catalog spine (beaches_gold).
  const { data: beaches, error: beachErr } = await supabase
    .from("beaches_gold")
    .select("fid, name, display_name_override, lat, lon")
    .eq("is_active", true)
    .order("name");

  if (beachErr || !beaches) return json({ error: "Failed to load beaches" }, 500);

  // Legacy location_id slugs for the small set of beaches that still have
  // a public.beaches row — kept on the response for BC with old frontend
  // bookmarks. Goes away when public.beaches is dropped.
  const fids = beaches.map(b => b.fid);
  const { data: legacyRows } = await supabase
    .from("beaches")
    .select("arena_group_id, location_id")
    .in("arena_group_id", fids);
  const legacySlug: Record<number, string> = {};
  for (const r of legacyRows ?? []) {
    if (r.arena_group_id) legacySlug[r.arena_group_id] = r.location_id;
  }

  // Day recommendations for these beaches on this date — keyed on arena_group_id
  const { data: days, error: daysErr } = await supabase
    .from("beach_day_recommendations")
    .select("arena_group_id, day_status, best_window_label, go_hours_count, avg_wind, avg_tide_height, busyness_category, summary_weather")
    .in("arena_group_id", fids)
    .eq("local_date", date);

  if (daysErr) return json({ error: daysErr.message }, 500);

  // Hourly scores for best window hours — keyed on arena_group_id
  const { data: hours, error: hoursErr } = await supabase
    .from("beach_day_hourly_scores")
    .select("arena_group_id, hour_score, explainability, hour_status")
    .in("arena_group_id", fids)
    .eq("local_date", date)
    .eq("is_in_best_window", true);

  if (hoursErr) return json({ error: hoursErr.message }, 500);

  // Build per-beach averaged component scores from best window hours
  const scoresByFid: Record<number, {
    hours: number; tide: number; wind: number; crowd: number; rain: number; temp: number; composite: number;
  }> = {};

  for (const h of hours ?? []) {
    const fid = h.arena_group_id as number;
    if (!fid) continue;
    const ex = h.explainability as Record<string, number> ?? {};
    if (!scoresByFid[fid]) {
      scoresByFid[fid] = { hours: 0, tide: 0, wind: 0, crowd: 0, rain: 0, temp: 0, composite: 0 };
    }
    const s = scoresByFid[fid];
    s.hours++;
    s.tide      += ex.tide_score  ?? 0;
    s.wind      += ex.wind_score  ?? 0;
    s.crowd     += ex.crowd_score ?? 0;
    s.rain      += ex.rain_score  ?? 0;
    s.temp      += ex.temp_score  ?? 0;
    s.composite += Number(h.hour_score ?? 0);
  }

  // Average them
  for (const fid in scoresByFid) {
    const s = scoresByFid[fid];
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
  const dayMap = Object.fromEntries((days ?? []).map(d => [d.arena_group_id, d]));

  const ranked = beaches.map(beach => {
    const day    = dayMap[beach.fid];
    const scores = scoresByFid[beach.fid];
    return {
      location_id:       legacySlug[beach.fid] ?? null,
      arena_group_id:    beach.fid,
      display_name:      beach.display_name_override ?? beach.name,
      latitude:          beach.lat,
      longitude:         beach.lon,
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

