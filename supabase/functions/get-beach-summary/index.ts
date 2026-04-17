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
    const url        = new URL(req.url);
    const locationId = url.searchParams.get("location_id") ?? "huntington-dog-beach";

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
    const today    = new Date().toISOString().slice(0, 10);

    // Fetch beach metadata
    const { data: beach, error: beachErr } = await supabase
      .from("beaches")
      .select("location_id, display_name, timezone, address, website")
      .eq("location_id", locationId)
      .single();

    if (beachErr || !beach) {
      return json({ error: "Beach not found" }, 404);
    }

    // Fetch 7 days of recommendations starting today
    const { data: days, error: daysErr } = await supabase
      .from("beach_day_recommendations")
      .select("*")
      .eq("location_id", locationId)
      .gte("local_date", today)
      .order("local_date", { ascending: true })
      .limit(7);

    if (daysErr) {
      return json({ error: daysErr.message }, 500);
    }

    // Fetch best-window hourly scores for the next 7 days to compute composite scores + metric statuses
    const dates = (days ?? []).map(d => d.local_date);
    const { data: hours } = dates.length ? await supabase
      .from("beach_day_hourly_scores")
      .select("local_date, hour_score, tide_status, wind_status, rain_status, crowd_status, temp_status, uv_status")
      .eq("location_id", locationId)
      .in("local_date", dates)
      .eq("is_in_best_window", true) : { data: [] };

    // Status priority for worst-case computation
    const statusRank: Record<string, number> = { go: 1, caution: 2, no_go: 3 };
    const worstStatus = (a: string | null, b: string | null): string | null => {
      if (!a) return b;
      if (!b) return a;
      return (statusRank[a] ?? 0) >= (statusRank[b] ?? 0) ? a : b;
    };

    // Average hour_score + worst metric status per date
    type DateAgg = {
      sum: number; count: number;
      tide: string | null; wind: string | null; rain: string | null;
      crowd: string | null; temp: string | null; uv: string | null;
    };
    const byDate: Record<string, DateAgg> = {};
    for (const h of hours ?? []) {
      if (!byDate[h.local_date]) byDate[h.local_date] = { sum: 0, count: 0, tide: null, wind: null, rain: null, crowd: null, temp: null, uv: null };
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
    const daysWithScore = (days ?? []).map(d => {
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

    // Fetch all active beaches for location switcher
    const { data: allBeaches } = await supabase
      .from("beaches")
      .select("location_id, display_name")
      .eq("is_active", true)
      .order("display_name");

    return json({ beach, days: daysWithScore, allBeaches: allBeaches ?? [] });

  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});
