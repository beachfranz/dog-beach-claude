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

    // Beach metadata
    const { data: beach, error: beachErr } = await supabase
      .from("beaches")
      .select("location_id, display_name, timezone, address, website")
      .eq("location_id", locationId)
      .single();

    if (beachErr || !beach) {
      return json({ error: "Beach not found" }, 404);
    }

    // Daily recommendation (narrative + summary)
    const { data: day, error: dayErr } = await supabase
      .from("beach_day_recommendations")
      .select("*")
      .eq("location_id", locationId)
      .eq("local_date", date)
      .single();

    if (dayErr || !day) {
      return json({ error: "No recommendation found for this date" }, 404);
    }

    // Hourly scores for the day — only daylight hours, ordered by local_hour
    const { data: hours, error: hoursErr } = await supabase
      .from("beach_day_hourly_scores")
      .select(
        "local_hour, hour_label, hour_status, is_in_best_window, " +
        "tide_height, wind_speed, temp_air, feels_like, precip_chance, busyness_score, " +
        "uv_index, weather_code, hour_text, is_daylight, hour_score, " +
        "tide_score, wind_score, crowd_score, rain_score, temp_score, uv_score, " +
        "tide_status, wind_status, crowd_status, rain_status, temp_status, uv_status, " +
        "temp_cold_status, temp_hot_status, sand_temp, asphalt_temp, sand_status, asphalt_status"
      )
      .eq("location_id", locationId)
      .eq("local_date", date)
      .eq("is_daylight", true)
      .order("local_hour", { ascending: true });

    if (hoursErr) {
      return json({ error: hoursErr.message }, 500);
    }

    return json({ beach, day, hours: hours ?? [] });

  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});

