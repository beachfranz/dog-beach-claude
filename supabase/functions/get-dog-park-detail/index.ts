// get-dog-park-detail/index.ts
// Hour-by-hour detail for one dog park on one date. Sibling to get-beach-detail.
//
// GET ?fid=<n>&date=YYYY-MM-DD
//   → { dog_park: {...minimal}, day: {recommendation row}, hours: [...hourly rows] }

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { corsHeaders } from "../_shared/cors.ts";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

Deno.serve(async (req: Request) => {
  const cors = { ...corsHeaders(req, "GET, OPTIONS"), "Content-Type": "application/json" };
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: cors });
  if (req.method === "OPTIONS") return new Response("ok", { headers: cors });

  try {
    const url = new URL(req.url);
    const fidParam = url.searchParams.get("fid");
    const date = url.searchParams.get("date");
    if (!fidParam) return json({ error: "fid required" }, 400);
    if (!date) return json({ error: "date required (YYYY-MM-DD)" }, 400);
    const fid = parseInt(fidParam, 10);
    if (!Number.isFinite(fid)) return json({ error: "fid must be a number" }, 400);

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

    const [parkRes, dayRes, hoursRes] = await Promise.all([
      supabase
        .from("dog_parks_gold")
        .select(`
          fid, name, display_name_override, lat, lon, state, surface,
          dog_park_dog_policy(
            leash_policy, off_leash_flag,
            hours_open_time, hours_close_time, hours_text,
            additional_rules, double_gate, small_dog_area, large_dog_area, lighting,
            has_fence, has_drinking_water,
            surface_overlay, description_overlay,
            source, source_url
          )
        `)
        .eq("fid", fid)
        .single(),
      supabase
        .from("dog_park_day_recommendations")
        .select("*")
        .eq("dog_park_fid", fid)
        .eq("local_date", date)
        .maybeSingle(),
      supabase
        .from("dog_park_day_hourly_scores")
        .select("*")
        .eq("dog_park_fid", fid)
        .eq("local_date", date)
        .order("local_hour", { ascending: true }),
    ]);

    if (parkRes.error) return json({ error: `park: ${parkRes.error.message}` }, 500);
    if (!parkRes.data) return json({ error: "dog park not found" }, 404);
    if (dayRes.error) return json({ error: `day: ${dayRes.error.message}` }, 500);
    if (hoursRes.error) return json({ error: `hours: ${hoursRes.error.message}` }, 500);

    const park: any = parkRes.data;
    const dp = Array.isArray(park.dog_park_dog_policy) ? park.dog_park_dog_policy[0] : park.dog_park_dog_policy;

    return json({
      dog_park: {
        fid: park.fid,
        name: park.name,
        display_name: park.display_name_override ?? park.name,
        latitude: park.lat,
        longitude: park.lon,
        state: park.state,
        surface: park.surface,
        policy: dp ?? null,
      },
      day: dayRes.data ?? null,
      hours: hoursRes.data ?? [],
    });
  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});
