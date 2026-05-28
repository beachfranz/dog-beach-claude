// get-dog-park-summary/index.ts
// 7-day rollup for one dog park. Sibling to get-beach-summary.
//
// GET ?fid=<n>  →  { dog_park: {...}, days: [7 day_recommendations rows] }

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
    if (!fidParam) return json({ error: "fid required" }, 400);
    const fid = parseInt(fidParam, 10);
    if (!Number.isFinite(fid)) return json({ error: "fid must be a number" }, 400);

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
    const todayPacific = new Intl.DateTimeFormat("en-CA", {
      timeZone: "America/Los_Angeles",
      year: "numeric", month: "2-digit", day: "2-digit",
    }).format(new Date());

    const [parkRes, daysRes, nowRes] = await Promise.all([
      supabase
        .from("dog_parks_gold")
        .select(`
          fid, name, display_name_override, lat, lon, state,
          county_name, address_street, address_city, address_zip,
          surface, has_fence, has_drinking_water, area_m2,
          website, description,
          dog_park_dog_policy(
            dogs_allowed, leash_policy, off_leash_flag,
            hours_open_time, hours_close_time, hours_text,
            additional_rules, double_gate, small_dog_area, large_dog_area, lighting,
            has_fence, has_drinking_water,
            has_shade, has_agility, has_water_play, has_picnic_tables,
            surface_overlay, description_overlay,
            source, source_url, operator_id, consensus_confidence
          ),
          operator:inferred_operator_id(id, name, web_url)
        `)
        .eq("fid", fid)
        .single(),
      supabase
        .from("dog_park_day_recommendations")
        .select("*")
        .eq("dog_park_fid", fid)
        .gte("local_date", todayPacific)
        .order("local_date", { ascending: true })
        .limit(7),
      supabase
        .from("dog_park_day_hourly_scores")
        .select("*")
        .eq("dog_park_fid", fid)
        .eq("is_now", true)
        .maybeSingle(),
    ]);

    if (parkRes.error) return json({ error: `park: ${parkRes.error.message}` }, 500);
    if (!parkRes.data) return json({ error: "dog park not found" }, 404);
    if (daysRes.error) return json({ error: `days: ${daysRes.error.message}` }, 500);

    const park: any = parkRes.data;
    const dp = Array.isArray(park.dog_park_dog_policy) ? park.dog_park_dog_policy[0] : park.dog_park_dog_policy;
    const op = Array.isArray(park.operator) ? park.operator[0] : park.operator;

    return json({
      dog_park: {
        fid: park.fid,
        name: park.name,
        display_name: park.display_name_override ?? park.name,
        latitude: park.lat,
        longitude: park.lon,
        state: park.state,
        county_name: park.county_name,
        address: [park.address_street, park.address_city, park.address_state, park.address_zip]
          .filter(Boolean).join(", "),
        // Identity (raw OSM/CPAD)
        surface: park.surface,
        has_fence: park.has_fence,
        has_drinking_water: park.has_drinking_water,
        area_m2: park.area_m2,
        website: park.website,
        description: park.description,
        operator: op ? { id: op.id, name: op.name, web_url: op.web_url } : null,
        // Policy overlay — consumer-side coalesces overlay ?? raw
        policy: dp ? {
          dogs_allowed: dp.dogs_allowed,
          leash_policy: dp.leash_policy,
          off_leash_flag: dp.off_leash_flag,
          hours_open_time: dp.hours_open_time,
          hours_close_time: dp.hours_close_time,
          hours_text: dp.hours_text,
          additional_rules: dp.additional_rules,
          double_gate: dp.double_gate,
          small_dog_area: dp.small_dog_area,
          large_dog_area: dp.large_dog_area,
          lighting: dp.lighting,
          has_fence: dp.has_fence,                 // overlay; null if not extracted
          has_drinking_water: dp.has_drinking_water,
          has_shade: dp.has_shade,
          has_agility: dp.has_agility,
          has_water_play: dp.has_water_play,
          has_picnic_tables: dp.has_picnic_tables,
          surface_overlay: dp.surface_overlay,
          description_overlay: dp.description_overlay,
          source: dp.source,
          source_url: dp.source_url,
          consensus_confidence: dp.consensus_confidence,
        } : null,
      },
      days: daysRes.data ?? [],
      now: nowRes.data ?? null,
    });
  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});
