-- 20260607_find_beaches_current_hour_score.sql
--
-- Extend find_beaches to return the CURRENT-HOUR hour_score_v2 for each
-- beach (using its own timezone), so find.html can show "Score now" on
-- card discs instead of today's peak (composite_score_v2).
--
-- Context: Franz 2026-06-07 — composite_score_v2 is the max hour score
-- within today's best window. The Now bar in beach.html's chart shows
-- the current hour score. They diverged: HI fid 13882 badge said 81
-- (6am peak) while the Now-hour bar showed 76-77. Beach.html switched
-- to current-hour locally (data.hours_today + _currentLocalHour). find
-- needs the same value from the RPC.
--
-- Implementation: LEFT JOIN beach_day_hourly_scores at (location_id,
-- local_date_at_beach_tz, current_hour_at_beach_tz). NULL when:
--   * Beach has no scoring tier (no hourly rows)
--   * Current hour is closed / pre-dawn / post-sunset / outside 6-21
--     window the bulk apply writes
--   * Beach timezone is NULL (defensive; we fall back to America/LA)
--
-- Return-table signature changing → DROP + CREATE.

DROP FUNCTION IF EXISTS public.find_beaches(date, double precision, double precision, text, integer, boolean, integer);

CREATE OR REPLACE FUNCTION public.find_beaches(
  p_date          date,
  p_lat           double precision DEFAULT NULL,
  p_lng           double precision DEFAULT NULL,
  p_leash         text             DEFAULT 'any',
  p_limit         integer          DEFAULT 500,
  p_scored_only   boolean          DEFAULT false,
  p_now_hour      integer          DEFAULT NULL
)
RETURNS TABLE (
  arena_group_id        bigint,
  location_id           text,
  display_name          text,
  latitude              double precision,
  longitude             double precision,
  access_rule           text,
  has_on_leash          boolean,
  has_off_leash         boolean,
  dogs_allowed          text,
  dogs_prohibited_start text,
  location_tier         text,
  distance_m            double precision,
  composite_score_v2    integer,
  current_hour_score    integer,   -- NEW 2026-06-07
  best_window_label     text,
  best_window_status    text,
  bacteria_risk         text,
  summary_weather       text,
  weather_code          integer,
  lowest_tide_height    numeric,
  avg_temp              numeric,
  avg_wind              numeric,
  busyness_category     text,
  go_hours_count        integer,
  avg_tide_height       numeric,
  has_parking           boolean,
  has_restrooms         boolean,
  has_drinking_water    boolean,
  has_disabled_access   boolean,
  has_showers           boolean,
  has_picnic_area       boolean,
  has_food              boolean,
  has_lifeguards        boolean,
  has_fire_pits         boolean
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = 'public'
AS $function$
  WITH
  base AS (
    SELECT
      g.fid AS arena_group_id,
      g.location_id,
      coalesce(g.display_name_override, g.name) AS display_name,
      g.lat::double precision AS latitude,
      g.lon::double precision AS longitude,
      dp.access_rule,
      dp.has_on_leash,
      dp.has_off_leash,
      dp.dogs_allowed,
      dp.dogs_prohibited_start::text AS dogs_prohibited_start,
      public.beach_location_tier(
        dp.dogs_allowed, dp.has_off_leash, dp.has_on_leash,
        dp.dogs_prohibited_start::text
      ) AS location_tier,
      CASE
        WHEN p_lat IS NOT NULL AND p_lng IS NOT NULL
        THEN ST_Distance(g.geom::geography, ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography)
        ELSE NULL
      END AS distance_m,
      dr.composite_score_v2,
      nh.hour_score_v2 AS current_hour_score,   -- NEW
      dr.best_window_label AS stored_best_window_label,
      dr.best_window_status,
      dr.bacteria_risk,
      dr.summary_weather,
      dr.weather_code,
      dr.lowest_tide_height,
      dr.avg_temp,
      dr.avg_wind,
      dr.busyness_category,
      dr.go_hours_count,
      dr.avg_tide_height,
      ba.has_parking,
      ba.has_restrooms,
      ba.has_drinking_water,
      ba.has_disabled_access,
      ba.has_showers,
      ba.has_picnic_area,
      ba.has_food,
      ba.has_lifeguards,
      ba.has_fire_pits,
      g.geom AS geom
    FROM public.beaches_gold g
    LEFT JOIN public.beach_day_recommendations dr
           ON dr.arena_group_id = g.fid
          AND dr.local_date     = p_date
    LEFT JOIN public.beach_dog_policy dp
           ON dp.arena_group_id = g.fid
    LEFT JOIN public.beach_amenities ba
           ON ba.arena_group_id = g.fid
    -- Current-hour score: join to per-hour table at the beach's OWN
    -- timezone (not the caller's). Defensive fallback to America/LA when
    -- bg.timezone is null (legacy beaches without tz set). Keyed on
    -- arena_group_id because beach_day_hourly_scores.location_id
    -- diverged from beaches_gold.location_id during path-3b
    -- (la-jolla-shores-san-diego vs la-jolla-shores) — fid is the
    -- stable identity per HARD rule [[arena-group-id-is-fid-not-group]].
    LEFT JOIN public.beach_day_hourly_scores nh
           ON nh.arena_group_id = g.fid
          AND nh.local_date     = (now() AT TIME ZONE COALESCE(g.timezone, 'America/Los_Angeles'))::date
          AND nh.local_hour     = extract(hour FROM now() AT TIME ZONE COALESCE(g.timezone, 'America/Los_Angeles'))::int
    WHERE g.is_active = true
      AND (p_leash = 'any' OR dp.access_rule = p_leash OR dp.access_rule IS NULL)
      AND (NOT p_scored_only OR dr.composite_score_v2 IS NOT NULL)
  ),

  remaining_islands AS (
    SELECT
      h.arena_group_id,
      h.local_hour,
      h.local_hour - row_number() OVER (
        PARTITION BY h.arena_group_id ORDER BY h.local_hour
      ) AS grp
    FROM public.beach_day_hourly_scores h
    WHERE p_now_hour IS NOT NULL
      AND h.local_date = p_date
      AND h.is_daylight = true
      AND h.is_candidate_window = true
      AND h.local_hour >= p_now_hour
  ),
  remaining_runs AS (
    SELECT
      arena_group_id,
      grp,
      min(local_hour) AS run_start,
      max(local_hour) AS run_end,
      count(*) AS run_len
    FROM remaining_islands
    GROUP BY arena_group_id, grp
  ),
  best_remaining AS (
    SELECT DISTINCT ON (arena_group_id)
      arena_group_id, run_start, run_end
    FROM remaining_runs
    ORDER BY arena_group_id, run_len DESC, run_start ASC
  )

  SELECT
    b.arena_group_id,
    b.location_id,
    b.display_name,
    b.latitude,
    b.longitude,
    b.access_rule,
    b.has_on_leash,
    b.has_off_leash,
    b.dogs_allowed,
    b.dogs_prohibited_start,
    b.location_tier,
    b.distance_m,
    b.composite_score_v2,
    b.current_hour_score,
    CASE
      WHEN p_now_hour IS NOT NULL THEN
        CASE
          WHEN br.run_start IS NOT NULL
            THEN public._format_hour_window(br.run_start::int, br.run_end::int)
          ELSE 'No good window remaining'
        END
      ELSE b.stored_best_window_label
    END AS best_window_label,
    b.best_window_status,
    b.bacteria_risk,
    b.summary_weather,
    b.weather_code,
    b.lowest_tide_height,
    b.avg_temp,
    b.avg_wind,
    b.busyness_category,
    b.go_hours_count,
    b.avg_tide_height,
    b.has_parking,
    b.has_restrooms,
    b.has_drinking_water,
    b.has_disabled_access,
    b.has_showers,
    b.has_picnic_area,
    b.has_food,
    b.has_lifeguards,
    b.has_fire_pits
  FROM base b
  LEFT JOIN best_remaining br ON br.arena_group_id = b.arena_group_id
  ORDER BY
    CASE
      WHEN p_lat IS NOT NULL AND p_lng IS NOT NULL
      THEN b.geom::geography <-> ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography
      ELSE NULL
    END NULLS LAST,
    CASE
      WHEN p_lat IS NULL AND p_lng IS NULL
      THEN b.composite_score_v2
      ELSE NULL
    END DESC NULLS LAST
  LIMIT COALESCE(NULLIF(p_limit, 0), 500);
$function$;

GRANT EXECUTE ON FUNCTION public.find_beaches(
  date, double precision, double precision, text, integer, boolean, integer
) TO anon, authenticated;
