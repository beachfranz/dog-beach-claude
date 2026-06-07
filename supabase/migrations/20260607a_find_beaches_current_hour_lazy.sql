-- 20260607a_find_beaches_current_hour_lazy.sql
--
-- HOTFIX over 20260607_find_beaches_current_hour_score.sql.
--
-- The prior migration LEFT JOIN'd beach_day_hourly_scores in the base CTE
-- to compute current_hour_score. That JOIN expanded for ALL ~3,700 active
-- beaches BEFORE the spatial KNN ORDER BY + LIMIT — pulling ~3,700 × 24h
-- rows per request. Execution time: 10.6s. Anon role statement_timeout
-- is 3s → every find_beaches call from find.html now 500s with code
-- 57014 ("canceling statement due to statement timeout"). Surfaced
-- 2026-06-07 evening at Huntington Beach.
--
-- Fix: lift the bdhs lookup OUT of the base CTE and into a scalar
-- subquery on the post-LIMIT result set. Only ~14 lookups fire per call
-- (the default NEAREST_LIMIT from find.html), each filtered by
-- arena_group_id (hits the existing single-column index) and a small
-- per-beach × current-date row set. Sub-second.
--
-- No behavior change for the consumer surface — current_hour_score is
-- still the same per-row value the prior JOIN would have produced. Only
-- the query shape changed.

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
  current_hour_score    integer,
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
      g.timezone,                     -- exposed for the lazy scalar subquery
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
    -- NB: NO join to beach_day_hourly_scores here — see scalar subquery
    -- below for the lazy per-row lookup on the post-LIMIT result.
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
  ),

  -- Pull the ORDER BY + LIMIT before computing the per-row lookup,
  -- so the bdhs scalar subquery below only fires for the rows that
  -- will actually be returned.
  ranked AS (
    SELECT
      b.*,
      br.run_start AS rr_start,
      br.run_end   AS rr_end
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
    LIMIT COALESCE(NULLIF(p_limit, 0), 500)
  )

  SELECT
    r.arena_group_id,
    r.location_id,
    r.display_name,
    r.latitude,
    r.longitude,
    r.access_rule,
    r.has_on_leash,
    r.has_off_leash,
    r.dogs_allowed,
    r.dogs_prohibited_start,
    r.location_tier,
    r.distance_m,
    r.composite_score_v2,
    (
      SELECT nh.hour_score_v2
      FROM public.beach_day_hourly_scores nh
      WHERE nh.arena_group_id = r.arena_group_id
        AND nh.local_date     = (now() AT TIME ZONE COALESCE(r.timezone, 'America/Los_Angeles'))::date
        AND nh.local_hour     = extract(hour FROM now() AT TIME ZONE COALESCE(r.timezone, 'America/Los_Angeles'))::int
      LIMIT 1
    ) AS current_hour_score,
    CASE
      WHEN p_now_hour IS NOT NULL THEN
        CASE
          WHEN r.rr_start IS NOT NULL
            THEN public._format_hour_window(r.rr_start::int, r.rr_end::int)
          ELSE 'No good window remaining'
        END
      ELSE r.stored_best_window_label
    END AS best_window_label,
    r.best_window_status,
    r.bacteria_risk,
    r.summary_weather,
    r.weather_code,
    r.lowest_tide_height,
    r.avg_temp,
    r.avg_wind,
    r.busyness_category,
    r.go_hours_count,
    r.avg_tide_height,
    r.has_parking,
    r.has_restrooms,
    r.has_drinking_water,
    r.has_disabled_access,
    r.has_showers,
    r.has_picnic_area,
    r.has_food,
    r.has_lifeguards,
    r.has_fire_pits
  FROM ranked r;
$function$;

GRANT EXECUTE ON FUNCTION public.find_beaches(
  date, double precision, double precision, text, integer, boolean, integer
) TO anon, authenticated;
