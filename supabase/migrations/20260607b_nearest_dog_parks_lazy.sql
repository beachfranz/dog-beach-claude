-- 20260607b_nearest_dog_parks_lazy.sql
--
-- Push compute_dog_park_score_v2 lateral OUT of the all-rows scan and
-- into a scalar subquery on the post-LIMIT result set.
--
-- Pre-fix: 1.4s baseline (EXPLAIN ANALYZE 2026-06-07), tips over the
-- 3s anon timeout under any contention. find.html dog-park results
-- come back 500 with code 57014 (statement_timeout). Surfaced
-- 2026-06-07 evening — Huntington Beach view shows beaches but no dog
-- parks.
--
-- Same shape of fix as 20260607a (find_beaches): compute the expensive
-- per-fid signal only for the rows that survive the spatial ORDER BY
-- + LIMIT, not all 2,603 active dog parks.
--
-- Sibling per HARD rule [[paired-functions-port-fixes-both-sides]].
-- No behavior change on the return shape.

DROP FUNCTION IF EXISTS public.nearest_dog_parks(double precision, double precision, integer, text, date);

CREATE OR REPLACE FUNCTION public.nearest_dog_parks(
  p_lat   double precision DEFAULT NULL,
  p_lng   double precision DEFAULT NULL,
  p_limit integer          DEFAULT 10,
  p_state text             DEFAULT NULL,
  p_date  date             DEFAULT NULL
)
RETURNS TABLE (
  fid                  bigint,
  name                 text,
  state                text,
  address_city         text,
  lat                  double precision,
  lon                  double precision,
  distance_miles       double precision,
  has_fence            boolean,
  has_drinking_water   boolean,
  surface              text,
  leash_policy         text,
  hours_text           text,
  hours_open_time      text,
  hours_close_time     text,
  additional_rules     text,
  source               text,
  source_url           text,
  consensus_confidence numeric,
  operator_id          bigint,
  operator_name        text,
  operator_short       text,
  composite_score      numeric,
  composite_score_v2   integer,
  best_window_label    text,
  best_window_status   text,
  go_hours_count       integer,
  summary_weather      text,
  weather_code         integer,
  avg_temp             numeric,
  avg_wind             numeric,
  score_v2             integer,
  score_v2_components  jsonb,
  drive_minutes        integer,
  has_shade            boolean,
  has_picnic_tables    boolean,
  has_water_play       boolean,
  has_agility          boolean
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $function$
  WITH
  user_pt AS (
    SELECT
      CASE WHEN p_lat IS NOT NULL AND p_lng IS NOT NULL
           THEN ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography
           ELSE NULL::geography END AS pt,
      COALESCE(p_date, CURRENT_DATE) AS d
  ),
  -- Spatial / state filter + distance sort + LIMIT happens BEFORE the
  -- compute_dog_park_score_v2 lateral fires. Was: lateral computed
  -- for all 2,603 active parks → blew the 3s anon timeout.
  ranked AS (
    SELECT
      g.fid, g.name, g.state, g.address_city, g.lat, g.lon, g.geom,
      g.has_fence, g.has_drinking_water, g.surface,
      CASE WHEN up.pt IS NOT NULL
           THEN ST_Distance(up.pt, g.geom::geography) / 1609.344
           ELSE NULL::double precision
      END AS distance_miles,
      up.d AS d
    FROM public.dog_parks_gold g
    CROSS JOIN user_pt up
    WHERE g.is_active = true
      AND (p_state IS NULL OR g.state = p_state)
    ORDER BY
      CASE WHEN up.pt IS NOT NULL THEN ST_Distance(up.pt, g.geom::geography) END NULLS LAST,
      g.name
    LIMIT GREATEST(COALESCE(p_limit, 10), 1)
  )
  SELECT
    r.fid, r.name, r.state, r.address_city, r.lat, r.lon, r.distance_miles,
    r.has_fence, r.has_drinking_water, r.surface,
    p.leash_policy, p.hours_text, p.hours_open_time, p.hours_close_time, p.additional_rules,
    p.source, p.source_url, p.consensus_confidence,
    p.operator_id, o.canonical_name AS operator_name, o.short_name AS operator_short,
    hs.composite_score,
    dr.composite_score_v2,
    dr.best_window_label, dr.best_window_status, dr.go_hours_count,
    dr.summary_weather, dr.weather_code, dr.avg_temp, dr.avg_wind,
    (sv.payload->>'score')::int          AS score_v2,
    sv.payload                            AS score_v2_components,
    (sv.payload->>'drive_minutes')::int  AS drive_minutes,
    p.has_shade,
    p.has_picnic_tables,
    p.has_water_play,
    p.has_agility
  FROM ranked r
  JOIN public.dog_park_dog_policy p ON p.dog_park_fid = r.fid
  LEFT JOIN public.operators o ON o.id = p.operator_id AND o.is_canonical = true
  LEFT JOIN public.dog_park_day_recommendations dr
         ON dr.dog_park_fid = r.fid AND dr.local_date = r.d
  LEFT JOIN LATERAL (
    SELECT round(avg(h.hour_score_v2)::numeric, 0) AS composite_score
      FROM public.dog_park_day_hourly_scores h
     WHERE h.dog_park_fid = r.fid
       AND h.local_date   = r.d
       AND h.hour_score_v2 IS NOT NULL
  ) hs ON true
  LEFT JOIN LATERAL (
    SELECT public.compute_dog_park_score_v2(r.fid, p_lat, p_lng, r.d) AS payload
  ) sv ON true;
$function$;

GRANT EXECUTE ON FUNCTION public.nearest_dog_parks(
  double precision, double precision, integer, text, date
) TO anon, authenticated;
