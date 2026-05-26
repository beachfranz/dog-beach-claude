-- Extend nearest_dog_parks RPC to return REAL composite_score + day fields
-- per date, matching the shape get-beaches-find returns. This kills the
-- 60/75 placeholder score in find.html that was steamrolling beaches on
-- TDY and losing to them on every other day.
--
-- New parameter: p_date (defaults to CURRENT_DATE).
-- New returned columns:
--   composite_score   numeric  — AVG(hour_score) across the day's hourly rows
--   day_status        text     — from dog_park_day_recommendations
--   best_window_label text
--   best_window_status text
--   go_hours_count    int
--   summary_weather   text
--   weather_code      int
--   avg_temp          numeric
--   avg_wind          numeric

CREATE OR REPLACE FUNCTION public.nearest_dog_parks(
  p_lat   double precision DEFAULT NULL,
  p_lng   double precision DEFAULT NULL,
  p_limit integer DEFAULT 10,
  p_state text DEFAULT NULL,
  p_date  date DEFAULT NULL
) RETURNS TABLE(
  fid bigint,
  name text,
  state text,
  address_city text,
  lat double precision,
  lon double precision,
  distance_miles double precision,
  has_fence boolean,
  has_drinking_water boolean,
  surface text,
  leash_policy text,
  hours_text text,
  hours_open_time text,
  hours_close_time text,
  additional_rules text,
  source text,
  source_url text,
  consensus_confidence numeric,
  operator_id bigint,
  operator_name text,
  operator_short text,
  composite_score numeric,
  day_status text,
  best_window_label text,
  best_window_status text,
  go_hours_count integer,
  summary_weather text,
  weather_code integer,
  avg_temp numeric,
  avg_wind numeric
)
LANGUAGE sql STABLE PARALLEL SAFE
AS $function$
  WITH user_pt AS (
    SELECT CASE WHEN p_lat IS NOT NULL AND p_lng IS NOT NULL
                THEN ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography
                ELSE NULL::geography END AS pt,
           COALESCE(p_date, CURRENT_DATE) AS d
  )
  SELECT
    g.fid, g.name, g.state, g.address_city, g.lat, g.lon,
    CASE WHEN up.pt IS NOT NULL
         THEN ST_Distance(up.pt, g.geom::geography) / 1609.344
         ELSE NULL::double precision
    END AS distance_miles,
    g.has_fence, g.has_drinking_water, g.surface,
    p.leash_policy, p.hours_text, p.hours_open_time, p.hours_close_time, p.additional_rules,
    p.source, p.source_url, p.consensus_confidence,
    p.operator_id, o.name AS operator_name, o.short_name AS operator_short,
    hs.composite_score,
    dr.day_status,
    dr.best_window_label,
    dr.best_window_status,
    dr.go_hours_count,
    dr.summary_weather,
    dr.weather_code,
    dr.avg_temp,
    dr.avg_wind
  FROM public.dog_parks_gold g
  JOIN public.dog_park_dog_policy p ON p.dog_park_fid = g.fid
  CROSS JOIN user_pt up
  LEFT JOIN public.operator o ON o.id = p.operator_id
  LEFT JOIN public.dog_park_day_recommendations dr
         ON dr.dog_park_fid = g.fid AND dr.local_date = up.d
  LEFT JOIN LATERAL (
    SELECT ROUND(AVG(h.hour_score)::numeric, 0) AS composite_score
      FROM public.dog_park_day_hourly_scores h
     WHERE h.dog_park_fid = g.fid
       AND h.local_date = up.d
       AND h.hour_score IS NOT NULL
  ) hs ON TRUE
  WHERE g.is_active = true
    AND (p_state IS NULL OR g.state = p_state)
  ORDER BY
    CASE WHEN up.pt IS NOT NULL THEN ST_Distance(up.pt, g.geom::geography) END NULLS LAST,
    g.name
  LIMIT GREATEST(COALESCE(p_limit, 10), 1);
$function$;

-- PostgREST needs to refresh its function cache after signature change.
NOTIFY pgrst, 'reload schema';
