-- find_dog_parks RPC: v3 cutover with v2 fallback.
--
-- Sibling of 20260615i for the dog-park side (per
-- [[paired-functions-port-fixes-both-sides]]). Two v2 references
-- swapped to COALESCE(_v3, _v2):
--   * composite_score_v2 SELECT (returned column)
--   * scored_only filter
--
-- find_dog_parks has no v3-style ORDER BY (sorts by distance), so no
-- sort-key swap needed.

BEGIN;

CREATE OR REPLACE FUNCTION public.find_dog_parks(
  p_date date,
  p_lat double precision DEFAULT NULL,
  p_lng double precision DEFAULT NULL,
  p_leash text DEFAULT 'any',
  p_limit integer DEFAULT NULL,
  p_scored_only boolean DEFAULT false,
  p_surface text DEFAULT NULL,
  p_fence boolean DEFAULT NULL,
  p_water boolean DEFAULT NULL
)
RETURNS TABLE(
  dog_park_fid bigint, name text, display_name text,
  latitude double precision, longitude double precision,
  surface text, has_fence boolean, has_drinking_water boolean,
  double_gate boolean, small_dog_area boolean, large_dog_area boolean,
  lighting boolean, leash_policy text, off_leash_flag boolean,
  hours_text text, hours_open_time text, hours_close_time text,
  additional_rules text, description text, source text, source_url text,
  operator_id bigint, operator_short text, distance_m double precision,
  composite_score_v2 integer, best_window_label text, best_window_status text,
  summary_weather text, weather_code integer,
  avg_temp numeric, avg_wind numeric, avg_uv numeric,
  go_hours_count integer, composite_score numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path TO 'public'
AS $function$
  SELECT
    g.fid AS dog_park_fid, g.name,
    coalesce(g.display_name_override, g.name) AS display_name,
    g.lat AS latitude, g.lon AS longitude,
    coalesce(dp.surface_overlay, g.surface) AS surface,
    coalesce(dp.has_fence, g.has_fence) AS has_fence,
    coalesce(dp.has_drinking_water, g.has_drinking_water) AS has_drinking_water,
    dp.double_gate, dp.small_dog_area, dp.large_dog_area, dp.lighting,
    coalesce(dp.leash_policy, 'off_leash') AS leash_policy,
    coalesce(dp.off_leash_flag, true) AS off_leash_flag,
    dp.hours_text, dp.hours_open_time, dp.hours_close_time,
    dp.additional_rules,
    coalesce(dp.description_overlay, g.description) AS description,
    dp.source, dp.source_url, dp.operator_id,
    op.canonical_name AS operator_short,
    CASE
      WHEN p_lat IS NOT NULL AND p_lng IS NOT NULL
      THEN ST_Distance(g.geom::geography, ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography)
      ELSE NULL
    END AS distance_m,
    -- v3 cutover: prefer v3, fall back to v2.
    COALESCE(dr.composite_score_v3::int, dr.composite_score_v2) AS composite_score_v2,
    dr.best_window_label, dr.best_window_status,
    dr.summary_weather, dr.weather_code, dr.avg_temp, dr.avg_wind,
    dr.avg_uv, dr.go_hours_count,
    CASE
      WHEN dr.go_hours_count IS NULL THEN NULL
      ELSE round(
        100.0 * dr.go_hours_count
        / nullif(dr.go_hours_count + dr.advisory_hours_count
               + dr.caution_hours_count + dr.no_go_hours_count, 0),
        0
      )::numeric
    END AS composite_score
  FROM public.dog_parks_gold g
  LEFT JOIN public.dog_park_dog_policy dp ON dp.dog_park_fid = g.fid
  LEFT JOIN public.operators op ON op.id = dp.operator_id AND op.is_canonical = true
  LEFT JOIN public.dog_park_day_recommendations dr
         ON dr.dog_park_fid = g.fid AND dr.local_date = p_date
  WHERE g.is_active = true AND g.is_scoreable = true
    AND (
      p_leash = 'any'
      OR (p_leash = 'off' AND coalesce(dp.off_leash_flag, true) IS TRUE)
      OR (p_leash = 'on'  AND dp.leash_policy = 'on_leash')
    )
    AND (p_surface IS NULL OR coalesce(dp.surface_overlay, g.surface) = p_surface)
    AND (p_fence   IS NULL OR coalesce(dp.has_fence,          g.has_fence)          = p_fence)
    AND (p_water   IS NULL OR coalesce(dp.has_drinking_water, g.has_drinking_water) = p_water)
    -- v3 cutover: scored gate accepts either column.
    AND (NOT p_scored_only OR COALESCE(dr.composite_score_v3, dr.composite_score_v2) IS NOT NULL)
  ORDER BY
    CASE
      WHEN p_lat IS NOT NULL AND p_lng IS NOT NULL
      THEN g.geom::geography <-> ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography
      ELSE NULL
    END NULLS LAST
  LIMIT CASE WHEN p_limit IS NOT NULL AND p_limit > 0 THEN p_limit ELSE NULL END;
$function$;

NOTIFY pgrst, 'reload schema';

COMMIT;
