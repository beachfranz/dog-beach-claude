-- 20260608e — perf fix: pre-aggregate warm tide stations once in picker
--
-- WHY: 20260608a added a tide_warm CTE that called tide_station_is_warm()
-- per row. The function is STABLE and selective, but per-row invocation
-- over ~3,942 eligible beaches inside the PostgREST request path hits the
-- service_role statement_timeout — recurring 500s on beach_refresh_chunked
-- (surfaced by the now-effective reconciler from 20260608d, which is
-- exactly the observability win Option A targets).
--
-- Direct EXPLAIN ANALYZE shows 380ms via CLI, but inside PostgREST the
-- per-row STABLE function calls force a per-tuple scan over
-- tide_grid_hourly. Pre-aggregating warm stations once eliminates the
-- per-row overhead — single GROUP BY scan + IN-list join.
--
-- Same semantics: tide_station_is_warm(s, 24) == ≥20 rows in next 24h.
-- Inline the threshold rather than calling the helper, so the planner
-- can see the predicate against tide_grid_hourly directly.

CREATE OR REPLACE FUNCTION public.beaches_due_for_refresh(
  p_target_fids         bigint[] DEFAULT NULL,
  p_target_location_ids text[]   DEFAULT NULL,
  p_skip_recent_hours   integer  DEFAULT NULL,
  p_limit               integer  DEFAULT NULL
)
RETURNS TABLE(fid bigint, last_generated_at timestamptz)
LANGUAGE sql
STABLE
AS $function$
  WITH eligible AS (
    SELECT bg.fid,
           bg.weather_grid_lat,
           bg.weather_grid_lon,
           bg.noaa_station_id
    FROM public.beaches_gold bg
    WHERE bg.is_active
      AND bg.scoring_tier IN ('daily','hourly')
      AND (p_target_fids IS NULL OR bg.fid = ANY(p_target_fids))
      AND (p_target_location_ids IS NULL
           OR bg.location_id = ANY(p_target_location_ids))
  ),
  grid_warm AS (
    SELECT e.fid
    FROM eligible e
    JOIN public.weather_grid_hourly wgh
      ON wgh.grid_lat = e.weather_grid_lat
     AND wgh.grid_lon = e.weather_grid_lon
     AND wgh.forecast_ts >  now()
     AND wgh.forecast_ts <= now() + interval '24 hours'
    GROUP BY e.fid
    HAVING count(*) >= 22    -- ≥92% of next 24h in this cell
  ),
  warm_tide_stations AS (
    -- Pre-aggregate once: every tide station with ≥20 forecast rows in
    -- next 24h. Single index scan + GROUP BY; cheaper than per-row
    -- function invocation in the tide_warm CTE.
    SELECT station_id
    FROM public.tide_grid_hourly
    WHERE forecast_ts >= now()
      AND forecast_ts <  now() + interval '24 hours'
    GROUP BY station_id
    HAVING count(*) >= 20
  ),
  tide_warm AS (
    SELECT e.fid
    FROM eligible e
    WHERE e.noaa_station_id IS NULL    -- inland: tide-neutral, gate passes
       OR e.noaa_station_id IN (SELECT station_id FROM warm_tide_stations)
  ),
  freshness AS (
    SELECT e.fid, max(r.generated_at) AS last_gen
    FROM eligible e
    LEFT JOIN public.beach_day_recommendations r
      ON r.arena_group_id = e.fid
    GROUP BY e.fid
  )
  SELECT f.fid, f.last_gen
  FROM freshness f
  JOIN grid_warm gw ON gw.fid = f.fid
  JOIN tide_warm tw ON tw.fid = f.fid
  WHERE p_skip_recent_hours IS NULL
     OR f.last_gen IS NULL
     OR f.last_gen < now() - (p_skip_recent_hours * interval '1 hour')
  ORDER BY f.last_gen NULLS FIRST
  LIMIT COALESCE(p_limit, 100000);
$function$;
