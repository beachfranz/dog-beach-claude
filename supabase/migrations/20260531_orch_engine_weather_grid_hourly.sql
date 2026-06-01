-- 20260531_orch_engine_weather_grid_hourly.sql
--
-- Supports the refresh-weather-grid edge function (TS port of Dagster
-- refresh_weather_grid asset). Adds the stale-cell picker SQL fn so the
-- edge function calls it via supabase-js .rpc() instead of inlining the
-- complex query in TS.
--
-- Edge function source: supabase/functions/refresh-weather-grid/index.ts
-- Dagster source-of-truth: scripts/dagster/.../assets/weather_grid.py

BEGIN;

-- ─── SQL helper: pick stale cells with tier-based threshold ──────────

CREATE OR REPLACE FUNCTION public._orch_pick_stale_weather_cells(
  p_hot_h  numeric DEFAULT 1,
  p_warm_h numeric DEFAULT 6,
  p_cold_h numeric DEFAULT 24,
  p_force  boolean DEFAULT false,
  p_state  text DEFAULT NULL,
  p_limit  integer DEFAULT 200
)
RETURNS TABLE(grid_lat numeric, grid_lon numeric, stale_threshold_hours numeric)
LANGUAGE sql STABLE PARALLEL SAFE
AS $$
  WITH cell_tier AS (
    SELECT g.grid_lat, g.grid_lon,
           CASE
             WHEN EXISTS (
               SELECT 1 FROM public.beaches_gold b
                WHERE b.weather_grid_lat = g.grid_lat
                  AND b.weather_grid_lon = g.grid_lon
                  AND b.is_active AND b.scoring_tier = 'hourly'
             ) THEN p_hot_h
             WHEN EXISTS (
               SELECT 1 FROM public.beaches_gold b
                WHERE b.weather_grid_lat = g.grid_lat
                  AND b.weather_grid_lon = g.grid_lon
                  AND b.is_active AND b.scoring_tier = 'daily'
             ) OR EXISTS (
               SELECT 1 FROM public.dog_parks_gold d
                WHERE d.weather_grid_lat = g.grid_lat
                  AND d.weather_grid_lon = g.grid_lon
                  AND d.is_active AND d.is_scoreable
             ) THEN p_warm_h
             ELSE p_cold_h
           END AS stale_threshold_hours,
           g.state
      FROM public.weather_grid g
  )
  SELECT ct.grid_lat, ct.grid_lon, ct.stale_threshold_hours
    FROM cell_tier ct
    LEFT JOIN LATERAL (
      SELECT MAX(fetched_at) AS last_fetched
        FROM public.weather_grid_hourly
       WHERE grid_lat = ct.grid_lat AND grid_lon = ct.grid_lon
    ) lf ON TRUE
   WHERE (p_force
          OR lf.last_fetched IS NULL
          OR lf.last_fetched < (now() - (ct.stale_threshold_hours || ' hours')::interval))
     AND (p_state IS NULL OR ct.state = p_state)
   ORDER BY ct.stale_threshold_hours, ct.grid_lat, ct.grid_lon
   LIMIT p_limit;
$$;

COMMENT ON FUNCTION public._orch_pick_stale_weather_cells IS
  'Stale cell picker used by refresh-weather-grid edge function. Tier-based threshold per [[weather-grid-reference-layer]].';

-- ─── Update orch_jobs catalog: noop → edge_function ─────────────────

UPDATE public.orch_jobs
   SET worker_kind   = 'edge_function',
       worker_target = 'refresh-weather-grid',
       worker_payload = jsonb_build_object('limit', 200),
       description = 'Refresh Open-Meteo hourly weather into weather_grid_hourly for stale cells. TS edge function port of Dagster refresh_weather_grid. Cell limit per call: 200; orchestrator re-fires hourly to drain.',
       updated_at = now()
 WHERE job_name = 'weather_grid_hourly';

COMMIT;
