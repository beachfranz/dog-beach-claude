-- Maintenance for public.precipitation_history.
--
-- precipitation_history is a derived 3-day rollup of weather_grid_hourly's
-- observed precipitation, bucketed to PT-local calendar days. The hourly
-- refresh (refresh-weather-grid) keeps weather_grid_hourly populated. This
-- function aggregates it down to one row per (cell, day) for the last 3 PT
-- calendar days and drops anything older.
--
-- Why a maintenance function instead of the view it replaced: precip_72h_for_point()
-- in a hot loop (daily-dog-park-refresh + future daily-beach-refresh) reads
-- 3 rows per cell from the materialized table instead of re-aggregating 72
-- hourly rows from weather_grid_hourly on every call.
--
-- Replaces the refresh-precipitation-history edge function attempt — that
-- path tried to do a separate daily Open-Meteo fetch and tripped the
-- free-tier rate limit at 600 location-requests/minute. Keeping the
-- hourly fetch and aggregating server-side avoids that constraint entirely.

CREATE OR REPLACE FUNCTION public._refresh_precipitation_history_from_grid()
RETURNS TABLE(rows_upserted bigint, rows_deleted bigint)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_today_pt date := (now() AT TIME ZONE 'America/Los_Angeles')::date;
  v_upserted bigint;
  v_deleted  bigint;
BEGIN
  -- Aggregate past 3 PT-local days from weather_grid_hourly observed rows.
  WITH agg AS (
    SELECT
      grid_lat,
      grid_lon,
      (forecast_ts AT TIME ZONE 'America/Los_Angeles')::date AS observation_date,
      sum(precip_mm) AS precip_mm
    FROM public.weather_grid_hourly
    WHERE is_observed = true
      AND precip_mm IS NOT NULL
      AND (forecast_ts AT TIME ZONE 'America/Los_Angeles')::date BETWEEN v_today_pt - 3 AND v_today_pt
    GROUP BY grid_lat, grid_lon, observation_date
  ),
  upserted AS (
    INSERT INTO public.precipitation_history
      (grid_lat, grid_lon, observation_date, precip_mm, computed_at)
    SELECT grid_lat, grid_lon, observation_date, precip_mm, now()
      FROM agg
    ON CONFLICT (grid_lat, grid_lon, observation_date)
    DO UPDATE SET precip_mm = EXCLUDED.precip_mm, computed_at = now()
    RETURNING 1
  )
  SELECT count(*) INTO v_upserted FROM upserted;

  -- Drop rows older than the 3-day rolling window.
  WITH deleted AS (
    DELETE FROM public.precipitation_history
     WHERE observation_date < v_today_pt - 3
    RETURNING 1
  )
  SELECT count(*) INTO v_deleted FROM deleted;

  RETURN QUERY SELECT v_upserted, v_deleted;
END;
$$;

COMMENT ON FUNCTION public._refresh_precipitation_history_from_grid() IS
  'Rolls up weather_grid_hourly observed precip into precipitation_history (3-day window, PT-local buckets). Drops rows older than 3 days. Idempotent — safe to call repeatedly.';
