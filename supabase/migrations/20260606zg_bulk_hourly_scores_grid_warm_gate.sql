-- 20260606zg_bulk_hourly_scores_grid_warm_gate.sql
--
-- Port the canonical per-cell warmth gate to refresh_beach_day_hourly_scores_bulk.
-- Shape B from [[grid-consumers-require-cell-warmth-gate]]: filter
-- inside _scope so cold weather cells silently drop out of the fanout
-- instead of inserting partial hourly rows during the loader race window.
--
-- This is the SQL-native bulk currently in orch_jobs as shadow_mode=true
-- behind depends_on=weather_grid_t1. Before it flips out of shadow it
-- inherits the same race-vulnerability the edge fn had — JOIN to
-- weather_grid_hourly is a silent no-op for cells the loader hasn't
-- finished. Race shape proven on HI fid 13882 2026-06-07: edge-fn version
-- wrote 213/240 hourly rows; bar charts ended at HI 1PM.
--
-- depends_on=weather_grid_t1 at the JOB level isn't enough — t1's
-- "successful" run only guarantees the next 48h horizon got fetched for
-- cells it was given, not that EVERY cell containing a state's scoreable
-- beaches is warm. (HI's first-launch t1 was mid-batch when the orch
-- job for it fired.) The per-cell gate IS the safety net.
--
-- Threshold: ≥45 forecast rows in [now, now+48h]. Same canon as picker
-- + marine (20260606ze, 20260606zf).
--
-- Function body is UNCHANGED from 20260606o except for the new
-- _warm_cells CTE filter applied to _scope.

CREATE OR REPLACE FUNCTION public.refresh_beach_day_hourly_scores_bulk(
  p_state       text     DEFAULT NULL,
  p_fids        bigint[] DEFAULT NULL,
  p_days_ahead  int      DEFAULT 6
) RETURNS TABLE(
  beaches_processed   bigint,
  hours_written       bigint,
  hours_with_tide     bigint,
  hours_with_weather  bigint
) LANGUAGE plpgsql AS $function$
DECLARE
  v_now timestamptz := now();
BEGIN
  DROP TABLE IF EXISTS _warm_cells, _scope, _weather, _merged;

  -- ──── NEW: per-cell warmth gate ([[grid-consumers-require-cell-warmth-gate]]) ────
  -- Cells with < 45 forecast rows in [now, now+48h] are mid-load; their
  -- beaches drop out of this run. Self-heals on the next t1 tick.
  --
  -- The state/fids filter applies HERE too so this CTE doesn't scan
  -- every cell in the country when called for just one state.
  CREATE TEMP TABLE _warm_cells ON COMMIT DROP AS
  SELECT bg.weather_grid_lat AS grid_lat, bg.weather_grid_lon AS grid_lon
  FROM public.beaches_gold bg
  JOIN public.weather_grid_hourly wgh
    ON wgh.grid_lat = bg.weather_grid_lat
   AND wgh.grid_lon = bg.weather_grid_lon
   AND wgh.forecast_ts >  v_now
   AND wgh.forecast_ts <= v_now + interval '48 hours'
  WHERE bg.is_active
    AND bg.scoring_tier IN ('daily','hourly')
    AND bg.location_id IS NOT NULL
    AND bg.weather_grid_lat IS NOT NULL
    AND (p_state IS NULL OR bg.state = upper(p_state))
    AND (p_fids  IS NULL OR bg.fid = ANY(p_fids))
  GROUP BY bg.weather_grid_lat, bg.weather_grid_lon
  HAVING count(*) >= 45;

  -- 1. Scope: scoreable beaches in filter range AND with warm grid cells.
  CREATE TEMP TABLE _scope ON COMMIT DROP AS
  SELECT bg.fid,
         bg.location_id,
         bg.fid AS arena_group_id,
         bg.timezone,
         bg.weather_grid_lat,
         bg.weather_grid_lon,
         bg.noaa_station_id
  FROM public.beaches_gold bg
  JOIN _warm_cells wc                        -- ← gate
    ON wc.grid_lat = bg.weather_grid_lat
   AND wc.grid_lon = bg.weather_grid_lon
  WHERE bg.is_active
    AND bg.scoring_tier IN ('daily','hourly')
    AND bg.location_id IS NOT NULL
    AND bg.weather_grid_lat IS NOT NULL
    AND (p_state IS NULL OR bg.state = upper(p_state))
    AND (p_fids  IS NULL OR bg.fid = ANY(p_fids));

  -- 2. Weather JOIN — one row per (fid, forecast_ts) over the horizon.
  CREATE TEMP TABLE _weather ON COMMIT DROP AS
  SELECT s.fid, s.location_id, s.arena_group_id, s.timezone, s.noaa_station_id,
         wgh.forecast_ts,
         wgh.temp_air,
         wgh.feels_like,
         wgh.wind_speed,
         wgh.precip_chance,
         wgh.weather_code,
         wgh.uv_index,
         wgh.cloud_cover,
         wgh.is_day
  FROM _scope s
  JOIN public.weather_grid_hourly wgh
    ON wgh.grid_lat = s.weather_grid_lat
   AND wgh.grid_lon = s.weather_grid_lon
  WHERE wgh.forecast_ts >= date_trunc('hour', v_now)
    AND wgh.forecast_ts <  date_trunc('hour', v_now) + ((p_days_ahead + 1) || ' days')::interval;

  -- 3. Merge with existing rows (tide preservation) + compute derived. UNCHANGED.
  CREATE TEMP TABLE _merged ON COMMIT DROP AS
  SELECT
    w.*,
    extract(hour FROM (w.forecast_ts AT TIME ZONE w.timezone))::int AS local_hour,
    (w.forecast_ts AT TIME ZONE w.timezone)::date                   AS local_date,
    CASE extract(hour FROM (w.forecast_ts AT TIME ZONE w.timezone))::int
      WHEN 0  THEN '12am'
      WHEN 12 THEN '12pm'
      ELSE
        CASE WHEN extract(hour FROM (w.forecast_ts AT TIME ZONE w.timezone))::int < 12
             THEN extract(hour FROM (w.forecast_ts AT TIME ZONE w.timezone))::int::text || 'am'
             ELSE (extract(hour FROM (w.forecast_ts AT TIME ZONE w.timezone))::int - 12)::text || 'pm'
        END
    END AS hour_label,
    h_old.tide_height,
    h_old.tide_refreshed_at,
    CASE WHEN COALESCE(w.is_day, false) AND w.temp_air IS NOT NULL
         THEN round((
           w.temp_air
           + GREATEST(0, COALESCE(w.uv_index, 0)) * 100
             * (1 - COALESCE(LEAST(GREATEST(w.cloud_cover, 0), 100), 0) / 100.0 * 0.7)
             * 0.025
           - LEAST(sqrt(GREATEST(COALESCE(w.wind_speed, 0), 0)) * 1.2, 8)
         )::numeric, 1)
         ELSE NULL
    END AS sand_temp,
    CASE WHEN COALESCE(w.is_day, false) AND w.temp_air IS NOT NULL
         THEN round((
           w.temp_air
           + GREATEST(0, COALESCE(w.uv_index, 0)) * 100
             * (1 - COALESCE(LEAST(GREATEST(w.cloud_cover, 0), 100), 0) / 100.0 * 0.7)
             * 0.05
           - LEAST(sqrt(GREATEST(COALESCE(w.wind_speed, 0), 0)) * 1.2, 8)
         )::numeric, 1)
         ELSE NULL
    END AS asphalt_temp
  FROM _weather w
  LEFT JOIN public.beach_day_hourly_scores h_old
    ON h_old.arena_group_id = w.arena_group_id
   AND h_old.forecast_ts    = w.forecast_ts;

  -- 4. Clear the forecast window. UNCHANGED, except _scope is now gated
  --    so cold-cell beaches keep their prior rows untouched (no data is
  --    deleted that won't be re-inserted in the same call).
  DELETE FROM public.beach_day_hourly_scores h
   USING _scope s
   WHERE h.location_id  = s.location_id
     AND h.forecast_ts >= date_trunc('hour', v_now)
     AND h.forecast_ts <  date_trunc('hour', v_now) + ((p_days_ahead + 1) || ' days')::interval;

  -- 5. Bulk INSERT. UNCHANGED.
  WITH ins AS (
    INSERT INTO public.beach_day_hourly_scores (
      location_id, arena_group_id, local_date, forecast_ts, local_hour, hour_label,
      is_daylight, is_candidate_window, is_in_best_window,
      weather_code, temp_air, feels_like, wind_speed, precip_chance, uv_index,
      tide_height, busyness_score, busyness_category,
      sand_temp, asphalt_temp,
      hour_score, tide_score, wind_score, crowd_score, rain_score, temp_score,
      uv_score, weather_score,
      positive_reason_codes, risk_reason_codes, explainability,
      hour_text, timezone, scoring_version,
      weather_refreshed_at, tide_refreshed_at, generated_at, updated_at
    )
    SELECT
      m.location_id, m.arena_group_id, m.local_date, m.forecast_ts, m.local_hour, m.hour_label,
      COALESCE(m.is_day, false), false, false,
      m.weather_code, m.temp_air, m.feels_like, m.wind_speed, m.precip_chance, m.uv_index,
      m.tide_height,
      NULL, NULL,
      m.sand_temp, m.asphalt_temp,
      NULL, NULL, NULL, NULL, NULL, NULL,
      NULL, NULL,
      '[]'::jsonb, '[]'::jsonb, '{}'::jsonb,
      NULL, m.timezone, 'v2_bulk',
      v_now, m.tide_refreshed_at, v_now, v_now
    FROM _merged m
    RETURNING 1
  )
  SELECT count(*) INTO hours_written FROM ins;

  beaches_processed  := (SELECT count(DISTINCT fid) FROM _scope);
  hours_with_tide    := (SELECT count(*) FROM _merged WHERE tide_height IS NOT NULL);
  hours_with_weather := (SELECT count(*) FROM _merged WHERE temp_air    IS NOT NULL);

  RETURN NEXT;
END;
$function$;

COMMENT ON FUNCTION public.refresh_beach_day_hourly_scores_bulk IS
  'Set-based replacement for the per-beach hourly upsert loop in daily-beach-refresh edge fn. Reads weather from weather_grid_hourly (gated on per-cell ≥45 forecast rows in next 48h per [[grid-consumers-require-cell-warmth-gate]]), preserves tide_height from existing rows (NOAA buffer refreshed weekly), computes surface temps. V1 score cols deliberately NULL — V2 columns are populated by chained apply_v2_best_window_to_beach_recommendations_bulk. Per pin [[advisories-sql-native]] Phase A.';
