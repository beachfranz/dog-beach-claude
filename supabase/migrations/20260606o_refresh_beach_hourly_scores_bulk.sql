-- 20260606o_refresh_beach_hourly_scores_bulk.sql
--
-- Replaces the per-beach scoring loop inside supabase/functions/daily-beach-refresh
-- with a set-based SQL function that:
--   1. JOINs beaches_gold × weather_grid_hourly via cached weather_grid_lat/lon
--      → raw weather columns (temp, wind, precip, weather_code, uv, etc.)
--   2. LEFT JOINs existing beach_day_hourly_scores → preserves tide_height
--      values cached from the weekly NOAA refresh
--   3. Computes derived surface temps (sand_temp, asphalt_temp) using the
--      formula in _shared/scoring.ts (UV → irradiance, cloud attenuation,
--      wind cooling)
--   4. UPSERTs raw columns into beach_day_hourly_scores
--   5. Sets V1 score columns (hour_score, *_score) to NULL — V1 retired
--      via Phase B (consumer pages now read hour_score_v2 transparently
--      via v2→v1 alias in beach.html, mobile-beach.html, index.html)
--   6. apply_v2_best_window_to_beach_recommendations_bulk (already shipped)
--      runs as the chained next orch job → fills hour_score_v2 + composite
--      + best_window_label on beach_day_recommendations
--
-- After cutover the consumer hot path is fully SQL:
--   :05 refresh_beach_day_hourly_scores_bulk        (this fn — writes raw)
--   :10 apply_v2_best_window_beach (existing)       (writes hour_score_v2 + recs)
--   :10 daily_weather_advisories (existing)         (bacteria + per-metric)
--
-- Replaces the every-2-min chunked-cron beach_refresh_chunked entirely.
-- The edge function daily-beach-refresh stays deployed as a safety net but
-- doesn't need to be called (decommission in a follow-up after a verification
-- window of clean runs).

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
  -- Self-clean temp tables (lesson from prior bulks).
  DROP TABLE IF EXISTS _scope, _weather, _merged;

  -- 1. Scope: scoreable beaches in filter range.
  -- beaches_gold has fid (canonical) + group_id (clustering), but no
  -- arena_group_id column. The downstream beach_day_hourly_scores.arena_group_id
  -- column is just a join key set to fid. So we alias fid AS arena_group_id
  -- throughout to keep the rest of the function readable.
  CREATE TEMP TABLE _scope ON COMMIT DROP AS
  SELECT bg.fid,
         bg.location_id,
         bg.fid AS arena_group_id,
         bg.timezone,
         bg.weather_grid_lat,
         bg.weather_grid_lon,
         bg.noaa_station_id
  FROM public.beaches_gold bg
  WHERE bg.is_active
    AND bg.scoring_tier IN ('daily','hourly')
    AND bg.location_id IS NOT NULL
    AND bg.weather_grid_lat IS NOT NULL
    AND (p_state IS NULL OR bg.state = upper(p_state))
    AND (p_fids  IS NULL OR bg.fid = ANY(p_fids));

  -- 2. Weather JOIN — one row per (fid, forecast_ts) over the horizon.
  --    Reads from weather_grid_hourly via cached cell coords (no per-fid
  --    function call). t3 tier owns past-3-days observed, t1/t2/t3 own
  --    future forecast; we don't filter by is_observed because we want
  --    everything in the [now, now+p_days_ahead] window.
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

  -- 3. Merge with existing rows (tide preservation) + compute derived.
  --    LEFT JOIN existing beach_day_hourly_scores so we keep tide_height
  --    from previous runs (the table itself is the tide cache; weekly NOAA
  --    refresh repopulates the buffer).
  --    Surface-temp formula mirrors _shared/scoring.ts:278 estimateSurfaceTemps.
  CREATE TEMP TABLE _merged ON COMMIT DROP AS
  SELECT
    w.*,
    -- Local hour / date / label
    extract(hour FROM (w.forecast_ts AT TIME ZONE w.timezone))::int AS local_hour,
    (w.forecast_ts AT TIME ZONE w.timezone)::date                   AS local_date,
    -- "6am" / "12pm" / "1pm" — mirrors TS formatter
    CASE extract(hour FROM (w.forecast_ts AT TIME ZONE w.timezone))::int
      WHEN 0  THEN '12am'
      WHEN 12 THEN '12pm'
      ELSE
        CASE WHEN extract(hour FROM (w.forecast_ts AT TIME ZONE w.timezone))::int < 12
             THEN extract(hour FROM (w.forecast_ts AT TIME ZONE w.timezone))::int::text || 'am'
             ELSE (extract(hour FROM (w.forecast_ts AT TIME ZONE w.timezone))::int - 12)::text || 'pm'
        END
    END AS hour_label,
    -- Preserved tide_height from prior runs (NULL when uncached)
    h_old.tide_height,
    h_old.tide_refreshed_at,
    -- Surface temp formula (only when daylight + temp_air known)
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

  -- 4. Clear the forecast window for these beaches (fid-drift safety,
  --    mirrors the edge fn's per-beach DELETE-before-UPSERT pattern).
  --    Scope to location_id since fid-drift creates orphans under old
  --    arena_group_id for the SAME location_id.
  DELETE FROM public.beach_day_hourly_scores h
   USING _scope s
   WHERE h.location_id  = s.location_id
     AND h.forecast_ts >= date_trunc('hour', v_now)
     AND h.forecast_ts <  date_trunc('hour', v_now) + ((p_days_ahead + 1) || ' days')::interval;

  -- 5. Bulk INSERT. V1 score columns intentionally NULL — Phase B (commit
  --    12e230c) routes consumer reads through hour_score_v2 alias on data
  --    load. V2 columns get populated by the chained
  --    apply_v2_best_window_to_beach_recommendations_bulk job.
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
      COALESCE(m.is_day, false), false, false,   -- candidate/in_best_window default false; apply_v2 doesn't need them
      m.weather_code, m.temp_air, m.feels_like, m.wind_speed, m.precip_chance, m.uv_index,
      m.tide_height,
      NULL, NULL,                              -- busyness: BestTime soft-removed
      m.sand_temp, m.asphalt_temp,
      NULL, NULL, NULL, NULL, NULL, NULL,      -- V1 score cols NULL (V1 retired via Phase B)
      NULL, NULL,
      '[]'::jsonb, '[]'::jsonb, '{}'::jsonb,   -- positive/risk codes + explainability empty (NOT NULL cols; V1 metadata)
      NULL, m.timezone, 'v2_bulk',
      v_now, m.tide_refreshed_at, v_now, v_now
    FROM _merged m
    RETURNING 1
  )
  SELECT count(*) INTO hours_written FROM ins;

  beaches_processed  := (SELECT count(DISTINCT fid) FROM _scope);
  hours_with_tide    := (SELECT count(*) FROM _merged WHERE tide_height    IS NOT NULL);
  hours_with_weather := (SELECT count(*) FROM _merged WHERE temp_air       IS NOT NULL);

  RETURN NEXT;
END;
$function$;

COMMENT ON FUNCTION public.refresh_beach_day_hourly_scores_bulk IS
  'Set-based replacement for the per-beach hourly upsert loop in daily-beach-refresh edge fn. Reads weather from weather_grid_hourly, preserves tide_height from existing rows (NOAA buffer refreshed weekly), computes surface temps. V1 score cols deliberately NULL — V2 columns are populated by chained apply_v2_best_window_to_beach_recommendations_bulk. Per pin [[advisories-sql-native]] Phase A.';

-- ─────────────────────────────────────────────────────────────────────────
-- Orch wrapper + job slot
-- ─────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public._orch_w_refresh_beach_hourly_scores_bulk(p jsonb)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  v_state             text;
  v_days_ahead        int;
  v_beaches           bigint;
  v_hours             bigint;
  v_with_tide         bigint;
  v_with_weather      bigint;
BEGIN
  v_state      := p->>'state';
  v_days_ahead := COALESCE(NULLIF(p->>'days_ahead', '')::int, 6);

  SELECT beaches_processed, hours_written, hours_with_tide, hours_with_weather
    INTO v_beaches, v_hours, v_with_tide, v_with_weather
    FROM public.refresh_beach_day_hourly_scores_bulk(v_state, NULL, v_days_ahead);

  RAISE NOTICE 'refresh_beach_hourly_bulk: state=% days=% beaches=% hours=% tide=% weather=%',
    COALESCE(v_state, 'ALL'), v_days_ahead,
    v_beaches, v_hours, v_with_tide, v_with_weather;
END;
$$;

-- New orch job. Runs every 5 min, depends on weather_grid_t1 (so we're
-- never reading stale weather). apply_v2_best_window_beach already
-- depends on beach_refresh_chunked; we add this new job as the upstream
-- WRITER of raw rows, so apply_v2_best_window_beach can later have its
-- depends_on swapped from beach_refresh_chunked to this new job.
INSERT INTO public.orch_jobs (
  job_name, description, cadence_kind, cadence_param, cron_expr,
  depends_on, depends_max_age, cost_check_fn, skip_if_succeeded_within,
  worker_kind, worker_target, worker_payload, scope_states,
  enabled, shadow_mode
) VALUES (
  'refresh_beach_hourly_scores_bulk',
  'Every 5 min: bulk-rewrite beach_day_hourly_scores raw columns from weather_grid_hourly + preserved tide cache via refresh_beach_day_hourly_scores_bulk(). Replaces the per-beach loop inside daily-beach-refresh edge fn. Chained downstream: apply_v2_best_window_beach reads these rows to write hour_score_v2 + composite_score_v2 + best_window_label.',
  'every_n_minutes',
  '5',
  NULL,
  ARRAY['weather_grid_t1']::text[],
  NULL,
  NULL,
  NULL,
  'sql_function',
  'public._orch_w_refresh_beach_hourly_scores_bulk',
  '{}'::jsonb,
  NULL,
  TRUE,
  TRUE  -- SHADOW MODE: writes happen but orch records as "shadowed" so
        -- we can compare against beach_refresh_chunked output before
        -- flipping the dep chain. Flip to FALSE after one clean cycle.
)
ON CONFLICT (job_name) DO UPDATE SET
  description    = EXCLUDED.description,
  cadence_kind   = EXCLUDED.cadence_kind,
  cadence_param  = EXCLUDED.cadence_param,
  depends_on     = EXCLUDED.depends_on,
  worker_kind    = EXCLUDED.worker_kind,
  worker_target  = EXCLUDED.worker_target,
  worker_payload = EXCLUDED.worker_payload,
  enabled        = EXCLUDED.enabled,
  shadow_mode    = EXCLUDED.shadow_mode;
