-- 20260607c_grid_warm_threshold_realism.sql
--
-- Relax the per-cell warmth gate threshold to match how t1 actually
-- populates cells.
--
-- Original threshold (20260606ze/zf/zg, shipped 2026-06-06):
--   >= 45 forecast rows in [now, now+48h]
--
-- New threshold:
--   >= 22 forecast rows in [now, now+24h]
--
-- Why this fixes ME launch:
--   weather_grid_t1 fetches Open-Meteo with `forecast_days=2`, which
--   returns hours from `today 00:00` to `tomorrow 23:00` (UTC). As the
--   day progresses, the forward-looking subset (rows > now()) erodes
--   from 48 → 24 by end of day. ME launched at 16:30 UTC — only ~32
--   hours of "next 48h" coverage was achievable per cell. 200 of 201
--   cells failed the 45-row threshold; the gate was unsatisfiable.
--   Yesterday's HI launch passed only because it ran at 00:21 UTC
--   right after t1 fetched a fresh day.
--
-- "≥22 of next 24h" preserves the gate's actual purpose (cell has
-- contiguous near-term forecast to score against — not the partial-
-- with-gaps situation that caused HI fid 13882's 1PM cutoff) while
-- being satisfiable at any UTC hour. 22/24 = 92% of the immediate-day
-- window, equivalent to the original's 45/48 = 94% in spirit.
--
-- Affected: beaches_due_for_refresh, dog_parks_due_for_refresh,
-- refresh_beach_day_hourly_scores_bulk._warm_cells,
-- refresh_marine_advisories._warm_cells. All four mirror each other
-- per [[paired-functions-port-fixes-both-sides]] and the
-- [[grid-consumers-require-cell-warmth-gate]] canon.

BEGIN;

-- ──── beaches_due_for_refresh ────
CREATE OR REPLACE FUNCTION public.beaches_due_for_refresh(
  p_target_fids         bigint[] DEFAULT NULL,
  p_target_location_ids text[]   DEFAULT NULL,
  p_skip_recent_hours   integer  DEFAULT NULL,
  p_limit               integer  DEFAULT NULL
)
RETURNS TABLE(fid bigint, last_generated_at timestamp with time zone)
LANGUAGE sql
STABLE
AS $$
  WITH eligible AS (
    SELECT bg.fid, bg.weather_grid_lat, bg.weather_grid_lon
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
  WHERE p_skip_recent_hours IS NULL
     OR f.last_gen IS NULL
     OR f.last_gen < now() - (p_skip_recent_hours * interval '1 hour')
  ORDER BY f.last_gen NULLS FIRST
  LIMIT COALESCE(p_limit, 100000);
$$;

-- ──── dog_parks_due_for_refresh ────
CREATE OR REPLACE FUNCTION public.dog_parks_due_for_refresh(
  p_target_fids       bigint[] DEFAULT NULL,
  p_state_filter      text[]   DEFAULT NULL,
  p_skip_recent_hours integer  DEFAULT NULL,
  p_limit             integer  DEFAULT NULL
)
RETURNS TABLE(fid bigint, last_generated_at timestamp with time zone)
LANGUAGE sql
STABLE
AS $$
  WITH eligible AS (
    SELECT dpg.fid, dpg.weather_grid_lat, dpg.weather_grid_lon
    FROM public.dog_parks_gold dpg
    WHERE dpg.is_active
      AND dpg.is_scoreable
      AND (p_target_fids IS NULL OR dpg.fid = ANY(p_target_fids))
      AND (p_state_filter IS NULL OR dpg.state = ANY(p_state_filter))
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
    HAVING count(*) >= 22
  ),
  freshness AS (
    SELECT e.fid, max(r.generated_at) AS last_gen
    FROM eligible e
    LEFT JOIN public.dog_park_day_recommendations r
      ON r.dog_park_fid = e.fid
    GROUP BY e.fid
  )
  SELECT f.fid, f.last_gen
  FROM freshness f
  JOIN grid_warm gw ON gw.fid = f.fid
  WHERE p_skip_recent_hours IS NULL
     OR f.last_gen IS NULL
     OR f.last_gen < now() - (p_skip_recent_hours * interval '1 hour')
  ORDER BY f.last_gen NULLS FIRST
  LIMIT COALESCE(p_limit, 100000);
$$;

-- ──── refresh_beach_day_hourly_scores_bulk ────
-- Body unchanged from 20260606zg except the _warm_cells gate window/threshold.
CREATE OR REPLACE FUNCTION public.refresh_beach_day_hourly_scores_bulk(
  p_state text DEFAULT NULL,
  p_fids bigint[] DEFAULT NULL,
  p_days_ahead integer DEFAULT 6
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

  CREATE TEMP TABLE _warm_cells ON COMMIT DROP AS
  SELECT bg.weather_grid_lat AS grid_lat, bg.weather_grid_lon AS grid_lon
  FROM public.beaches_gold bg
  JOIN public.weather_grid_hourly wgh
    ON wgh.grid_lat = bg.weather_grid_lat
   AND wgh.grid_lon = bg.weather_grid_lon
   AND wgh.forecast_ts >  v_now
   AND wgh.forecast_ts <= v_now + interval '24 hours'
  WHERE bg.is_active
    AND bg.scoring_tier IN ('daily','hourly')
    AND bg.location_id IS NOT NULL
    AND bg.weather_grid_lat IS NOT NULL
    AND (p_state IS NULL OR bg.state = upper(p_state))
    AND (p_fids  IS NULL OR bg.fid = ANY(p_fids))
  GROUP BY bg.weather_grid_lat, bg.weather_grid_lon
  HAVING count(*) >= 22;

  CREATE TEMP TABLE _scope ON COMMIT DROP AS
  SELECT bg.fid, bg.location_id, bg.fid AS arena_group_id, bg.timezone,
         bg.weather_grid_lat, bg.weather_grid_lon, bg.noaa_station_id
  FROM public.beaches_gold bg
  JOIN _warm_cells wc
    ON wc.grid_lat = bg.weather_grid_lat AND wc.grid_lon = bg.weather_grid_lon
  WHERE bg.is_active AND bg.scoring_tier IN ('daily','hourly')
    AND bg.location_id IS NOT NULL AND bg.weather_grid_lat IS NOT NULL
    AND (p_state IS NULL OR bg.state = upper(p_state))
    AND (p_fids  IS NULL OR bg.fid = ANY(p_fids));

  CREATE TEMP TABLE _weather ON COMMIT DROP AS
  SELECT s.fid, s.location_id, s.arena_group_id, s.timezone, s.noaa_station_id,
         wgh.forecast_ts, wgh.temp_air, wgh.feels_like, wgh.wind_speed,
         wgh.precip_chance, wgh.weather_code, wgh.uv_index, wgh.cloud_cover, wgh.is_day
  FROM _scope s
  JOIN public.weather_grid_hourly wgh
    ON wgh.grid_lat = s.weather_grid_lat AND wgh.grid_lon = s.weather_grid_lon
  WHERE wgh.forecast_ts >= date_trunc('hour', v_now)
    AND wgh.forecast_ts <  date_trunc('hour', v_now) + ((p_days_ahead + 1) || ' days')::interval;

  CREATE TEMP TABLE _merged ON COMMIT DROP AS
  SELECT w.*,
    extract(hour FROM (w.forecast_ts AT TIME ZONE w.timezone))::int AS local_hour,
    (w.forecast_ts AT TIME ZONE w.timezone)::date AS local_date,
    CASE extract(hour FROM (w.forecast_ts AT TIME ZONE w.timezone))::int
      WHEN 0 THEN '12am' WHEN 12 THEN '12pm'
      ELSE CASE WHEN extract(hour FROM (w.forecast_ts AT TIME ZONE w.timezone))::int < 12
                THEN extract(hour FROM (w.forecast_ts AT TIME ZONE w.timezone))::int::text || 'am'
                ELSE (extract(hour FROM (w.forecast_ts AT TIME ZONE w.timezone))::int - 12)::text || 'pm' END
    END AS hour_label,
    h_old.tide_height, h_old.tide_refreshed_at,
    CASE WHEN COALESCE(w.is_day, false) AND w.temp_air IS NOT NULL
         THEN round((w.temp_air
           + GREATEST(0, COALESCE(w.uv_index, 0)) * 100
             * (1 - COALESCE(LEAST(GREATEST(w.cloud_cover, 0), 100), 0) / 100.0 * 0.7) * 0.025
           - LEAST(sqrt(GREATEST(COALESCE(w.wind_speed, 0), 0)) * 1.2, 8))::numeric, 1)
         ELSE NULL END AS sand_temp,
    CASE WHEN COALESCE(w.is_day, false) AND w.temp_air IS NOT NULL
         THEN round((w.temp_air
           + GREATEST(0, COALESCE(w.uv_index, 0)) * 100
             * (1 - COALESCE(LEAST(GREATEST(w.cloud_cover, 0), 100), 0) / 100.0 * 0.7) * 0.05
           - LEAST(sqrt(GREATEST(COALESCE(w.wind_speed, 0), 0)) * 1.2, 8))::numeric, 1)
         ELSE NULL END AS asphalt_temp
  FROM _weather w
  LEFT JOIN public.beach_day_hourly_scores h_old
    ON h_old.arena_group_id = w.arena_group_id AND h_old.forecast_ts = w.forecast_ts;

  DELETE FROM public.beach_day_hourly_scores h
   USING _scope s
   WHERE h.location_id = s.location_id
     AND h.forecast_ts >= date_trunc('hour', v_now)
     AND h.forecast_ts <  date_trunc('hour', v_now) + ((p_days_ahead + 1) || ' days')::interval;

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
      weather_refreshed_at, tide_refreshed_at, generated_at, updated_at)
    SELECT
      m.location_id, m.arena_group_id, m.local_date, m.forecast_ts, m.local_hour, m.hour_label,
      COALESCE(m.is_day, false), false, false,
      m.weather_code, m.temp_air, m.feels_like, m.wind_speed, m.precip_chance, m.uv_index,
      m.tide_height, NULL, NULL,
      m.sand_temp, m.asphalt_temp,
      NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
      '[]'::jsonb, '[]'::jsonb, '{}'::jsonb,
      NULL, m.timezone, 'v2_bulk',
      v_now, m.tide_refreshed_at, v_now, v_now
    FROM _merged m RETURNING 1)
  SELECT count(*) INTO hours_written FROM ins;

  beaches_processed  := (SELECT count(DISTINCT fid) FROM _scope);
  hours_with_tide    := (SELECT count(*) FROM _merged WHERE tide_height IS NOT NULL);
  hours_with_weather := (SELECT count(*) FROM _merged WHERE temp_air    IS NOT NULL);
  RETURN NEXT;
END;
$function$;

-- ──── refresh_marine_advisories ────
-- Body unchanged from 20260606zf except the _warm_cells gate window/threshold.
CREATE OR REPLACE FUNCTION public.refresh_marine_advisories(
  p_state          text   DEFAULT NULL,
  p_fid            bigint DEFAULT NULL,
  p_horizon_hours  int    DEFAULT 48
) RETURNS TABLE(upserted bigint, retired bigint)
LANGUAGE plpgsql AS $function$
DECLARE
  v_now timestamptz := now();
  v_upserted bigint := 0;
  v_retired  bigint := 0;
BEGIN
  DROP TABLE IF EXISTS _rules, _warm_cells, _hours, _triggered, _extremes;

  CREATE TEMP TABLE _rules ON COMMIT DROP AS
  SELECT * FROM (VALUES
    ('too_cold_swim',  'sst_c',         '<',  13.0,  'moderate',  'Cold water', '❄️',  'skip_swim',
       'Water temp %s°C — too cold for swimming. Keep your dog out of the water.', '%s°C', 'min', 1),
    ('paw_cold',       'sst_c',         '<',  5.0,   'minor',     'Cold paws',  '🥶',  'cold_paws',
       'Cold ground — limit time on damp sand; small dogs may need booties.',     '%s°C', 'min', 1),
    ('too_warm_swim',  'sst_c',         '>',  27.0,  'moderate',  'Warm water', '🌡️', 'review_required',
       'Water warm enough that algal blooms become a risk — confirm local water-quality before swimming.',
                                                                                  '%s°C', 'max', 1),
    ('swim_advisory',  'wave_height_m', '>',  1.2,   'moderate',  'Big surf',   '🌊',  'skip_swim',
       'Surf is up (%sm). Skip the swim — currents and waves too strong for safe dog play.',
                                                                                  '%sm',  'max', 1),
    ('powerful_surf',  'swell_power_kj_m', '>', 50.0, 'moderate', 'Powerful swell', '🌊', 'skip_swim',
       'Long-period swell (%s kJ/m wavefront). Even small waves punch hard; rip risk elevated. Skip the swim.',
                                                                                  '%s kJ/m', 'max', 0),
    ('strong_drift',   'ocean_current_velocity_ms', '>', 0.5, 'moderate', 'Strong drift', '🌀', 'skip_swim',
       'Ocean current %s m/s — drift risk for any dog more than ankle-deep. Keep her on the sand.',
                                                                                  '%s m/s', 'max', 1),
    ('choppy',         'wind_wave_height_m', '>', 0.8, 'minor', 'Choppy', '💨', 'review_required',
       'Choppy wind-driven waves (%sm) — messy water, less fun for play, harder visibility.',
                                                                                  '%sm', 'max', 1)
  ) AS r(key, field, op, threshold, severity, label, icon, klass, text_tmpl, value_fmt, agg, decimals);

  CREATE TEMP TABLE _warm_cells ON COMMIT DROP AS
  SELECT bg.marine_grid_lat AS grid_lat, bg.marine_grid_lon AS grid_lon
  FROM public.beaches_gold bg
  JOIN public.marine_grid_hourly mgh
    ON mgh.grid_lat = bg.marine_grid_lat
   AND mgh.grid_lon = bg.marine_grid_lon
   AND mgh.forecast_ts >  v_now
   AND mgh.forecast_ts <= v_now + interval '24 hours'
  WHERE bg.is_active AND bg.scoring_tier IN ('daily','hourly')
    AND bg.marine_grid_lat IS NOT NULL
    AND (p_state IS NULL OR bg.state = upper(p_state))
    AND (p_fid   IS NULL OR bg.fid   = p_fid)
  GROUP BY bg.marine_grid_lat, bg.marine_grid_lon
  HAVING count(*) >= 22;

  CREATE TEMP TABLE _hours ON COMMIT DROP AS
  SELECT bg.fid AS beach_fid, mgh.forecast_ts AS ts,
         mgh.wave_height_m, mgh.sst_c, mgh.swell_power_kj_m,
         mgh.ocean_current_velocity_ms, mgh.wind_wave_height_m, mgh.swell_wave_height_m
  FROM public.beaches_gold bg
  JOIN _warm_cells wc ON wc.grid_lat = bg.marine_grid_lat AND wc.grid_lon = bg.marine_grid_lon
  JOIN public.marine_grid_hourly mgh
    ON mgh.grid_lat = bg.marine_grid_lat AND mgh.grid_lon = bg.marine_grid_lon
  WHERE bg.is_active AND bg.scoring_tier IN ('daily','hourly')
    AND bg.marine_grid_lat IS NOT NULL
    AND mgh.forecast_ts >= date_trunc('hour', v_now)
    AND mgh.forecast_ts <  date_trunc('hour', v_now) + (p_horizon_hours || ' hours')::interval
    AND (p_state IS NULL OR bg.state = upper(p_state))
    AND (p_fid   IS NULL OR bg.fid   = p_fid);

  CREATE TEMP TABLE _triggered ON COMMIT DROP AS
  SELECT h.beach_fid, h.ts, r.key, r.field, r.op, r.threshold,
    r.severity, r.label, r.icon, r.klass, r.text_tmpl, r.value_fmt, r.agg, r.decimals,
    CASE r.field
      WHEN 'sst_c'                     THEN h.sst_c
      WHEN 'wave_height_m'             THEN h.wave_height_m
      WHEN 'swell_power_kj_m'          THEN h.swell_power_kj_m
      WHEN 'ocean_current_velocity_ms' THEN h.ocean_current_velocity_ms
      WHEN 'wind_wave_height_m'        THEN h.wind_wave_height_m
    END AS field_val
  FROM _hours h CROSS JOIN _rules r
  WHERE CASE r.field
          WHEN 'sst_c'                     THEN h.sst_c
          WHEN 'wave_height_m'             THEN h.wave_height_m
          WHEN 'swell_power_kj_m'          THEN h.swell_power_kj_m
          WHEN 'ocean_current_velocity_ms' THEN h.ocean_current_velocity_ms
          WHEN 'wind_wave_height_m'        THEN h.wind_wave_height_m
        END IS NOT NULL
    AND CASE r.op
          WHEN '<' THEN CASE r.field
              WHEN 'sst_c'                     THEN h.sst_c                     < r.threshold
              WHEN 'wave_height_m'             THEN h.wave_height_m             < r.threshold
              WHEN 'swell_power_kj_m'          THEN h.swell_power_kj_m          < r.threshold
              WHEN 'ocean_current_velocity_ms' THEN h.ocean_current_velocity_ms < r.threshold
              WHEN 'wind_wave_height_m'        THEN h.wind_wave_height_m        < r.threshold END
          WHEN '>' THEN CASE r.field
              WHEN 'sst_c'                     THEN h.sst_c                     > r.threshold
              WHEN 'wave_height_m'             THEN h.wave_height_m             > r.threshold
              WHEN 'swell_power_kj_m'          THEN h.swell_power_kj_m          > r.threshold
              WHEN 'ocean_current_velocity_ms' THEN h.ocean_current_velocity_ms > r.threshold
              WHEN 'wind_wave_height_m'        THEN h.wind_wave_height_m        > r.threshold END
        END
    AND CASE r.key
          WHEN 'choppy' THEN h.wind_wave_height_m > coalesce(h.swell_wave_height_m, 0) * 1.5
          ELSE TRUE END;

  CREATE TEMP TABLE _extremes ON COMMIT DROP AS
  SELECT beach_fid, key, severity, label, icon, klass, text_tmpl, value_fmt, decimals,
    CASE agg WHEN 'min' THEN min(field_val) ELSE max(field_val) END AS extreme,
    min(ts) AS first_ts, max(ts) AS last_ts, count(*) AS hours_triggered
  FROM _triggered
  GROUP BY beach_fid, key, severity, label, icon, klass, text_tmpl, value_fmt, agg, decimals;

  WITH ins AS (
    INSERT INTO public.beach_advisory (
      beach_fid, advisory_key, source, event_type, severity,
      valid_from, valid_to, dog_impact_class, dog_impact_text,
      translation_source, label, value, icon, raw_data, fetched_at)
    SELECT e.beach_fid,
      format('marine:%s:%s', e.key, CURRENT_DATE),
      'marine_threshold', e.key, e.severity,
      e.first_ts, e.last_ts, e.klass,
      format(e.text_tmpl, round(e.extreme, e.decimals)::text),
      'rule', e.label, format(e.value_fmt, round(e.extreme, e.decimals)::text),
      e.icon,
      jsonb_build_object('hours_triggered', e.hours_triggered,
                         'extreme_value', e.extreme::float,
                         'rule', e.key, 'scoring_version', 'v2_grid'),
      v_now
    FROM _extremes e
    ON CONFLICT (beach_fid, advisory_key) DO UPDATE SET
      severity = EXCLUDED.severity, valid_from = EXCLUDED.valid_from,
      valid_to = EXCLUDED.valid_to, dog_impact_text = EXCLUDED.dog_impact_text,
      value = EXCLUDED.value, raw_data = EXCLUDED.raw_data,
      fetched_at = EXCLUDED.fetched_at
    RETURNING 1)
  SELECT count(*) INTO v_upserted FROM ins;

  WITH del AS (
    DELETE FROM public.beach_advisory ba
    USING public.beaches_gold bg
    WHERE ba.beach_fid = bg.fid AND ba.source = 'marine_threshold'
      AND bg.is_active AND bg.scoring_tier IN ('daily','hourly')
      AND (p_state IS NULL OR bg.state = upper(p_state))
      AND (p_fid   IS NULL OR bg.fid   = p_fid)
      AND ba.fetched_at < v_now - interval '5 seconds'
      AND EXISTS (SELECT 1 FROM _warm_cells wc
                   WHERE wc.grid_lat = bg.marine_grid_lat
                     AND wc.grid_lon = bg.marine_grid_lon)
    RETURNING 1)
  SELECT count(*) INTO v_retired FROM del;

  RETURN QUERY SELECT v_upserted, v_retired;
END;
$function$;

COMMIT;
