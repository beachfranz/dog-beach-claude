-- 20260606zf_marine_advisories_grid_warm_gate.sql
--
-- Port the canonical per-cell warmth gate to refresh_marine_advisories.
-- Shape B from [[grid-consumers-require-cell-warmth-gate]]: filter
-- inside the function's scope so cold marine cells silently drop out
-- of the fanout instead of producing degraded advisories during the
-- loader race window.
--
-- Marine cells warm via refresh-marine-grid (hourly cron, depends_on=[]).
-- The trigger tg_compute_marine_grid_cell registers cells when beaches
-- insert; the next-48h forecast doesn't land until the next loader tick.
-- If hourly_marine_advisories races (its depends_on=refresh_marine_grid
-- is job-level not per-cell), advisories under-fire on cold cells until
-- the next tick. Self-healing via the gate.
--
-- Threshold = ≥45 forecast rows in [now, now+48h]. Mirrors weather
-- picker gate (20260606ze). marine_grid_hourly has no is_observed
-- distinction so the count is pure forecast.
--
-- This file faithfully re-creates refresh_marine_advisories(); the
-- ONLY change vs 20260606l is the new _warm_cells CTE + the JOIN
-- that filters _hours to its members. Everything else is verbatim.

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

  -- Threshold rules (unchanged from 20260606l).
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

  -- ──── NEW: per-cell warmth gate ([[grid-consumers-require-cell-warmth-gate]]) ────
  -- Cells with < 45 forecast rows in [now, now+48h] are mid-load; their
  -- beaches drop out of this run. Self-heals on the next marine-grid tick.
  CREATE TEMP TABLE _warm_cells ON COMMIT DROP AS
  SELECT bg.marine_grid_lat AS grid_lat, bg.marine_grid_lon AS grid_lon
  FROM public.beaches_gold bg
  JOIN public.marine_grid_hourly mgh
    ON mgh.grid_lat = bg.marine_grid_lat
   AND mgh.grid_lon = bg.marine_grid_lon
   AND mgh.forecast_ts >  v_now
   AND mgh.forecast_ts <= v_now + interval '48 hours'
  WHERE bg.is_active
    AND bg.scoring_tier IN ('daily','hourly')
    AND bg.marine_grid_lat IS NOT NULL
    AND (p_state IS NULL OR bg.state = upper(p_state))
    AND (p_fid   IS NULL OR bg.fid   = p_fid)
  GROUP BY bg.marine_grid_lat, bg.marine_grid_lon
  HAVING count(*) >= 45;

  -- Scope: JOIN beaches_gold to marine_grid_hourly AND _warm_cells.
  -- Beaches whose cell didn't pass the gate are silently absent.
  CREATE TEMP TABLE _hours ON COMMIT DROP AS
  SELECT
    bg.fid AS beach_fid,
    mgh.forecast_ts AS ts,
    mgh.wave_height_m,
    mgh.sst_c,
    mgh.swell_power_kj_m,
    mgh.ocean_current_velocity_ms,
    mgh.wind_wave_height_m,
    mgh.swell_wave_height_m
  FROM public.beaches_gold bg
  JOIN _warm_cells wc
    ON wc.grid_lat = bg.marine_grid_lat
   AND wc.grid_lon = bg.marine_grid_lon
  JOIN public.marine_grid_hourly mgh
    ON mgh.grid_lat = bg.marine_grid_lat
   AND mgh.grid_lon = bg.marine_grid_lon
  WHERE bg.is_active
    AND bg.scoring_tier IN ('daily','hourly')
    AND bg.marine_grid_lat IS NOT NULL
    AND mgh.forecast_ts >= date_trunc('hour', v_now)
    AND mgh.forecast_ts <  date_trunc('hour', v_now) + (p_horizon_hours || ' hours')::interval
    AND (p_state IS NULL OR bg.state = upper(p_state))
    AND (p_fid   IS NULL OR bg.fid   = p_fid);

  -- Threshold-firing rows per (beach, hour, rule). UNCHANGED from 20260606l.
  CREATE TEMP TABLE _triggered ON COMMIT DROP AS
  SELECT
    h.beach_fid, h.ts, r.key, r.field, r.op, r.threshold,
    r.severity, r.label, r.icon, r.klass, r.text_tmpl, r.value_fmt, r.agg, r.decimals,
    CASE r.field
      WHEN 'sst_c'                     THEN h.sst_c
      WHEN 'wave_height_m'             THEN h.wave_height_m
      WHEN 'swell_power_kj_m'          THEN h.swell_power_kj_m
      WHEN 'ocean_current_velocity_ms' THEN h.ocean_current_velocity_ms
      WHEN 'wind_wave_height_m'        THEN h.wind_wave_height_m
    END AS field_val
  FROM _hours h
  CROSS JOIN _rules r
  WHERE CASE r.field
          WHEN 'sst_c'                     THEN h.sst_c
          WHEN 'wave_height_m'             THEN h.wave_height_m
          WHEN 'swell_power_kj_m'          THEN h.swell_power_kj_m
          WHEN 'ocean_current_velocity_ms' THEN h.ocean_current_velocity_ms
          WHEN 'wind_wave_height_m'        THEN h.wind_wave_height_m
        END IS NOT NULL
    AND CASE r.op
          WHEN '<'  THEN
            CASE r.field
              WHEN 'sst_c'                     THEN h.sst_c                     < r.threshold
              WHEN 'wave_height_m'             THEN h.wave_height_m             < r.threshold
              WHEN 'swell_power_kj_m'          THEN h.swell_power_kj_m          < r.threshold
              WHEN 'ocean_current_velocity_ms' THEN h.ocean_current_velocity_ms < r.threshold
              WHEN 'wind_wave_height_m'        THEN h.wind_wave_height_m        < r.threshold
            END
          WHEN '>'  THEN
            CASE r.field
              WHEN 'sst_c'                     THEN h.sst_c                     > r.threshold
              WHEN 'wave_height_m'             THEN h.wave_height_m             > r.threshold
              WHEN 'swell_power_kj_m'          THEN h.swell_power_kj_m          > r.threshold
              WHEN 'ocean_current_velocity_ms' THEN h.ocean_current_velocity_ms > r.threshold
              WHEN 'wind_wave_height_m'        THEN h.wind_wave_height_m        > r.threshold
            END
        END
    AND CASE r.key
          WHEN 'choppy' THEN
            h.wind_wave_height_m > coalesce(h.swell_wave_height_m, 0) * 1.5
          ELSE TRUE
        END;

  -- Per-(beach, rule) extreme over future hours. UNCHANGED from 20260606l.
  CREATE TEMP TABLE _extremes ON COMMIT DROP AS
  SELECT
    beach_fid, key, severity, label, icon, klass, text_tmpl, value_fmt, decimals,
    CASE agg WHEN 'min' THEN min(field_val) ELSE max(field_val) END AS extreme,
    min(ts) AS first_ts,
    max(ts) AS last_ts,
    count(*) AS hours_triggered
  FROM _triggered
  GROUP BY beach_fid, key, severity, label, icon, klass, text_tmpl, value_fmt, agg, decimals;

  -- UPSERT. UNCHANGED from 20260606l.
  WITH ins AS (
    INSERT INTO public.beach_advisory (
      beach_fid, advisory_key, source, event_type, severity,
      valid_from, valid_to, dog_impact_class, dog_impact_text,
      translation_source, label, value, icon, raw_data, fetched_at
    )
    SELECT
      e.beach_fid,
      format('marine:%s:%s', e.key, CURRENT_DATE),
      'marine_threshold',
      e.key,
      e.severity,
      e.first_ts,
      e.last_ts,
      e.klass,
      format(e.text_tmpl, round(e.extreme, e.decimals)::text),
      'rule',
      e.label,
      format(e.value_fmt, round(e.extreme, e.decimals)::text),
      e.icon,
      jsonb_build_object(
        'hours_triggered',  e.hours_triggered,
        'extreme_value',    e.extreme::float,
        'rule',             e.key,
        'scoring_version',  'v2_grid'
      ),
      v_now
    FROM _extremes e
    ON CONFLICT (beach_fid, advisory_key) DO UPDATE SET
      severity        = EXCLUDED.severity,
      valid_from      = EXCLUDED.valid_from,
      valid_to        = EXCLUDED.valid_to,
      dog_impact_text = EXCLUDED.dog_impact_text,
      value           = EXCLUDED.value,
      raw_data        = EXCLUDED.raw_data,
      fetched_at      = EXCLUDED.fetched_at
    RETURNING 1
  )
  SELECT count(*) INTO v_upserted FROM ins;

  -- Stale-row sweep. UNCHANGED from 20260606l, BUT now also leaves
  -- advisories for cold-cell beaches alone (since they didn't run this
  -- pass, retiring them would orphan correct prior-tick advisories).
  -- The fetched_at < v_now - 5s gate already handles this naturally:
  -- only sweep rows that DID fire this run.
  WITH del AS (
    DELETE FROM public.beach_advisory ba
    USING public.beaches_gold bg
    WHERE ba.beach_fid = bg.fid
      AND ba.source    = 'marine_threshold'
      AND bg.is_active
      AND bg.scoring_tier IN ('daily','hourly')
      AND (p_state IS NULL OR bg.state = upper(p_state))
      AND (p_fid   IS NULL OR bg.fid   = p_fid)
      AND ba.fetched_at < v_now - interval '5 seconds'
      -- NEW: only sweep within warm cells this run, so cold-cell
      -- advisories from prior ticks aren't auto-retired
      AND EXISTS (
        SELECT 1 FROM _warm_cells wc
         WHERE wc.grid_lat = bg.marine_grid_lat
           AND wc.grid_lon = bg.marine_grid_lon
      )
    RETURNING 1
  )
  SELECT count(*) INTO v_retired FROM del;

  RETURN QUERY SELECT v_upserted, v_retired;
END;
$function$;
