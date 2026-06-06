-- 20260606q_strong_drift_honest_copy.sql
--
-- Honesty re-copy on the strong_drift advisory, rebased onto the
-- watermark-sweep cleanup (20260606p_advisory_watermark_sweep_orphans.sql).
--
-- Origin: an untracked working-tree draft (`20260606p_strong_drift_recopy_regional.sql`)
-- had the right intent but recreated refresh_marine_advisories from the
-- pre-cleanup version, which would have reverted task 2 of today's
-- cleanup pass. This migration is the same content rebased onto the
-- cleaned-up watermark sweep.
--
-- Why the honesty pass:
--   Old copy implied shore-scale swimmer-safety risk ("drift risk for any
--   dog more than ankle-deep. Keep her on the sand"). The underlying signal
--   is Open-Meteo / CMEMS ocean_current_velocity_ms — a bulk regional
--   surface current at ~0.1° (~11km) cell resolution. That's the
--   California Current branch passing by, not the surf-zone current a dog
--   wading at the shore actually feels. Re-copy reflects the regional
--   nature without claiming shore-scale risk. Matches [[honest-brand-prefers-candor]].
--
-- Changes (label + dog_impact_text only):
--   * label:       'Strong drift'  -> 'Regional current'
--   * text tmpl:   'Ocean current %s m/s — drift risk for any dog more
--                   than ankle-deep. Keep her on the sand.'
--               -> 'Regional ocean current %s m/s offshore today. Bulk
--                   flow at the marine-grid scale; local shore conditions
--                   may vary.'
--
-- Threshold (>0.5 m/s), severity (moderate), klass ('skip_swim'),
-- icon (🌀), and rule key ('strong_drift') unchanged.
--
-- The ON CONFLICT DO UPDATE clause now refreshes `label` too, so existing
-- strong_drift advisory rows re-render with the new label on the next fn
-- call — without that line, only newly-inserted rows would get the new
-- label and consumers would see a mix of "Strong drift" and "Regional
-- current" until the old rows expired.

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
  DROP TABLE IF EXISTS _rules, _hours, _triggered, _extremes;

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
    -- Re-copied 2026-06-06: regional bulk current, not shore-scale drift risk.
    ('strong_drift',   'ocean_current_velocity_ms', '>', 0.5, 'moderate', 'Regional current', '🌀', 'skip_swim',
       'Regional ocean current %s m/s offshore today. Bulk flow at the marine-grid scale; local shore conditions may vary.',
                                                                                  '%s m/s', 'max', 1),
    ('choppy',         'wind_wave_height_m', '>', 0.8, 'minor', 'Choppy', '💨', 'review_required',
       'Choppy wind-driven waves (%sm) — messy water, less fun for play, harder visibility.',
                                                                                  '%sm', 'max', 1)
  ) AS r(key, field, op, threshold, severity, label, icon, klass, text_tmpl, value_fmt, agg, decimals);

  CREATE TEMP TABLE _hours ON COMMIT DROP AS
  SELECT bg.fid AS beach_fid, mgh.forecast_ts AS ts,
         mgh.wave_height_m, mgh.sst_c, mgh.swell_power_kj_m,
         mgh.ocean_current_velocity_ms, mgh.wind_wave_height_m,
         mgh.swell_wave_height_m
  FROM public.beaches_gold bg
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
          WHEN 'choppy' THEN h.wind_wave_height_m > coalesce(h.swell_wave_height_m, 0) * 1.5
          ELSE TRUE
        END;

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
      translation_source, label, value, icon, raw_data, fetched_at
    )
    SELECT e.beach_fid,
      format('marine:%s:%s', e.key, CURRENT_DATE),
      'marine_threshold', e.key, e.severity,
      e.first_ts, e.last_ts, e.klass,
      format(e.text_tmpl, round(e.extreme, e.decimals)::text), 'rule', e.label,
      format(e.value_fmt, round(e.extreme, e.decimals)::text), e.icon,
      jsonb_build_object('hours_triggered', e.hours_triggered,
                         'extreme_value', e.extreme::float,
                         'rule', e.key, 'scoring_version', 'v2_grid'),
      v_now
    FROM _extremes e
    ON CONFLICT (beach_fid, advisory_key) DO UPDATE SET
      severity        = EXCLUDED.severity,
      valid_from      = EXCLUDED.valid_from,
      valid_to        = EXCLUDED.valid_to,
      dog_impact_text = EXCLUDED.dog_impact_text,
      label           = EXCLUDED.label,     -- new: refreshes label on existing rows
      value           = EXCLUDED.value,
      raw_data        = EXCLUDED.raw_data,
      fetched_at      = EXCLUDED.fetched_at
    RETURNING 1
  )
  SELECT count(*) INTO v_upserted FROM ins;

  -- Watermark sweep — cleaned-up scope (no is_active / scoring_tier
  -- filter; see 20260606p_advisory_watermark_sweep_orphans.sql).
  WITH del AS (
    DELETE FROM public.beach_advisory ba
    USING public.beaches_gold bg
    WHERE ba.beach_fid = bg.fid
      AND ba.source = 'marine_threshold'
      AND (p_state IS NULL OR bg.state = upper(p_state))
      AND (p_fid   IS NULL OR bg.fid   = p_fid)
      AND ba.fetched_at < v_now - interval '5 seconds'
    RETURNING 1
  )
  SELECT count(*) INTO v_retired FROM del;

  RETURN QUERY SELECT v_upserted, v_retired;
END;
$function$;
