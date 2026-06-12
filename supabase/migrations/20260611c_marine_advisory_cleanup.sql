-- 20260611c_marine_advisory_cleanup.sql
--
-- Three small advisory cleanups (Franz 2026-06-11):
--
-- 1. ALIAS sst_c → water_temp_f
--    `sst_c` (Celsius) is the canonical Open-Meteo Marine source value.
--    Add `water_temp_f` as a STORED generated column (Fahrenheit) for
--    consumer surfaces. Existing readers of `sst_c` keep working.
--
-- 2. RETIRE `choppy` advisory
--    Wind-wave-dominant signal is a fun-killer, not a safety signal.
--    Once wave_height_m lands as a v3 score input (Tier 2), the
--    "messy water" signal is implicit in the wave/swell math. Retire
--    the standalone advisory.
--
-- 3. RETUNE `too_warm_swim`
--    Threshold was 27°C ≈ 80.6°F — that's pleasant Caribbean water.
--    Bump to 30°C ≈ 86°F (true HAB / bath-water territory) and drop
--    severity from `moderate` to `minor`. Keeps the chip informational,
--    stops dragging the score on warm-but-fine swim days.
--
-- 4. CONVERT sst-based advisory display from °C to °F
--    The advisory chips render `value` and `dog_impact_text` strings.
--    Showing "Water 13°C" to a US dog owner is opaque; "Water 55°F" is
--    legible. Conversion only at the display step — comparisons still
--    use the °C source column.

begin;

-- ── 1. water_temp_f generated column ──────────────────────────────────

ALTER TABLE public.marine_grid_hourly
  ADD COLUMN IF NOT EXISTS water_temp_f numeric
    GENERATED ALWAYS AS (sst_c * 9.0 / 5.0 + 32) STORED;

COMMENT ON COLUMN public.marine_grid_hourly.water_temp_f IS
  'Sea-surface temperature in Fahrenheit, derived from sst_c. Consumer-facing alias — sst_c is the Open-Meteo Marine canonical source.';


-- ── 2–4. refresh_marine_advisories — drop choppy, retune too_warm,
--         convert sst displays to °F.
-- Function body otherwise identical to 20260606l_refresh_marine_advisories_via_grid.sql.

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

  -- (key, field, op, threshold, severity, label, icon, klass, text_tmpl,
  --  value_fmt, agg, decimals).
  CREATE TEMP TABLE _rules ON COMMIT DROP AS
  SELECT * FROM (VALUES
    -- Cold-water + paw-cold + warm-water — all sst_c. Display °F.
    ('too_cold_swim',  'sst_c',         '<',  13.0,  'moderate',  'Cold water', '❄️',  'skip_swim',
       'Water temp %s°F — too cold for swimming. Keep your dog out of the water.', '%s°F', 'min', 0),
    ('paw_cold',       'sst_c',         '<',  5.0,   'minor',     'Cold paws',  '🥶',  'cold_paws',
       'Cold ground — limit time on damp sand; small dogs may need booties.',     '%s°F', 'min', 0),
    -- RETUNED 2026-06-11: threshold 27°C → 30°C, severity moderate → minor.
    -- 27°C is pleasant swim water; 30°C is true HAB territory.
    ('too_warm_swim',  'sst_c',         '>',  30.0,  'minor',     'Warm water', '🌡️', 'review_required',
       'Water warm enough that algal blooms become a risk — confirm local water-quality before swimming.',
                                                                                  '%s°F', 'max', 0),
    ('swim_advisory',  'wave_height_m', '>',  1.2,   'moderate',  'Big surf',   '🌊',  'skip_swim',
       'Surf is up (%sm). Skip the swim — currents and waves too strong for safe dog play.',
                                                                                  '%sm',  'max', 1),
    ('powerful_surf',  'swell_power_kj_m', '>', 50.0, 'moderate', 'Powerful swell', '🌊', 'skip_swim',
       'Long-period swell (%s kJ/m wavefront). Even small waves punch hard; rip risk elevated. Skip the swim.',
                                                                                  '%s kJ/m', 'max', 0),
    ('strong_drift',   'ocean_current_velocity_ms', '>', 0.5, 'moderate', 'Strong drift', '🌀', 'skip_swim',
       'Ocean current %s m/s — drift risk for any dog more than ankle-deep. Keep her on the sand.',
                                                                                  '%s m/s', 'max', 1)
    -- RETIRED 2026-06-11: `choppy` — wave_height_m will carry the messy-water
    -- signal once wired into v3 scoring (Tier 2). Standalone advisory removed.
  ) AS r(key, field, op, threshold, severity, label, icon, klass, text_tmpl, value_fmt, agg, decimals);

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
        END;

  -- _extremes carries `field` so display step knows when to °C→°F convert.
  CREATE TEMP TABLE _extremes ON COMMIT DROP AS
  SELECT
    beach_fid, key, field, severity, label, icon, klass, text_tmpl, value_fmt, decimals,
    CASE agg WHEN 'min' THEN min(field_val) ELSE max(field_val) END AS extreme,
    min(ts) AS first_ts,
    max(ts) AS last_ts,
    count(*) AS hours_triggered
  FROM _triggered
  GROUP BY beach_fid, key, field, severity, label, icon, klass, text_tmpl, value_fmt, agg, decimals;

  -- Display: convert sst_c extreme to °F when formatting; round to nearest int.
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
      format(
        e.text_tmpl,
        CASE WHEN e.field = 'sst_c'
             THEN round(e.extreme * 9.0/5.0 + 32)::text
             ELSE round(e.extreme, e.decimals)::text
        END
      ),
      'rule',
      e.label,
      format(
        e.value_fmt,
        CASE WHEN e.field = 'sst_c'
             THEN round(e.extreme * 9.0/5.0 + 32)::text
             ELSE round(e.extreme, e.decimals)::text
        END
      ),
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

  -- Stale-row sweep: retires any choppy + over-threshold too_warm_swim
  -- rows from earlier ticks (this run won't re-upsert them, so they age out).
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
    RETURNING 1
  )
  SELECT count(*) INTO v_retired FROM del;

  RETURN QUERY SELECT v_upserted, v_retired;
END;
$function$;

commit;
notify pgrst, 'reload schema';
