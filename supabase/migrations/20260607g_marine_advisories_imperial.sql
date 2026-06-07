-- 20260607g_marine_advisories_imperial.sql
--
-- Imperial across consumer copy. American app to start; metric toggle
-- is a parked Phase 2.
--
-- Audit of active beach_advisory templates (2026-06-07) showed only
-- marine_threshold rules carry metric units. Everything else
-- (asphalt_status, car_heat_status, sand_status, temp_*, wind_status,
-- etc.) was already in °F / mph / %. So this migration scopes narrowly:
-- format SST in °F and wave height in feet inside
-- refresh_marine_advisories. The underlying marine_grid_hourly storage
-- stays metric (single source of truth, Open-Meteo native), but the
-- generated dog_impact_text + value strings render imperial.
--
-- Affected templates:
--   * too_cold_swim    "Water temp 13°C ..."     →  "Water temp 55°F ..."
--   * paw_cold         no value in template       →  unchanged
--   * too_warm_swim    no value in template       →  unchanged
--   * swim_advisory    "Surf is up (1.6m)..."     →  "Surf is up (5.2ft)..."
--   * powerful_surf    "(50 kJ/m wavefront)"      →  unchanged (energy density,
--                                                     no imperial equiv used in
--                                                     surf forecasting)
--   * choppy           "Choppy (0.8m)..."         →  "Choppy (2.6ft)..."
--
-- Note: thresholds stay metric so behavior (which beaches fire which
-- advisory) is unchanged — only display switches. This avoids a
-- behavioral change masked as a copy change.
--
-- After applying: SELECT public.refresh_marine_advisories(); to
-- re-render existing rows immediately (watermark sweep would otherwise
-- take up to the next refresh tick to catch up).
--
-- Toggle (°F / °C, ft / m): not in this migration. When we want it,
-- the cleanest path is to store extreme + unit_class in raw_data
-- (already does this for extreme_value) and have the consumer format,
-- so toggle = one localStorage flag + render-time conversion. Today's
-- imperial values pre-rendered into dog_impact_text would need to be
-- re-derived on the client — small refactor, deferred until there's
-- actual demand.

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

  -- Thresholds stay metric (compared against marine_grid_hourly which is
  -- metric); display strings format imperial. The 'imperial_kind' column
  -- drives the per-rule conversion: 'temp_c_to_f' / 'len_m_to_ft' / 'none'.
  CREATE TEMP TABLE _rules ON COMMIT DROP AS
  SELECT * FROM (VALUES
    ('too_cold_swim',  'sst_c',         '<',  13.0,  'moderate',  'Cold water', '❄️',  'skip_swim',
       'Water temp %s°F — too cold for swimming. Keep your dog out of the water.', '%s°F', 'min', 0, 'temp_c_to_f'),
    ('paw_cold',       'sst_c',         '<',  5.0,   'minor',     'Cold paws',  '🥶',  'cold_paws',
       'Cold ground — limit time on damp sand; small dogs may need booties.',     '%s°F', 'min', 0, 'temp_c_to_f'),
    ('too_warm_swim',  'sst_c',         '>',  27.0,  'moderate',  'Warm water', '🌡️', 'review_required',
       'Water warm enough that algal blooms become a risk — confirm local water-quality before swimming.',
                                                                                  '%s°F', 'max', 0, 'temp_c_to_f'),
    ('swim_advisory',  'wave_height_m', '>',  1.2,   'moderate',  'Big surf',   '🌊',  'skip_swim',
       'Surf is up (%sft). Skip the swim — currents and waves too strong for safe dog play.',
                                                                                  '%sft', 'max', 1, 'len_m_to_ft'),
    ('powerful_surf',  'swell_power_kj_m', '>', 50.0, 'moderate', 'Powerful swell', '🌊', 'skip_swim',
       'Long-period swell (%s kJ/m wavefront). Even small waves punch hard; rip risk elevated. Skip the swim.',
                                                                                  '%s kJ/m', 'max', 0, 'none'),
    ('choppy',         'wind_wave_height_m', '>', 0.8, 'minor', 'Choppy', '💨', 'review_required',
       'Choppy wind-driven waves (%sft) — messy water, less fun for play, harder visibility.',
                                                                                  '%sft', 'max', 1, 'len_m_to_ft')
  ) AS r(key, field, op, threshold, severity, label, icon, klass, text_tmpl, value_fmt, agg, decimals, imperial_kind);

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
    r.imperial_kind,
    CASE r.field
      WHEN 'sst_c'                     THEN h.sst_c
      WHEN 'wave_height_m'             THEN h.wave_height_m
      WHEN 'swell_power_kj_m'          THEN h.swell_power_kj_m
      WHEN 'wind_wave_height_m'        THEN h.wind_wave_height_m
    END AS field_val
  FROM _hours h
  CROSS JOIN _rules r
  WHERE CASE r.field
          WHEN 'sst_c'                     THEN h.sst_c
          WHEN 'wave_height_m'             THEN h.wave_height_m
          WHEN 'swell_power_kj_m'          THEN h.swell_power_kj_m
          WHEN 'wind_wave_height_m'        THEN h.wind_wave_height_m
        END IS NOT NULL
    AND CASE r.op
          WHEN '<'  THEN
            CASE r.field
              WHEN 'sst_c'                     THEN h.sst_c                     < r.threshold
              WHEN 'wave_height_m'             THEN h.wave_height_m             < r.threshold
              WHEN 'swell_power_kj_m'          THEN h.swell_power_kj_m          < r.threshold
              WHEN 'wind_wave_height_m'        THEN h.wind_wave_height_m        < r.threshold
            END
          WHEN '>'  THEN
            CASE r.field
              WHEN 'sst_c'                     THEN h.sst_c                     > r.threshold
              WHEN 'wave_height_m'             THEN h.wave_height_m             > r.threshold
              WHEN 'swell_power_kj_m'          THEN h.swell_power_kj_m          > r.threshold
              WHEN 'wind_wave_height_m'        THEN h.wind_wave_height_m        > r.threshold
            END
        END
    AND CASE r.key
          WHEN 'choppy' THEN
            h.wind_wave_height_m > coalesce(h.swell_wave_height_m, 0) * 1.5
          ELSE TRUE
        END;

  CREATE TEMP TABLE _extremes ON COMMIT DROP AS
  SELECT
    beach_fid, key, severity, label, icon, klass, text_tmpl, value_fmt, decimals,
    imperial_kind,
    CASE agg WHEN 'min' THEN min(field_val) ELSE max(field_val) END AS extreme,
    min(ts) AS first_ts,
    max(ts) AS last_ts,
    count(*) AS hours_triggered
  FROM _triggered
  GROUP BY beach_fid, key, severity, label, icon, klass, text_tmpl, value_fmt, agg, decimals, imperial_kind;

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
      -- Convert metric extreme to imperial for display per-rule kind.
      -- text_tmpl and value_fmt are already imperial in _rules above.
      format(e.text_tmpl,
        round(
          CASE e.imperial_kind
            WHEN 'temp_c_to_f' THEN e.extreme * 1.8 + 32
            WHEN 'len_m_to_ft' THEN e.extreme * 3.28084
            ELSE e.extreme
          END,
          e.decimals
        )::text
      ),
      'rule',
      e.label,
      format(e.value_fmt,
        round(
          CASE e.imperial_kind
            WHEN 'temp_c_to_f' THEN e.extreme * 1.8 + 32
            WHEN 'len_m_to_ft' THEN e.extreme * 3.28084
            ELSE e.extreme
          END,
          e.decimals
        )::text
      ),
      e.icon,
      jsonb_build_object(
        'hours_triggered',  e.hours_triggered,
        'extreme_value',    e.extreme::float,         -- METRIC (source of truth, drives future toggle)
        'extreme_unit',     CASE e.imperial_kind
                              WHEN 'temp_c_to_f' THEN 'C'
                              WHEN 'len_m_to_ft' THEN 'm'
                              ELSE NULL
                            END,
        'imperial_kind',    e.imperial_kind,
        'rule',             e.key,
        'scoring_version',  'v2_grid_imperial'
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

-- Force a refresh so existing rows render in imperial immediately
-- rather than waiting for the next hourly tick.
SELECT public.refresh_marine_advisories();
