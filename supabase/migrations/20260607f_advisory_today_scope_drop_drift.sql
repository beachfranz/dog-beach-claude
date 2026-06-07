-- 20260607f_advisory_today_scope_drop_drift.sql
--
-- Three fixes to the beach advisory surface, all flagged by Franz
-- 2026-06-07 evening:
--
-- 1. "Regional current is on every beach — it's noise at the caution
--    level." Drop strong_drift. The rule fires from
--    ocean_current_velocity_ms > 0.5 m/s, which is the resting state of
--    California Current. 2,021 of 23,782 active advisories (8.5%) were
--    strong_drift. We already renamed it to "Regional current" earlier
--    today as an honest re-copy (commit e8184ec), but honesty doesn't
--    fix noise — Franz called it: kill the rule.
--
-- 2. "Only show cautions for today, not tomorrow." get_beach_advisories
--    default horizon_hours=24 meant a 1pm Pacific request returned
--    advisories starting up to 1pm tomorrow Pacific. Cautions card on
--    beach.html bled into tomorrow's morning. Scope reads to TODAY at
--    the BEACH's timezone (not the caller's wall clock).
--
-- 3. "If an alert bleeds into the next day, chop it at midnight." Big
--    surf advisories often span 47+ hours. Today's caution card
--    showed valid_to=06-09 (day after tomorrow). Clip valid_from /
--    valid_to to today's local bounds so the rendered window reads
--    "starts X, ends midnight" not "ends 9pm Monday."
--
-- Sibling: dog parks don't have a public get_dog_park_advisories RPC,
-- so the [[paired-functions-port-fixes-both-sides]] sweep doesn't
-- apply on this pass. If a dog-park read path is added later it should
-- get the same today-scope + midnight-chop treatment.

-- ─────────────────────────────────────────────────────────────────────────
-- 1. Drop strong_drift from refresh_marine_advisories rule list.
-- ─────────────────────────────────────────────────────────────────────────
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

  -- Threshold rules. strong_drift removed 2026-06-07 — was firing on
  -- 8.5% of all advisories because the 0.1° regional ocean_current
  -- signal commonly sits >0.5 m/s along the California Current. Noise
  -- at the caution level per Franz. If we ever get true surf-zone
  -- drift data (NOAA station current OR finer-grain Open-Meteo), it
  -- can come back as a real signal.
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
    ('choppy',         'wind_wave_height_m', '>', 0.8, 'minor', 'Choppy', '💨', 'review_required',
       'Choppy wind-driven waves (%sm) — messy water, less fun for play, harder visibility.',
                                                                                  '%sm', 'max', 1)
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
    CASE agg WHEN 'min' THEN min(field_val) ELSE max(field_val) END AS extreme,
    min(ts) AS first_ts,
    max(ts) AS last_ts,
    count(*) AS hours_triggered
  FROM _triggered
  GROUP BY beach_fid, key, severity, label, icon, klass, text_tmpl, value_fmt, agg, decimals;

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


-- ─────────────────────────────────────────────────────────────────────────
-- 2. Purge existing strong_drift rows. The watermark sweep above would
--    delete them on next refresh, but that takes ~12 hours to fire.
--    Wipe them now so users stop seeing the chip immediately.
-- ─────────────────────────────────────────────────────────────────────────
DELETE FROM public.beach_advisory WHERE event_type = 'strong_drift';


-- ─────────────────────────────────────────────────────────────────────────
-- 3. Today-scope + midnight chop in get_beach_advisories.
--
-- "Today" = the calendar date at the BEACH's local timezone (not the
-- caller's wall clock). A San Diego viewer querying an HI beach gets
-- HI's today, which is what they want — the cautions are about
-- conditions AT THE BEACH.
--
-- Filter: advisory window [valid_from, valid_to] must overlap today
-- AND valid_to > now() (existing "still in effect" gate).
-- Display: clip the returned valid_from / valid_to to today's bounds
-- so renderers don't need to do their own chop math.
--
-- horizon_hours arg kept on the signature for backwards-compat but
-- IGNORED — semantics are pure today-at-beach-tz now.
-- ─────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.get_beach_advisories(
  p_fid           bigint,
  p_horizon_hours integer DEFAULT 24  -- kept for sig compat; ignored
)
RETURNS jsonb
LANGUAGE sql STABLE PARALLEL SAFE SECURITY DEFINER SET search_path TO 'public'
AS $function$
  WITH bounds AS (
    SELECT
      COALESCE(bg.timezone, 'America/Los_Angeles') AS tz,
      ((now() AT TIME ZONE COALESCE(bg.timezone, 'America/Los_Angeles'))::date::timestamp)
        AT TIME ZONE COALESCE(bg.timezone, 'America/Los_Angeles') AS today_start_utc,
      (((now() AT TIME ZONE COALESCE(bg.timezone, 'America/Los_Angeles'))::date + 1)::timestamp)
        AT TIME ZONE COALESCE(bg.timezone, 'America/Los_Angeles') AS today_end_utc
    FROM public.beaches_gold bg
    WHERE bg.fid = p_fid
    LIMIT 1
  )
  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'source',             a.source,
    'severity',           a.severity,
    'event_type',         a.event_type,
    'dog_impact_class',   a.dog_impact_class,
    'dog_impact_text',    a.dog_impact_text,
    'label',              a.label,
    'value',              a.value,
    'icon',               a.icon,
    'valid_from',         GREATEST(a.valid_from, b.today_start_utc),
    'valid_to',           LEAST(a.valid_to, b.today_end_utc),
    'translation_source', a.translation_source,
    'raw_data',           a.raw_data
  ) ORDER BY public._advisory_severity_rank(a.severity) DESC, a.valid_from ASC),
                  '[]'::jsonb)
    FROM public.beach_advisory a
    CROSS JOIN bounds b
   WHERE a.beach_fid   = p_fid
     AND a.event_type  <> 'strong_drift'             -- defense-in-depth
     AND a.valid_to    >  b.today_start_utc          -- overlaps today
     AND a.valid_from  <  b.today_end_utc
     AND a.valid_to    >  now()                      -- still in effect
$function$;

GRANT EXECUTE ON FUNCTION public.get_beach_advisories(bigint, integer) TO anon, authenticated;
