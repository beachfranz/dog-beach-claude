-- 2026-06-06 — Pure-SQL port of compute_marine_advisories.py.
--
-- Third instance of the SQL-native refresh template. Same shape as
-- 20260606e (refresh_*_advisories) but simpler:
--   * 4 threshold rules (cold water, paw cold, warm water, big surf)
--   * Direct numeric comparisons (no v2_signal_status band lookup)
--   * Severities are categorical per rule (not derived from bands)
--   * Single side of the table (beach_advisory, source='marine_threshold')
--   * Sparse scope (~30 coastal beaches with marine_forecast data)
--
-- Reads:  beach_marine_forecast (ts, wave_height_m, sst_c) for the
--         next `p_horizon_hours` hours from now()
-- Writes: beach_advisory  source='marine_threshold'  advisory_key='marine:{key}:{local_date}'
--
-- Stale-sweep watermark: same as advisory function — anything older than
-- v_now - 5s in scope gets retired. Catches:
--   * spike-passed (no future hours triggered)
--   * threshold flipped back (e.g. wave dropped below 1.2m)
--   * marine_forecast row went stale and dropped from horizon
--
-- IMPORTANT: today's beach_marine_forecast data is itself stale (last
-- loaded 2026-05-20). The data-loader is a separate concern; this
-- function will produce empty results until marine_forecast refresh
-- is fixed. Wiring this into Dagster MUST include the upstream loader
-- as a dep (or the asset will silently produce no rows).

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
  -- Self-clean temp tables.
  DROP TABLE IF EXISTS _rules, _hours, _triggered, _extremes;

  -- Threshold rules. (key, field, op, threshold, severity, label, icon,
  -- klass, text_tmpl, value_fmt, agg, decimals).
  -- value_fmt embeds the unit ('°C', 'm'); text_tmpl is the dog-impact body.
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
                                                                                  '%sm',  'max', 1)
  ) AS r(key, field, op, threshold, severity, label, icon, klass, text_tmpl, value_fmt, agg, decimals);

  -- Scope: marine_forecast rows for beaches in p_state / p_fid filter
  -- across the horizon.
  CREATE TEMP TABLE _hours ON COMMIT DROP AS
  SELECT mf.beach_fid, mf.ts, mf.wave_height_m, mf.sst_c
  FROM public.beach_marine_forecast mf
  JOIN public.beaches_gold bg ON bg.fid = mf.beach_fid
  WHERE bg.is_active
    AND bg.scoring_tier IN ('daily','hourly')
    AND mf.ts >= date_trunc('hour', v_now)
    AND mf.ts <  date_trunc('hour', v_now) + (p_horizon_hours || ' hours')::interval
    AND (p_state IS NULL OR bg.state = upper(p_state))
    AND (p_fid   IS NULL OR bg.fid   = p_fid);

  -- Unpivot to (fid, ts, rule, field_val) — one row per (hour, rule) pair
  -- where the rule fires.
  CREATE TEMP TABLE _triggered ON COMMIT DROP AS
  SELECT
    h.beach_fid, h.ts, r.key, r.field, r.op, r.threshold,
    r.severity, r.label, r.icon, r.klass, r.text_tmpl, r.value_fmt, r.agg, r.decimals,
    CASE r.field
      WHEN 'sst_c'         THEN h.sst_c::numeric
      WHEN 'wave_height_m' THEN h.wave_height_m::numeric
    END AS field_val
  FROM _hours h
  CROSS JOIN _rules r
  WHERE CASE r.field
          WHEN 'sst_c'         THEN h.sst_c
          WHEN 'wave_height_m' THEN h.wave_height_m
        END IS NOT NULL
    AND CASE r.op
          WHEN '<'  THEN CASE r.field WHEN 'sst_c'         THEN h.sst_c::numeric <  r.threshold
                                       WHEN 'wave_height_m' THEN h.wave_height_m::numeric <  r.threshold END
          WHEN '<=' THEN CASE r.field WHEN 'sst_c'         THEN h.sst_c::numeric <= r.threshold
                                       WHEN 'wave_height_m' THEN h.wave_height_m::numeric <= r.threshold END
          WHEN '>'  THEN CASE r.field WHEN 'sst_c'         THEN h.sst_c::numeric >  r.threshold
                                       WHEN 'wave_height_m' THEN h.wave_height_m::numeric >  r.threshold END
          WHEN '>=' THEN CASE r.field WHEN 'sst_c'         THEN h.sst_c::numeric >= r.threshold
                                       WHEN 'wave_height_m' THEN h.wave_height_m::numeric >= r.threshold END
        END;

  -- Extreme per (fid, rule) over the horizon. min for cold metrics, max
  -- for hot/big-surf. Envelope tracks first→last triggered ts.
  CREATE TEMP TABLE _extremes ON COMMIT DROP AS
  SELECT
    beach_fid, key, severity, label, icon, klass, text_tmpl, value_fmt, decimals,
    CASE agg WHEN 'min' THEN min(field_val) ELSE max(field_val) END AS extreme,
    min(ts) AS first_ts,
    max(ts) AS last_ts,
    count(*) AS hours_triggered
  FROM _triggered
  GROUP BY beach_fid, key, severity, label, icon, klass, text_tmpl, value_fmt, agg, decimals;

  -- UPSERT. advisory_key includes CURRENT_DATE so re-runs same-day
  -- refresh the same row; new day = new row.
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
        'scoring_version',  'v2'
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

  -- Stale sweep: anything in scope that didn't get its fetched_at bumped.
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

-- Usage:
--   SELECT * FROM public.refresh_marine_advisories();           -- all scored coastal beaches
--   SELECT * FROM public.refresh_marine_advisories('CA');       -- one state
--   SELECT * FROM public.refresh_marine_advisories(NULL, 8347); -- one beach
--   SELECT * FROM public.refresh_marine_advisories(NULL, NULL, 24);  -- 24-hour horizon
