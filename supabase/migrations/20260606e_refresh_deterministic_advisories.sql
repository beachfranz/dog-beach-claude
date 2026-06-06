-- 2026-06-06 — Pure-SQL port of compute_weather_advisories.py +
-- compute_dog_park_advisories.py. Replaces a ~90-minute Python loop with
-- two functions that finish in seconds.
--
-- One round-trip per call. Schedulable as a Dagster asset, pg_cron job,
-- or one-line edge function. Stdout buffering on Windows can't bite
-- because there's no Python in the hot path.
--
-- Functions:
--   refresh_beach_advisories(p_state text DEFAULT NULL, p_fid bigint DEFAULT NULL)
--     → 11 hourly weather metrics + bacteria_risk daily, upserts to
--       public.beach_advisory, retires stale rows. Returns (upserted, retired).
--   refresh_dog_park_advisories(p_state text DEFAULT NULL, p_fid bigint DEFAULT NULL)
--     → 8 hourly weather metrics + static `unfenced` flag, upserts to
--       public.dog_park_advisory, retires stale. Returns (upserted, retired).
--
-- Both rely on:
--   - public.v2_signal_status(entity_type, signal_key, raw)  — band lookup
--   - public.scoring_config_v2.bands                          — thresholds
--   - public.beach_day_hourly_scores / dog_park_day_hourly_scores — raw
--   - public.beach_day_recommendations.bacteria_risk          — daily

-- ─────────────────────────────────────────────────────────────────────────
-- 1. Beach advisories
-- ─────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.refresh_beach_advisories(
  p_state text DEFAULT NULL,
  p_fid   bigint DEFAULT NULL
) RETURNS TABLE(upserted bigint, retired bigint)
LANGUAGE plpgsql AS $function$
DECLARE
  v_now timestamptz := now();
  v_upserted bigint := 0;
  v_retired  bigint := 0;
BEGIN
  -- Self-clean temp tables. ON COMMIT DROP only fires at transaction end,
  -- so callers that invoke this function in a loop inside one transaction
  -- would otherwise hit "relation already exists" on the second call.
  DROP TABLE IF EXISTS _metrics, _hours, _long, _per_event, _extremes;

  -- Metrics dictionary. The %s in text_tmpl is replaced by the extreme
  -- value (rounded per `decimals`); the %s in unit_fmt is the same value
  -- formatted as the chip label. agg='min' takes the COLDEST hour.
  CREATE TEMP TABLE _metrics ON COMMIT DROP AS
  SELECT * FROM (VALUES
    -- (event_type, signal_key, raw_col, label, icon, klass, text_tmpl, unit_fmt, agg, decimals)
    ('sand_status',     'sand_temp_neg',  'sand_temp',     'Hot sand',    '🪣',  'paws_warning',
       'Sand will hit %s°F — paws will burn. Go dawn or dusk.',                                        '%s°F',   'max', 0),
    ('asphalt_status',  'asphalt_neg',    'asphalt_temp',  'Hot asphalt', '🚶',  'paws_warning',
       'Parking-lot asphalt %s°F — booties for the walk in.',                                           '%s°F',   'max', 0),
    ('uv_status',       'uv_neg',         'uv_index',      'High UV',     '☀️',  'review_required',
       'UV peaks at %s — sunscreen for you, shade breaks for the pup.',                                 '%s',     'max', 0),
    ('wind_status',     'wind_harsh_neg', 'wind_speed',    'Strong wind', '💨',  'blowing_sand',
       'Wind gusts %smph — blowing sand will sting.',                                                   '%smph',  'max', 0),
    ('tide_status',     'tide_neg',       'tide_height',   'High tide',   '🌊',  'skip_swim',
       'High tide ≥%sft — limited beach to walk on.',                                                   '%sft',   'max', 1),
    ('rain_status',     'precip_chance',  'precip_chance', 'Rain',        '🌧️', 'review_required',
       'Rain likely (%s%% chance) — bring a towel.',                                                    '%s%%',   'max', 0),
    ('temp_hot_status', 'feels_like_hot', 'feels_like',    'Heat',        '🥵',  'paws_warning',
       'Hot day (feels like %s°F) — dawn or dusk, plenty of water.',                                    '%s°F',   'max', 0),
    ('temp_cold_status','feels_like_cold','feels_like',    'Cold',        '🥶',  'cold_paws',
       'Cold (feels like %s°F) — short coats may need a jacket.',                                       '%s°F',   'min', 0),
    ('car_heat_status', 'car_heat_neg',   'temp_air',      'Hot car',     '🚗',  'skip_car',
       '%s°F outside — don''t leave your dog in a parked car. Interior climbs to 100°F+ within 10 min, fatal heatstroke risk in 15.',
                                                                                                         '%s°F',   'max', 0),
    ('car_cold_status', 'car_cold_neg',   'temp_air',      'Cold car',    '🥶',  'skip_car',
       '%s°F outside — don''t leave your dog in a parked car. Interior cools to ambient quickly; hypothermia risk for short coats.',
                                                                                                         '%s°F',   'min', 0),
    ('crowd_status',    'crowd_neg',      'busyness_score','Crowded',     '👥',  'review_required',
       'Beach is busy (score %s) — reactive dogs may struggle.',                                        '%s',     'max', 0)
  ) AS m(event_type, signal_key, raw_col, label, icon, klass, text_tmpl, unit_fmt, agg, decimals);

  -- 2. Scope + hour rows (one query, no per-fid round-trip).
  CREATE TEMP TABLE _hours ON COMMIT DROP AS
  SELECT bg.fid AS beach_fid, bg.location_id,
         h.local_date, h.local_hour, h.forecast_ts,
         h.sand_temp, h.asphalt_temp, h.uv_index, h.wind_speed,
         h.tide_height, h.precip_chance, h.feels_like, h.temp_air,
         h.busyness_score
  FROM public.beaches_gold bg
  JOIN public.beach_day_hourly_scores h
    ON h.location_id = bg.location_id
  WHERE bg.is_active
    AND bg.scoring_tier IN ('daily','hourly')
    AND bg.location_id IS NOT NULL
    AND (p_state IS NULL OR bg.state = upper(p_state))
    AND (p_fid   IS NULL OR bg.fid   = p_fid)
    AND h.local_date BETWEEN CURRENT_DATE AND CURRENT_DATE + 1;

  -- 3. Unpivot to one row per (fid, date, hour, metric) with v2 band status.
  CREATE TEMP TABLE _long ON COMMIT DROP AS
  SELECT h.beach_fid, h.local_date, h.local_hour, h.forecast_ts,
         m.event_type, m.agg, m.decimals,
         CASE m.raw_col
           WHEN 'sand_temp'      THEN h.sand_temp::numeric
           WHEN 'asphalt_temp'   THEN h.asphalt_temp::numeric
           WHEN 'uv_index'       THEN h.uv_index::numeric
           WHEN 'wind_speed'     THEN h.wind_speed::numeric
           WHEN 'tide_height'    THEN h.tide_height::numeric
           WHEN 'precip_chance'  THEN h.precip_chance::numeric
           WHEN 'feels_like'     THEN h.feels_like::numeric
           WHEN 'temp_air'       THEN h.temp_air::numeric
           WHEN 'busyness_score' THEN h.busyness_score::numeric
         END AS raw_val,
         public.v2_signal_status('beach', m.signal_key,
           CASE m.raw_col
             WHEN 'sand_temp'      THEN h.sand_temp::numeric
             WHEN 'asphalt_temp'   THEN h.asphalt_temp::numeric
             WHEN 'uv_index'       THEN h.uv_index::numeric
             WHEN 'wind_speed'     THEN h.wind_speed::numeric
             WHEN 'tide_height'    THEN h.tide_height::numeric
             WHEN 'precip_chance'  THEN h.precip_chance::numeric
             WHEN 'feels_like'     THEN h.feels_like::numeric
             WHEN 'temp_air'       THEN h.temp_air::numeric
             WHEN 'busyness_score' THEN h.busyness_score::numeric
           END) AS status_v2
  FROM _hours h
  CROSS JOIN _metrics m;

  -- 4. Worst severity per (fid, date, event_type) over the FULL day —
  --    used for advisory_key so re-runs in the same day refresh the
  --    same row even if the spike has passed.
  CREATE TEMP TABLE _per_event ON COMMIT DROP AS
  SELECT beach_fid, local_date, event_type, agg, decimals,
         CASE max(CASE status_v2 WHEN 'no_go'    THEN 3
                                  WHEN 'caution'  THEN 2
                                  WHEN 'advisory' THEN 1
                                  ELSE 0 END)
           WHEN 3 THEN 'no_go'
           WHEN 2 THEN 'caution'
           WHEN 1 THEN 'advisory'
         END AS worst_status
  FROM _long
  WHERE coalesce(status_v2,'clear') IN ('advisory','caution','no_go')
  GROUP BY beach_fid, local_date, event_type, agg, decimals
  HAVING max(CASE status_v2 WHEN 'no_go' THEN 3 WHEN 'caution' THEN 2 WHEN 'advisory' THEN 1 ELSE 0 END) >= 1;

  -- 5. Extreme value + envelope over FUTURE-ONLY triggered hours.
  --    Events whose spikes already passed don't produce a row here →
  --    the stale-DELETE in step 7 cleans them up.
  CREATE TEMP TABLE _extremes ON COMMIT DROP AS
  SELECT l.beach_fid, l.local_date, l.event_type,
         CASE l.agg WHEN 'min' THEN min(l.raw_val) ELSE max(l.raw_val) END AS extreme,
         min(l.forecast_ts) AS first_ts,
         max(l.forecast_ts) AS last_ts,
         count(*)            AS hours_triggered
  FROM _long l
  WHERE l.forecast_ts >= v_now
    AND coalesce(l.status_v2,'clear') IN ('advisory','caution','no_go')
    AND l.raw_val IS NOT NULL
  GROUP BY l.beach_fid, l.local_date, l.event_type, l.agg;

  -- 6. UPSERT.
  WITH upserts AS (
    SELECT pe.beach_fid, pe.local_date, pe.event_type,
           pe.worst_status, pe.decimals,
           CASE pe.worst_status WHEN 'no_go' THEN 'severe'
                                WHEN 'caution' THEN 'moderate'
                                ELSE 'minor' END AS severity,
           e.extreme, e.first_ts, e.last_ts, e.hours_triggered,
           m.label, m.icon, m.klass, m.text_tmpl, m.unit_fmt
    FROM _per_event pe
    JOIN _extremes  e  USING (beach_fid, local_date, event_type)
    JOIN _metrics   m  USING (event_type)
  ),
  ins AS (
    INSERT INTO public.beach_advisory (
      beach_fid, advisory_key, source, event_type, severity,
      valid_from, valid_to, dog_impact_class, dog_impact_text,
      translation_source, label, value, icon, raw_data, fetched_at
    )
    SELECT
      u.beach_fid,
      format('det:%s_%s:%s', u.event_type, u.worst_status, u.local_date),
      'deterministic_weather',
      u.event_type,
      u.severity,
      u.first_ts,
      u.last_ts,
      u.klass,
      format(u.text_tmpl, round(u.extreme, u.decimals)::text),
      'rule',
      u.label,
      format(u.unit_fmt, round(u.extreme, u.decimals)::text),
      u.icon,
      jsonb_build_object(
        'hours_triggered', u.hours_triggered,
        'extreme_value',   u.extreme::float,
        'worst_v2_status', u.worst_status,
        'scoring_version', 'v2'
      ),
      v_now
    FROM upserts u
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

  -- 7. Bacteria — separate metric, daily field on beach_day_recommendations.
  WITH bact AS (
    SELECT bg.fid AS beach_fid, dr.local_date, dr.bacteria_risk
    FROM public.beaches_gold bg
    JOIN public.beach_day_recommendations dr ON dr.location_id = bg.location_id
    WHERE bg.is_active
      AND bg.scoring_tier IN ('daily','hourly')
      AND bg.location_id IS NOT NULL
      AND (p_state IS NULL OR bg.state = upper(p_state))
      AND (p_fid   IS NULL OR bg.fid   = p_fid)
      AND dr.local_date BETWEEN CURRENT_DATE AND CURRENT_DATE + 1
      AND dr.bacteria_risk IN ('moderate','high')
  ),
  ins_bact AS (
    INSERT INTO public.beach_advisory (
      beach_fid, advisory_key, source, event_type, severity,
      valid_from, valid_to, dog_impact_class, dog_impact_text,
      translation_source, label, value, icon, raw_data, fetched_at
    )
    SELECT
      b.beach_fid,
      format('det:bacteria_%s:%s', b.bacteria_risk, b.local_date),
      'deterministic_bacteria',
      'bacteria_risk',
      CASE b.bacteria_risk WHEN 'high' THEN 'severe' ELSE 'moderate' END,
      b.local_date::timestamptz,
      (b.local_date + interval '1 day')::timestamptz,
      'skip_swim',
      format('Water-quality %s risk (recent rain) — keep your dog out of the surf.', b.bacteria_risk),
      'rule',
      'Water-quality risk',
      b.bacteria_risk,
      '⚠️',
      jsonb_build_object('bacteria_risk', b.bacteria_risk),
      v_now
    FROM bact b
    ON CONFLICT (beach_fid, advisory_key) DO UPDATE SET
      severity   = EXCLUDED.severity,
      valid_from = EXCLUDED.valid_from,
      valid_to   = EXCLUDED.valid_to,
      fetched_at = EXCLUDED.fetched_at
    RETURNING 1
  )
  SELECT v_upserted + count(*) INTO v_upserted FROM ins_bact;

  -- 8. Retire stale rows: any deterministic-source row in the scope whose
  --    fetched_at didn't get bumped to v_now this run. Covers (a) spike
  --    already in the past, (b) bacteria flipped back to 'low', (c) older
  --    pre-v2 rows whose advisory_key shape is gone.
  WITH del AS (
    DELETE FROM public.beach_advisory ba
    USING public.beaches_gold bg
    WHERE ba.beach_fid = bg.fid
      AND ba.source IN ('deterministic_weather','deterministic_bacteria')
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
-- 2. Dog-park advisories
-- ─────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.refresh_dog_park_advisories(
  p_state text DEFAULT NULL,
  p_fid   bigint DEFAULT NULL
) RETURNS TABLE(upserted bigint, retired bigint)
LANGUAGE plpgsql AS $function$
DECLARE
  v_now timestamptz := now();
  v_upserted bigint := 0;
  v_retired  bigint := 0;
BEGIN
  -- Self-clean temp tables (see note in refresh_beach_advisories).
  DROP TABLE IF EXISTS _metrics, _hours, _long, _per_event, _extremes, _unfenced;

  CREATE TEMP TABLE _metrics ON COMMIT DROP AS
  SELECT * FROM (VALUES
    ('asphalt_status',  'asphalt_neg',    'asphalt_temp',  'Hot asphalt', '🐾',  'paws_warning',
       'Parking-lot asphalt %s°F — booties for the walk in.',                                           '%s°F',   'max', 0),
    ('uv_status',       'uv_neg',         'uv_index',      'High UV',     '☀️',  'review_required',
       'UV peaks at %s — sunscreen for you, shade breaks for the pup.',                                 '%s',     'max', 0),
    ('wind_status',     'wind_harsh_neg', 'wind_speed',    'Strong wind', '💨',  'blowing_sand',
       'Wind gusts %smph — secure leashes when leaving; dusty conditions likely.',                      '%smph',  'max', 0),
    ('rain_status',     'precip_chance',  'precip_chance', 'Rain',        '🌧️', 'review_required',
       'Rain likely (%s%% chance) — bring a towel.',                                                    '%s%%',   'max', 0),
    ('temp_hot_status', 'feels_like_hot', 'feels_like',    'Heat',        '🌡️', 'paws_warning',
       'Hot (feels like %s°F) — heat stress risk; short visits + plenty of water.',                     '%s°F',   'max', 0),
    ('temp_cold_status','feels_like_cold','feels_like',    'Cold',        '❄️',  'cold_paws',
       'Cold (feels like %s°F) — short coats may need a jacket.',                                       '%s°F',   'min', 0),
    ('car_heat_status', 'car_heat_neg',   'temp_air',      'Hot car',     '🚗',  'skip_car',
       '%s°F outside — don''t leave your dog in a parked car. Interior climbs to 100°F+ within 10 min, fatal heatstroke risk in 15.',
                                                                                                         '%s°F',   'max', 0),
    ('car_cold_status', 'car_cold_neg',   'temp_air',      'Cold car',    '🥶',  'skip_car',
       '%s°F outside — don''t leave your dog in a parked car. Interior cools to ambient quickly; hypothermia risk for short coats.',
                                                                                                         '%s°F',   'min', 0)
  ) AS m(event_type, signal_key, raw_col, label, icon, klass, text_tmpl, unit_fmt, agg, decimals);

  CREATE TEMP TABLE _hours ON COMMIT DROP AS
  SELECT g.fid AS dog_park_fid,
         h.local_date, h.local_hour, h.forecast_ts,
         h.asphalt_temp, h.uv_index, h.wind_speed,
         h.precip_chance, h.feels_like, h.temp_air
  FROM public.dog_parks_gold g
  JOIN public.dog_park_day_hourly_scores h ON h.dog_park_fid = g.fid
  WHERE g.is_active
    AND g.is_scoreable = true
    AND (p_state IS NULL OR g.state = upper(p_state))
    AND (p_fid   IS NULL OR g.fid   = p_fid)
    AND h.local_date BETWEEN CURRENT_DATE AND CURRENT_DATE + 1;

  CREATE TEMP TABLE _long ON COMMIT DROP AS
  SELECT h.dog_park_fid, h.local_date, h.local_hour, h.forecast_ts,
         m.event_type, m.agg, m.decimals,
         CASE m.raw_col
           WHEN 'asphalt_temp'   THEN h.asphalt_temp::numeric
           WHEN 'uv_index'       THEN h.uv_index::numeric
           WHEN 'wind_speed'     THEN h.wind_speed::numeric
           WHEN 'precip_chance'  THEN h.precip_chance::numeric
           WHEN 'feels_like'     THEN h.feels_like::numeric
           WHEN 'temp_air'       THEN h.temp_air::numeric
         END AS raw_val,
         public.v2_signal_status('dog_park', m.signal_key,
           CASE m.raw_col
             WHEN 'asphalt_temp'   THEN h.asphalt_temp::numeric
             WHEN 'uv_index'       THEN h.uv_index::numeric
             WHEN 'wind_speed'     THEN h.wind_speed::numeric
             WHEN 'precip_chance'  THEN h.precip_chance::numeric
             WHEN 'feels_like'     THEN h.feels_like::numeric
             WHEN 'temp_air'       THEN h.temp_air::numeric
           END) AS status_v2
  FROM _hours h
  CROSS JOIN _metrics m;

  CREATE TEMP TABLE _per_event ON COMMIT DROP AS
  SELECT dog_park_fid, local_date, event_type, agg, decimals,
         CASE max(CASE status_v2 WHEN 'no_go'    THEN 3
                                  WHEN 'caution'  THEN 2
                                  WHEN 'advisory' THEN 1
                                  ELSE 0 END)
           WHEN 3 THEN 'no_go'
           WHEN 2 THEN 'caution'
           WHEN 1 THEN 'advisory'
         END AS worst_status
  FROM _long
  WHERE coalesce(status_v2,'clear') IN ('advisory','caution','no_go')
  GROUP BY dog_park_fid, local_date, event_type, agg, decimals
  HAVING max(CASE status_v2 WHEN 'no_go' THEN 3 WHEN 'caution' THEN 2 WHEN 'advisory' THEN 1 ELSE 0 END) >= 1;

  CREATE TEMP TABLE _extremes ON COMMIT DROP AS
  SELECT l.dog_park_fid, l.local_date, l.event_type,
         CASE l.agg WHEN 'min' THEN min(l.raw_val) ELSE max(l.raw_val) END AS extreme,
         min(l.forecast_ts) AS first_ts,
         max(l.forecast_ts) AS last_ts,
         count(*)            AS hours_triggered
  FROM _long l
  WHERE l.forecast_ts >= v_now
    AND coalesce(l.status_v2,'clear') IN ('advisory','caution','no_go')
    AND l.raw_val IS NOT NULL
  GROUP BY l.dog_park_fid, l.local_date, l.event_type, l.agg;

  -- 6. UPSERT weather metrics.
  WITH upserts AS (
    SELECT pe.dog_park_fid, pe.local_date, pe.event_type,
           pe.worst_status, pe.decimals,
           CASE pe.worst_status WHEN 'no_go' THEN 'severe'
                                WHEN 'caution' THEN 'moderate'
                                ELSE 'minor' END AS severity,
           e.extreme, e.first_ts, e.last_ts, e.hours_triggered,
           m.label, m.icon, m.klass, m.text_tmpl, m.unit_fmt
    FROM _per_event pe
    JOIN _extremes  e  USING (dog_park_fid, local_date, event_type)
    JOIN _metrics   m  USING (event_type)
  ),
  ins AS (
    INSERT INTO public.dog_park_advisory (
      dog_park_fid, advisory_key, source, event_type, severity,
      valid_from, valid_to, dog_impact_class, dog_impact_text,
      translation_source, label, value, icon, raw_data, fetched_at
    )
    SELECT
      u.dog_park_fid,
      format('det:%s_%s:%s', u.event_type, u.worst_status, u.local_date),
      'deterministic_weather',
      u.event_type,
      u.severity,
      u.first_ts,
      u.last_ts,
      u.klass,
      format(u.text_tmpl, round(u.extreme, u.decimals)::text),
      'rule',
      u.label,
      format(u.unit_fmt, round(u.extreme, u.decimals)::text),
      u.icon,
      jsonb_build_object(
        'hours_triggered', u.hours_triggered,
        'extreme_value',   u.extreme::float,
        'worst_v2_status', u.worst_status,
        'scoring_version', 'v2'
      ),
      v_now
    FROM upserts u
    ON CONFLICT (dog_park_fid, advisory_key) DO UPDATE SET
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

  -- 7. Static `unfenced` advisory — set when has_fence = false.
  WITH unfenced AS (
    SELECT g.fid AS dog_park_fid
    FROM public.dog_parks_gold g
    LEFT JOIN public.dog_park_dog_policy p ON p.dog_park_fid = g.fid
    WHERE g.is_active
      AND g.is_scoreable = true
      AND (p_state IS NULL OR g.state = upper(p_state))
      AND (p_fid   IS NULL OR g.fid   = p_fid)
      AND coalesce(p.has_fence, g.has_fence) IS FALSE
  ),
  ins_unf AS (
    INSERT INTO public.dog_park_advisory (
      dog_park_fid, advisory_key, source, event_type, severity,
      valid_from, valid_to, dog_impact_class, dog_impact_text,
      translation_source, label, value, icon, raw_data, fetched_at
    )
    SELECT u.dog_park_fid, 'static:unfenced', 'static_attribute',
           'unfenced_status', 'moderate',
           v_now, v_now + interval '1 year',
           'review_required',
           'This park has no perimeter fence. Recall-trained dogs only; consider a long line.',
           'rule', 'Unfenced', NULL, '⚠️',
           jsonb_build_object('static', true, 'field', 'has_fence', 'value', false),
           v_now
    FROM unfenced u
    ON CONFLICT (dog_park_fid, advisory_key) DO UPDATE SET
      valid_from = EXCLUDED.valid_from,
      valid_to   = EXCLUDED.valid_to,
      fetched_at = EXCLUDED.fetched_at
    RETURNING 1
  )
  SELECT v_upserted + count(*) INTO v_upserted FROM ins_unf;

  -- 8. Retire stale rows in scope.
  WITH del AS (
    DELETE FROM public.dog_park_advisory da
    USING public.dog_parks_gold g
    WHERE da.dog_park_fid = g.fid
      AND da.source IN ('deterministic_weather','static_attribute')
      AND g.is_active
      AND g.is_scoreable = true
      AND (p_state IS NULL OR g.state = upper(p_state))
      AND (p_fid   IS NULL OR g.fid   = p_fid)
      AND da.fetched_at < v_now - interval '5 seconds'
    RETURNING 1
  )
  SELECT count(*) INTO v_retired FROM del;

  RETURN QUERY SELECT v_upserted, v_retired;
END;
$function$;

-- Usage:
--   SELECT * FROM public.refresh_beach_advisories();           -- all scored beaches
--   SELECT * FROM public.refresh_beach_advisories('CA');       -- one state
--   SELECT * FROM public.refresh_beach_advisories(NULL, 8347); -- one beach
--   SELECT * FROM public.refresh_dog_park_advisories();
