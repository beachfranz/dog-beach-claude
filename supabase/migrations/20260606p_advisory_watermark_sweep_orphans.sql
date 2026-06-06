-- 20260606p_advisory_watermark_sweep_orphans.sql
--
-- Cleanup task 2 from end-of-day cleanup list. The watermark DELETE in
-- refresh_*_advisories filters scoping by bg.is_active AND
-- bg.scoring_tier IN ('daily','hourly') — meaning advisory rows on
-- beaches/parks that demoted to scoring_tier='none' (or got is_active
-- flipped off) sit as orphans forever, because the sweep won't touch them.
--
-- Surfaced during the marine cutover (`a49ffe0`) where 4 zombie CA marine
-- advisories on demoted-from-scoreable beaches had to be hand-cleaned via
-- a one-off DELETE.
--
-- Fix: scope the watermark sweep ONLY by `source` + `fetched_at` watermark
-- + optional state/fid filters. Drop the is_active + scoring_tier filters
-- from the DELETE clauses. Orphans get cleaned regardless of current
-- scoring state of the underlying entity.
--
-- The is_active + scoring_tier filters were a defensive belt+suspenders
-- that prevented the sweep from over-deleting if the fn was called with
-- a wide scope — but the watermark itself (`fetched_at < v_now - 5s`)
-- is sufficient: any row from THIS run will have fetched_at = v_now;
-- anything older is genuinely stale and should go.

-- ─────────────────────────────────────────────────────────────────────────
-- 1. refresh_beach_advisories
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
  DROP TABLE IF EXISTS _metrics, _hours, _long, _per_event, _extremes;

  -- (Metrics dict + scope + unpivot + per_event + extremes blocks unchanged.)

  CREATE TEMP TABLE _metrics ON COMMIT DROP AS
  SELECT * FROM (VALUES
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
      u.event_type, u.severity,
      u.first_ts, u.last_ts, u.klass,
      format(u.text_tmpl, round(u.extreme, u.decimals)::text),
      'rule', u.label,
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
      severity=EXCLUDED.severity, valid_from=EXCLUDED.valid_from,
      valid_to=EXCLUDED.valid_to, dog_impact_text=EXCLUDED.dog_impact_text,
      value=EXCLUDED.value, raw_data=EXCLUDED.raw_data, fetched_at=EXCLUDED.fetched_at
    RETURNING 1
  )
  SELECT count(*) INTO v_upserted FROM ins;

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
    SELECT b.beach_fid,
      format('det:bacteria_%s:%s', b.bacteria_risk, b.local_date),
      'deterministic_bacteria', 'bacteria_risk',
      CASE b.bacteria_risk WHEN 'high' THEN 'severe' ELSE 'moderate' END,
      b.local_date::timestamptz, (b.local_date + interval '1 day')::timestamptz,
      'skip_swim',
      format('Water-quality %s risk (recent rain) — keep your dog out of the surf.', b.bacteria_risk),
      'rule', 'Water-quality risk', b.bacteria_risk, '⚠️',
      jsonb_build_object('bacteria_risk', b.bacteria_risk), v_now
    FROM bact b
    ON CONFLICT (beach_fid, advisory_key) DO UPDATE SET
      severity=EXCLUDED.severity, valid_from=EXCLUDED.valid_from,
      valid_to=EXCLUDED.valid_to, fetched_at=EXCLUDED.fetched_at
    RETURNING 1
  )
  SELECT v_upserted + count(*) INTO v_upserted FROM ins_bact;

  -- ─── Watermark sweep — DROPPED is_active + scoring_tier filter ───
  -- Was: AND bg.is_active AND bg.scoring_tier IN ('daily','hourly')
  -- Now: source + fetched_at watermark + optional state/fid scope.
  -- Orphan advisories on demoted-to-none or inactive beaches get cleaned.
  WITH del AS (
    DELETE FROM public.beach_advisory ba
    USING public.beaches_gold bg
    WHERE ba.beach_fid = bg.fid
      AND ba.source IN ('deterministic_weather','deterministic_bacteria')
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
-- 2. refresh_dog_park_advisories
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
  SELECT g.fid AS dog_park_fid, h.local_date, h.local_hour, h.forecast_ts,
         h.asphalt_temp, h.uv_index, h.wind_speed,
         h.precip_chance, h.feels_like, h.temp_air
  FROM public.dog_parks_gold g
  JOIN public.dog_park_day_hourly_scores h ON h.dog_park_fid = g.fid
  WHERE g.is_active AND g.is_scoreable = true
    AND (p_state IS NULL OR g.state = upper(p_state))
    AND (p_fid   IS NULL OR g.fid   = p_fid)
    AND h.local_date BETWEEN CURRENT_DATE AND CURRENT_DATE + 1;

  CREATE TEMP TABLE _long ON COMMIT DROP AS
  SELECT h.dog_park_fid, h.local_date, h.local_hour, h.forecast_ts,
         m.event_type, m.agg, m.decimals,
         CASE m.raw_col
           WHEN 'asphalt_temp'  THEN h.asphalt_temp::numeric
           WHEN 'uv_index'      THEN h.uv_index::numeric
           WHEN 'wind_speed'    THEN h.wind_speed::numeric
           WHEN 'precip_chance' THEN h.precip_chance::numeric
           WHEN 'feels_like'    THEN h.feels_like::numeric
           WHEN 'temp_air'      THEN h.temp_air::numeric
         END AS raw_val,
         public.v2_signal_status('dog_park', m.signal_key,
           CASE m.raw_col
             WHEN 'asphalt_temp'  THEN h.asphalt_temp::numeric
             WHEN 'uv_index'      THEN h.uv_index::numeric
             WHEN 'wind_speed'    THEN h.wind_speed::numeric
             WHEN 'precip_chance' THEN h.precip_chance::numeric
             WHEN 'feels_like'    THEN h.feels_like::numeric
             WHEN 'temp_air'      THEN h.temp_air::numeric
           END) AS status_v2
  FROM _hours h CROSS JOIN _metrics m;

  CREATE TEMP TABLE _per_event ON COMMIT DROP AS
  SELECT dog_park_fid, local_date, event_type, agg, decimals,
         CASE max(CASE status_v2 WHEN 'no_go' THEN 3 WHEN 'caution' THEN 2 WHEN 'advisory' THEN 1 ELSE 0 END)
           WHEN 3 THEN 'no_go' WHEN 2 THEN 'caution' WHEN 1 THEN 'advisory'
         END AS worst_status
  FROM _long WHERE coalesce(status_v2,'clear') IN ('advisory','caution','no_go')
  GROUP BY dog_park_fid, local_date, event_type, agg, decimals
  HAVING max(CASE status_v2 WHEN 'no_go' THEN 3 WHEN 'caution' THEN 2 WHEN 'advisory' THEN 1 ELSE 0 END) >= 1;

  CREATE TEMP TABLE _extremes ON COMMIT DROP AS
  SELECT l.dog_park_fid, l.local_date, l.event_type,
         CASE l.agg WHEN 'min' THEN min(l.raw_val) ELSE max(l.raw_val) END AS extreme,
         min(l.forecast_ts) AS first_ts, max(l.forecast_ts) AS last_ts,
         count(*) AS hours_triggered
  FROM _long l WHERE l.forecast_ts >= v_now
    AND coalesce(l.status_v2,'clear') IN ('advisory','caution','no_go')
    AND l.raw_val IS NOT NULL
  GROUP BY l.dog_park_fid, l.local_date, l.event_type, l.agg;

  WITH upserts AS (
    SELECT pe.dog_park_fid, pe.local_date, pe.event_type, pe.worst_status, pe.decimals,
           CASE pe.worst_status WHEN 'no_go' THEN 'severe' WHEN 'caution' THEN 'moderate' ELSE 'minor' END AS severity,
           e.extreme, e.first_ts, e.last_ts, e.hours_triggered,
           m.label, m.icon, m.klass, m.text_tmpl, m.unit_fmt
    FROM _per_event pe JOIN _extremes e USING (dog_park_fid, local_date, event_type) JOIN _metrics m USING (event_type)
  ),
  ins AS (
    INSERT INTO public.dog_park_advisory (
      dog_park_fid, advisory_key, source, event_type, severity,
      valid_from, valid_to, dog_impact_class, dog_impact_text,
      translation_source, label, value, icon, raw_data, fetched_at
    )
    SELECT u.dog_park_fid,
      format('det:%s_%s:%s', u.event_type, u.worst_status, u.local_date),
      'deterministic_weather', u.event_type, u.severity,
      u.first_ts, u.last_ts, u.klass,
      format(u.text_tmpl, round(u.extreme, u.decimals)::text),
      'rule', u.label,
      format(u.unit_fmt, round(u.extreme, u.decimals)::text),
      u.icon,
      jsonb_build_object('hours_triggered', u.hours_triggered, 'extreme_value', u.extreme::float,
                         'worst_v2_status', u.worst_status, 'scoring_version', 'v2'),
      v_now
    FROM upserts u
    ON CONFLICT (dog_park_fid, advisory_key) DO UPDATE SET
      severity=EXCLUDED.severity, valid_from=EXCLUDED.valid_from, valid_to=EXCLUDED.valid_to,
      dog_impact_text=EXCLUDED.dog_impact_text, value=EXCLUDED.value,
      raw_data=EXCLUDED.raw_data, fetched_at=EXCLUDED.fetched_at
    RETURNING 1
  )
  SELECT count(*) INTO v_upserted FROM ins;

  -- Static unfenced (unchanged scope — only meaningful for active scoreable parks).
  WITH unfenced AS (
    SELECT g.fid AS dog_park_fid
    FROM public.dog_parks_gold g
    LEFT JOIN public.dog_park_dog_policy p ON p.dog_park_fid = g.fid
    WHERE g.is_active AND g.is_scoreable = true
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
           v_now, v_now + interval '1 year', 'review_required',
           'This park has no perimeter fence. Recall-trained dogs only; consider a long line.',
           'rule', 'Unfenced', NULL, '⚠️',
           jsonb_build_object('static', true, 'field', 'has_fence', 'value', false),
           v_now
    FROM unfenced u
    ON CONFLICT (dog_park_fid, advisory_key) DO UPDATE SET
      valid_from=EXCLUDED.valid_from, valid_to=EXCLUDED.valid_to, fetched_at=EXCLUDED.fetched_at
    RETURNING 1
  )
  SELECT v_upserted + count(*) INTO v_upserted FROM ins_unf;

  -- ─── Watermark sweep — DROPPED is_active + is_scoreable filter ────
  WITH del AS (
    DELETE FROM public.dog_park_advisory da
    USING public.dog_parks_gold g
    WHERE da.dog_park_fid = g.fid
      AND da.source IN ('deterministic_weather','static_attribute')
      AND (p_state IS NULL OR g.state = upper(p_state))
      AND (p_fid   IS NULL OR g.fid   = p_fid)
      AND da.fetched_at < v_now - interval '5 seconds'
    RETURNING 1
  )
  SELECT count(*) INTO v_retired FROM del;

  RETURN QUERY SELECT v_upserted, v_retired;
END;
$function$;

-- ─────────────────────────────────────────────────────────────────────────
-- 3. refresh_marine_advisories
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
      severity=EXCLUDED.severity, valid_from=EXCLUDED.valid_from, valid_to=EXCLUDED.valid_to,
      dog_impact_text=EXCLUDED.dog_impact_text, value=EXCLUDED.value,
      raw_data=EXCLUDED.raw_data, fetched_at=EXCLUDED.fetched_at
    RETURNING 1
  )
  SELECT count(*) INTO v_upserted FROM ins;

  -- ─── Watermark sweep — DROPPED is_active + scoring_tier filter ────
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
