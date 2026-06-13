-- 20260613a_wmo_advisory_emitter.sql
--
-- Surface WMO weather-code conditions as Cautions on beach.html.
-- The Weather and Tides card has always shown the WMO icon (☀️/🌫️/⛈️)
-- but nothing flowed into the Cautions card below it. This adds a
-- categorical emitter alongside the existing numeric-band emitter
-- (refresh_beach_advisories) so a thunderstorm forecast fires a
-- lightning_status caution, fog fires a fog_status caution, etc.
--
-- WMO bucket → event_type mapping (every code rendered by beach.html
-- WMO_ICON map gets explicit treatment; positives are no-op):
--
--   0,1,2,3       Clear/Mostly sunny/Partly cloudy/Overcast  → no caution
--   45, 48        Fog                                         → fog_status (minor; moderate if ≥3h)
--   51-65, 80-81  Drizzle / light-mod rain / showers          → no caution (rain_status covers via precip_chance)
--   65, 82        Heavy rain / violent showers                → heavy_rain_status (moderate)
--   71, 73, 75    Snow                                        → snow_status (minor; moderate if ≥3h)
--   95, 96, 99    Thunderstorm                                → lightning_status (severe — any 1h triggers)
--   56,57,66,67   Freezing drizzle/rain                       → freezing_precip_status (severe — any 1h triggers)
--
-- Severity tiering: lightning + freezing are always severe (one
-- forecast hour is sufficient). Fog/snow/heavy-rain tier by
-- hours_triggered (1-2 → minor; ≥3 → moderate), so an AM fog burn-off
-- doesn't fire the same severity as 6-hour all-day fog.
--
-- Off-by-one fix from 20260612a is baked in from the start:
-- valid_to = max(forecast_ts) + interval '1 hour'.

BEGIN;

-- ─────────────────────────────────────────────────────────────────────
-- 1. refresh_beach_wmo_advisories
-- ─────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.refresh_beach_wmo_advisories(
  p_state text   DEFAULT NULL,
  p_fid   bigint DEFAULT NULL
) RETURNS TABLE(upserted bigint, retired bigint)
LANGUAGE plpgsql AS $function$
DECLARE
  v_now timestamptz := now();
  v_upserted bigint := 0;
  v_retired  bigint := 0;
BEGIN
  DROP TABLE IF EXISTS _wmo_rules, _hours, _triggered;

  CREATE TEMP TABLE _wmo_rules ON COMMIT DROP AS
  SELECT * FROM (VALUES
    ('lightning_status',       ARRAY[95,96,99]::int[],          'severe',   true,  'Lightning',         '⚡',  'skip_outdoor',
       'Thunderstorm forecast (%s hr) — skip the beach. Lightning is deadly; thunder terrifies most dogs.'),
    ('freezing_precip_status', ARRAY[56,57,66,67]::int[],       'severe',   true,  'Freezing precip',   '🥶',  'skip_outdoor',
       'Freezing precipitation forecast (%s hr) — slippery surfaces; skip the beach.'),
    ('fog_status',             ARRAY[45,48]::int[],             'minor',    false, 'Fog',               '🌫️', 'review_required',
       'Foggy conditions (%s hr) — keep dogs close, visibility limited.'),
    ('snow_status',            ARRAY[71,73,75,77,85,86]::int[], 'minor',    false, 'Snow',              '🌨️', 'cold_paws',
       'Snow forecast (%s hr) — cold paws; booties for short-coated dogs.'),
    ('heavy_rain_status',      ARRAY[65,82]::int[],             'moderate', false, 'Heavy rain',        '🌧',  'review_required',
       'Heavy rain forecast (%s hr) — bring towels, expect a muddy pup.')
  ) AS r(event_type, wmo_codes, default_severity, always_severe, label, icon, klass, text_tmpl);

  CREATE TEMP TABLE _hours ON COMMIT DROP AS
  SELECT bg.fid AS beach_fid, bg.location_id,
         h.local_date, h.forecast_ts, h.weather_code
  FROM public.beaches_gold bg
  JOIN public.beach_day_hourly_scores h ON h.location_id = bg.location_id
  WHERE bg.is_active
    AND bg.scoring_tier IN ('daily','hourly')
    AND bg.location_id IS NOT NULL
    AND (p_state IS NULL OR bg.state = upper(p_state))
    AND (p_fid   IS NULL OR bg.fid   = p_fid)
    AND h.local_date BETWEEN CURRENT_DATE AND CURRENT_DATE + 1
    AND h.weather_code IS NOT NULL;

  CREATE TEMP TABLE _triggered ON COMMIT DROP AS
  SELECT h.beach_fid, h.local_date,
         r.event_type, r.default_severity, r.always_severe,
         r.label, r.icon, r.klass, r.text_tmpl,
         min(h.forecast_ts) AS first_ts,
         max(h.forecast_ts) + interval '1 hour' AS last_ts,
         count(*) AS hours_triggered
  FROM _hours h
  JOIN _wmo_rules r ON h.weather_code = ANY(r.wmo_codes)
  WHERE h.forecast_ts >= v_now
  GROUP BY h.beach_fid, h.local_date,
           r.event_type, r.default_severity, r.always_severe,
           r.label, r.icon, r.klass, r.text_tmpl;

  WITH upserts AS (
    SELECT t.*,
      CASE
        WHEN t.always_severe                                  THEN 'severe'
        WHEN t.hours_triggered >= 3 AND t.default_severity = 'minor' THEN 'moderate'
        ELSE t.default_severity
      END AS severity
    FROM _triggered t
  ),
  ins AS (
    INSERT INTO public.beach_advisory (
      beach_fid, advisory_key, source, event_type, severity,
      valid_from, valid_to, dog_impact_class, dog_impact_text,
      translation_source, label, value, icon, raw_data, fetched_at
    )
    SELECT u.beach_fid,
      format('wmo:%s_%s:%s', u.event_type, u.severity, u.local_date),
      'deterministic_wmo',
      u.event_type, u.severity,
      u.first_ts, u.last_ts, u.klass,
      format(u.text_tmpl, u.hours_triggered::text),
      'rule', u.label,
      u.hours_triggered::text || ' hr',
      u.icon,
      jsonb_build_object(
        'hours_triggered', u.hours_triggered,
        'scoring_version', 'wmo'
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

  -- Watermark sweep
  WITH del AS (
    DELETE FROM public.beach_advisory ba
    USING public.beaches_gold bg
    WHERE ba.beach_fid = bg.fid
      AND ba.source = 'deterministic_wmo'
      AND (p_state IS NULL OR bg.state = upper(p_state))
      AND (p_fid   IS NULL OR bg.fid   = p_fid)
      AND ba.fetched_at < v_now - interval '5 seconds'
    RETURNING 1
  )
  SELECT count(*) INTO v_retired FROM del;

  RETURN QUERY SELECT v_upserted, v_retired;
END;
$function$;

-- ─────────────────────────────────────────────────────────────────────
-- 2. refresh_dog_park_wmo_advisories  (paired-functions sibling)
-- ─────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.refresh_dog_park_wmo_advisories(
  p_state text   DEFAULT NULL,
  p_fid   bigint DEFAULT NULL
) RETURNS TABLE(upserted bigint, retired bigint)
LANGUAGE plpgsql AS $function$
DECLARE
  v_now timestamptz := now();
  v_upserted bigint := 0;
  v_retired  bigint := 0;
BEGIN
  DROP TABLE IF EXISTS _wmo_rules, _hours, _triggered;

  -- DP gets the same rules MINUS lightning_status' beach-specific phrasing
  -- nuance, and minus snow (snow at a dog park has its own dynamics
  -- — leashes get tangled; we still want it surfaced).
  CREATE TEMP TABLE _wmo_rules ON COMMIT DROP AS
  SELECT * FROM (VALUES
    ('lightning_status',       ARRAY[95,96,99]::int[],          'severe',   true,  'Lightning',         '⚡',  'skip_outdoor',
       'Thunderstorm forecast (%s hr) — close the gate, head home. Lightning is deadly; thunder terrifies most dogs.'),
    ('freezing_precip_status', ARRAY[56,57,66,67]::int[],       'severe',   true,  'Freezing precip',   '🥶',  'skip_outdoor',
       'Freezing precipitation forecast (%s hr) — slippery surfaces; skip the park.'),
    ('fog_status',             ARRAY[45,48]::int[],             'minor',    false, 'Fog',               '🌫️', 'review_required',
       'Foggy conditions (%s hr) — keep dogs close, visibility limited.'),
    ('snow_status',            ARRAY[71,73,75,77,85,86]::int[], 'minor',    false, 'Snow',              '🌨️', 'cold_paws',
       'Snow forecast (%s hr) — cold paws; booties for short-coated dogs.'),
    ('heavy_rain_status',      ARRAY[65,82]::int[],             'moderate', false, 'Heavy rain',        '🌧',  'review_required',
       'Heavy rain forecast (%s hr) — bring towels, expect a muddy pup.')
  ) AS r(event_type, wmo_codes, default_severity, always_severe, label, icon, klass, text_tmpl);

  CREATE TEMP TABLE _hours ON COMMIT DROP AS
  SELECT g.fid AS dog_park_fid,
         h.local_date, h.forecast_ts, h.weather_code
  FROM public.dog_parks_gold g
  JOIN public.dog_park_day_hourly_scores h ON h.dog_park_fid = g.fid
  WHERE g.is_active AND g.is_scoreable = true
    AND (p_state IS NULL OR g.state = upper(p_state))
    AND (p_fid   IS NULL OR g.fid   = p_fid)
    AND h.local_date BETWEEN CURRENT_DATE AND CURRENT_DATE + 1
    AND h.weather_code IS NOT NULL;

  CREATE TEMP TABLE _triggered ON COMMIT DROP AS
  SELECT h.dog_park_fid, h.local_date,
         r.event_type, r.default_severity, r.always_severe,
         r.label, r.icon, r.klass, r.text_tmpl,
         min(h.forecast_ts) AS first_ts,
         max(h.forecast_ts) + interval '1 hour' AS last_ts,
         count(*) AS hours_triggered
  FROM _hours h
  JOIN _wmo_rules r ON h.weather_code = ANY(r.wmo_codes)
  WHERE h.forecast_ts >= v_now
  GROUP BY h.dog_park_fid, h.local_date,
           r.event_type, r.default_severity, r.always_severe,
           r.label, r.icon, r.klass, r.text_tmpl;

  WITH upserts AS (
    SELECT t.*,
      CASE
        WHEN t.always_severe                                  THEN 'severe'
        WHEN t.hours_triggered >= 3 AND t.default_severity = 'minor' THEN 'moderate'
        ELSE t.default_severity
      END AS severity
    FROM _triggered t
  ),
  ins AS (
    INSERT INTO public.dog_park_advisory (
      dog_park_fid, advisory_key, source, event_type, severity,
      valid_from, valid_to, dog_impact_class, dog_impact_text,
      translation_source, label, value, icon, raw_data, fetched_at
    )
    SELECT u.dog_park_fid,
      format('wmo:%s_%s:%s', u.event_type, u.severity, u.local_date),
      'deterministic_wmo',
      u.event_type, u.severity,
      u.first_ts, u.last_ts, u.klass,
      format(u.text_tmpl, u.hours_triggered::text),
      'rule', u.label,
      u.hours_triggered::text || ' hr',
      u.icon,
      jsonb_build_object(
        'hours_triggered', u.hours_triggered,
        'scoring_version', 'wmo'
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

  WITH del AS (
    DELETE FROM public.dog_park_advisory da
    USING public.dog_parks_gold g
    WHERE da.dog_park_fid = g.fid
      AND da.source = 'deterministic_wmo'
      AND (p_state IS NULL OR g.state = upper(p_state))
      AND (p_fid   IS NULL OR g.fid   = p_fid)
      AND da.fetched_at < v_now - interval '5 seconds'
    RETURNING 1
  )
  SELECT count(*) INTO v_retired FROM del;

  RETURN QUERY SELECT v_upserted, v_retired;
END;
$function$;

COMMIT;

NOTIFY pgrst, 'reload schema';
