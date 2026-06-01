-- 20260531_orch_engine_weather_advisories.sql
--
-- PL/pgSQL port of scripts/compute_weather_advisories.py (339 lines Python).
-- The Python wraps SQL (v2_signal_status calls + INSERT/UPDATE/DELETE on
-- beach_advisory) with Python orchestration. The port keeps:
--
--   - 11 hourly metric specs (sand_temp, asphalt, uv, wind, tide, rain,
--     temp_hot, temp_cold, car_heat, car_cold, crowd) via a constant
--     spec table _orch_weather_advisory_spec
--   - Bacteria from beach_day_recommendations (daily, separate source)
--   - Per-metric per-day worst severity & future-only observed extreme
--   - Past-only spike retirement (DELETE prior advisory_key)
--   - v1-era sweep (rows without raw_data->>'scoring_version'='v2')
--
-- Cadence: orch_jobs.daily_weather_advisories (daily_at 12:00).
-- worker_payload: {"states": [...], "only_fid": N}; defaults to MVP+.
--
-- Dagster source-of-truth (scripts/compute_weather_advisories.py)
-- remains in repo unchanged for local ad-hoc work.

BEGIN;

-- ─── Spec table: 11 hourly metrics ────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.orch_weather_advisory_spec (
  event_type        TEXT PRIMARY KEY,
  signal_key        TEXT NOT NULL,         -- input to v2_signal_status()
  raw_col           TEXT NOT NULL,         -- column in beach_day_hourly_scores
  label             TEXT NOT NULL,
  icon              TEXT NOT NULL,
  dog_impact_class  TEXT NOT NULL,
  text_tmpl         TEXT NOT NULL,         -- '{observed}' placeholder
  value_decimals    INT NOT NULL,          -- 0 / 1
  value_suffix      TEXT NOT NULL,         -- e.g. '°F', 'mph', 'ft', '%'
  aggregate         TEXT NOT NULL CHECK (aggregate IN ('max','min'))
);

TRUNCATE public.orch_weather_advisory_spec;
INSERT INTO public.orch_weather_advisory_spec VALUES
  ('sand_status',      'sand_temp_neg',   'sand_temp',     'Hot sand',    E'\U0001F3D6️', 'paws_warning',
   'Sand will hit {observed}°F — paws will burn. Go dawn or dusk.',                       0, '°F', 'max'),
  ('asphalt_status',   'asphalt_neg',     'asphalt_temp',  'Hot asphalt', E'\U0001F6B6',       'paws_warning',
   'Parking-lot asphalt {observed}°F — booties for the walk in.',                          0, '°F', 'max'),
  ('uv_status',        'uv_neg',          'uv_index',      'High UV',     E'☀️',     'review_required',
   'UV peaks at {observed} — sunscreen for you, shade breaks for the pup.',                0, '',   'max'),
  ('wind_status',      'wind_harsh_neg',  'wind_speed',    'Strong wind', E'\U0001F4A8',       'blowing_sand',
   'Wind gusts {observed}mph — blowing sand will sting.',                                  0, 'mph','max'),
  ('tide_status',      'tide_neg',        'tide_height',   'High tide',   E'\U0001F30A',       'skip_swim',
   'High tide ≥{observed}ft — limited beach to walk on.',                                  1, 'ft', 'max'),
  ('rain_status',      'precip_chance',   'precip_chance', 'Rain',        E'\U0001F327️', 'review_required',
   'Rain likely ({observed}% chance) — bring a towel.',                                    0, '%',  'max'),
  ('temp_hot_status',  'feels_like_hot',  'feels_like',    'Heat',        E'\U0001F975',       'paws_warning',
   'Hot day (feels like {observed}°F) — dawn or dusk, plenty of water.',                   0, '°F', 'max'),
  ('temp_cold_status', 'feels_like_cold', 'feels_like',    'Cold',        E'\U0001F976',       'cold_paws',
   'Cold (feels like {observed}°F) — short coats may need a jacket.',                      0, '°F', 'min'),
  ('car_heat_status',  'car_heat_neg',    'temp_air',      'Hot car',     E'\U0001F697',       'skip_car',
   '{observed}°F outside — don''t leave your dog in a parked car. Interior climbs to 100°F+ within 10 min, fatal heatstroke risk in 15.',
                                                                                            0, '°F', 'max'),
  ('car_cold_status',  'car_cold_neg',    'temp_air',      'Cold car',    E'\U0001F976',       'skip_car',
   '{observed}°F outside — don''t leave your dog in a parked car. Interior cools to ambient quickly; hypothermia risk for short coats.',
                                                                                            0, '°F', 'min'),
  ('crowd_status',     'crowd_neg',       'busyness_score','Crowded',     E'\U0001F465',       'review_required',
   'Beach is busy (score {observed}) — reactive dogs may struggle.',                       0, '',   'max');

COMMENT ON TABLE public.orch_weather_advisory_spec IS
  'Static spec table read by _orch_compute_weather_advisories_one. '
  'Edit rows in place to add/tune metrics — no code change needed.';

-- ─── Per-beach core ───────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public._orch_compute_weather_advisories_one(
  p_fid bigint, p_location_id text
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER AS $core$
DECLARE
  v_now           TIMESTAMPTZ := now();
  v_spec          RECORD;
  v_date          DATE;
  v_dates         DATE[];
  v_worst_status  TEXT;
  v_first_ts      TIMESTAMPTZ;
  v_last_ts       TIMESTAMPTZ;
  v_extreme_num   NUMERIC;
  v_observed_str  TEXT;
  v_value_str     TEXT;
  v_text          TEXT;
  v_severity      TEXT;
  v_advisory_key  TEXT;
  v_n_triggered   INT;
  v_n_future_trig INT;
  v_n_upserted    INT := 0;
  v_n_retired     INT := 0;
  v_bact          RECORD;
BEGIN
  -- Discover today + tomorrow's dates the beach has scoring for
  SELECT array_agg(DISTINCT local_date ORDER BY local_date)
    INTO v_dates
    FROM public.beach_day_hourly_scores
   WHERE location_id = p_location_id
     AND local_date BETWEEN (v_now AT TIME ZONE 'UTC')::date
                        AND ((v_now AT TIME ZONE 'UTC')::date + INTERVAL '1 day');

  IF v_dates IS NULL THEN
    RETURN jsonb_build_object('fid', p_fid, 'upserted', 0, 'retired', 0, 'no_data', true);
  END IF;

  FOREACH v_date IN ARRAY v_dates LOOP
    FOR v_spec IN SELECT * FROM public.orch_weather_advisory_spec LOOP
      -- Compute worst v2 status across the whole day + future-only counts
      SELECT
        max(public.v2_signal_status(
              'beach'::text, v_spec.signal_key,
              CASE v_spec.raw_col
                WHEN 'sand_temp'      THEN sand_temp::numeric
                WHEN 'asphalt_temp'   THEN asphalt_temp::numeric
                WHEN 'uv_index'       THEN uv_index::numeric
                WHEN 'wind_speed'     THEN wind_speed::numeric
                WHEN 'tide_height'    THEN tide_height::numeric
                WHEN 'precip_chance'  THEN precip_chance::numeric
                WHEN 'feels_like'     THEN feels_like::numeric
                WHEN 'busyness_score' THEN busyness_score::numeric
                WHEN 'temp_air'       THEN temp_air::numeric
              END
        )::text) AS worst,
        count(*) FILTER (WHERE public.v2_signal_status(
              'beach'::text, v_spec.signal_key,
              CASE v_spec.raw_col
                WHEN 'sand_temp'      THEN sand_temp::numeric
                WHEN 'asphalt_temp'   THEN asphalt_temp::numeric
                WHEN 'uv_index'       THEN uv_index::numeric
                WHEN 'wind_speed'     THEN wind_speed::numeric
                WHEN 'tide_height'    THEN tide_height::numeric
                WHEN 'precip_chance'  THEN precip_chance::numeric
                WHEN 'feels_like'     THEN feels_like::numeric
                WHEN 'busyness_score' THEN busyness_score::numeric
                WHEN 'temp_air'       THEN temp_air::numeric
              END
        )::text IN ('advisory','caution','no_go')) AS n_trig,
        count(*) FILTER (
          WHERE forecast_ts >= v_now
            AND public.v2_signal_status(
              'beach'::text, v_spec.signal_key,
              CASE v_spec.raw_col
                WHEN 'sand_temp'      THEN sand_temp::numeric
                WHEN 'asphalt_temp'   THEN asphalt_temp::numeric
                WHEN 'uv_index'       THEN uv_index::numeric
                WHEN 'wind_speed'     THEN wind_speed::numeric
                WHEN 'tide_height'    THEN tide_height::numeric
                WHEN 'precip_chance'  THEN precip_chance::numeric
                WHEN 'feels_like'     THEN feels_like::numeric
                WHEN 'busyness_score' THEN busyness_score::numeric
                WHEN 'temp_air'       THEN temp_air::numeric
              END
        )::text IN ('advisory','caution','no_go')) AS n_future_trig
        INTO v_worst_status, v_n_triggered, v_n_future_trig
        FROM public.beach_day_hourly_scores
       WHERE location_id = p_location_id
         AND local_date = v_date;

      -- Map worst status (ranked) for max() across triggered statuses
      -- Note: text sort gives advisory < caution < clear < no_go; we need
      -- a rank-based comparison. Recompute properly via ORDER BY in case
      -- of text-sort surprise:
      SELECT public.v2_signal_status(
              'beach'::text, v_spec.signal_key,
              CASE v_spec.raw_col
                WHEN 'sand_temp'      THEN sand_temp::numeric
                WHEN 'asphalt_temp'   THEN asphalt_temp::numeric
                WHEN 'uv_index'       THEN uv_index::numeric
                WHEN 'wind_speed'     THEN wind_speed::numeric
                WHEN 'tide_height'    THEN tide_height::numeric
                WHEN 'precip_chance'  THEN precip_chance::numeric
                WHEN 'feels_like'     THEN feels_like::numeric
                WHEN 'busyness_score' THEN busyness_score::numeric
                WHEN 'temp_air'       THEN temp_air::numeric
              END
        )::text AS s INTO v_worst_status
        FROM public.beach_day_hourly_scores
       WHERE location_id = p_location_id AND local_date = v_date
       ORDER BY CASE public.v2_signal_status(
              'beach'::text, v_spec.signal_key,
              CASE v_spec.raw_col
                WHEN 'sand_temp'      THEN sand_temp::numeric
                WHEN 'asphalt_temp'   THEN asphalt_temp::numeric
                WHEN 'uv_index'       THEN uv_index::numeric
                WHEN 'wind_speed'     THEN wind_speed::numeric
                WHEN 'tide_height'    THEN tide_height::numeric
                WHEN 'precip_chance'  THEN precip_chance::numeric
                WHEN 'feels_like'     THEN feels_like::numeric
                WHEN 'busyness_score' THEN busyness_score::numeric
                WHEN 'temp_air'       THEN temp_air::numeric
              END
        )::text
              WHEN 'no_go'    THEN 3
              WHEN 'caution'  THEN 2
              WHEN 'advisory' THEN 1
              ELSE 0 END DESC NULLS LAST
       LIMIT 1;

      IF v_worst_status IS NULL OR v_worst_status = 'clear' OR v_n_triggered = 0 THEN
        CONTINUE;
      END IF;

      v_advisory_key := format('det:%s_%s:%s', v_spec.event_type, v_worst_status, v_date);

      IF v_n_future_trig = 0 THEN
        -- Spike already past: retire prior advisory if any.
        DELETE FROM public.beach_advisory
         WHERE beach_fid = p_fid AND advisory_key = v_advisory_key;
        IF FOUND THEN v_n_retired := v_n_retired + 1; END IF;
        CONTINUE;
      END IF;

      v_severity := CASE v_worst_status
        WHEN 'no_go'    THEN 'severe'
        WHEN 'caution'  THEN 'moderate'
        WHEN 'advisory' THEN 'minor'
      END;

      -- Future-only extreme observed value + bracketing timestamps
      EXECUTE format($q$
        SELECT %s(%I)::numeric, min(forecast_ts), max(forecast_ts)
          FROM public.beach_day_hourly_scores
         WHERE location_id = $1 AND local_date = $2
           AND forecast_ts >= $3
           AND public.v2_signal_status('beach', %L, %I::numeric)::text
               IN ('advisory','caution','no_go')
           AND %I IS NOT NULL
      $q$, v_spec.aggregate, v_spec.raw_col, v_spec.signal_key, v_spec.raw_col, v_spec.raw_col)
        INTO v_extreme_num, v_first_ts, v_last_ts
        USING p_location_id, v_date, v_now;

      IF v_extreme_num IS NULL THEN CONTINUE; END IF;

      v_observed_str := CASE v_spec.value_decimals
        WHEN 0 THEN round(v_extreme_num)::TEXT
        ELSE   round(v_extreme_num, v_spec.value_decimals)::TEXT
      END;
      v_value_str := v_observed_str || v_spec.value_suffix;
      v_text := replace(v_spec.text_tmpl, '{observed}', v_observed_str);

      INSERT INTO public.beach_advisory (
        beach_fid, advisory_key, source, event_type, severity,
        valid_from, valid_to, dog_impact_class, dog_impact_text,
        translation_source, label, value, icon, raw_data, fetched_at
      ) VALUES (
        p_fid, v_advisory_key, 'deterministic_weather', v_spec.event_type, v_severity,
        v_first_ts, v_last_ts, v_spec.dog_impact_class, v_text,
        'rule', v_spec.label, v_value_str, v_spec.icon,
        jsonb_build_object(
          'extreme_value', v_extreme_num::float,
          'worst_v2_status', v_worst_status,
          'signal_key', v_spec.signal_key,
          'scoring_version', 'v2'
        ),
        now()
      )
      ON CONFLICT (beach_fid, advisory_key) DO UPDATE SET
        severity        = EXCLUDED.severity,
        valid_from      = EXCLUDED.valid_from,
        valid_to        = EXCLUDED.valid_to,
        dog_impact_text = EXCLUDED.dog_impact_text,
        value           = EXCLUDED.value,
        raw_data        = EXCLUDED.raw_data,
        fetched_at      = now();
      v_n_upserted := v_n_upserted + 1;
    END LOOP;  -- spec
  END LOOP;    -- date

  -- v1-era sweep: any old deterministic_weather row without scoring_version='v2'
  DELETE FROM public.beach_advisory
   WHERE beach_fid = p_fid
     AND source = 'deterministic_weather'
     AND (raw_data->>'scoring_version') IS DISTINCT FROM 'v2'
     AND fetched_at < now() - interval '1 minute';
  GET DIAGNOSTICS v_n_retired = ROW_COUNT;  -- overwrite for total
  v_n_retired := v_n_retired + COALESCE(v_n_retired, 0);

  -- Bacteria from beach_day_recommendations (separate source)
  FOR v_bact IN
    SELECT local_date, bacteria_risk
      FROM public.beach_day_recommendations
     WHERE location_id = p_location_id
       AND local_date BETWEEN (v_now AT TIME ZONE 'UTC')::date
                          AND ((v_now AT TIME ZONE 'UTC')::date + INTERVAL '1 day')
       AND bacteria_risk IN ('moderate','high')
  LOOP
    v_advisory_key := format('det:bacteria_%s:%s', v_bact.bacteria_risk, v_bact.local_date);
    INSERT INTO public.beach_advisory (
      beach_fid, advisory_key, source, event_type, severity,
      valid_from, valid_to, dog_impact_class, dog_impact_text,
      translation_source, label, value, icon, raw_data, fetched_at
    ) VALUES (
      p_fid, v_advisory_key, 'deterministic_bacteria', 'bacteria_risk',
      CASE v_bact.bacteria_risk WHEN 'high' THEN 'severe' ELSE 'moderate' END,
      v_bact.local_date::timestamptz,
      (v_bact.local_date + INTERVAL '1 day')::timestamptz,
      'skip_swim',
      format('Water-quality %s risk (recent rain) — keep your dog out of the surf.', v_bact.bacteria_risk),
      'rule', 'Water-quality risk', v_bact.bacteria_risk::text, E'⚠️',
      jsonb_build_object('bacteria_risk', v_bact.bacteria_risk),
      now()
    )
    ON CONFLICT (beach_fid, advisory_key) DO UPDATE SET
      severity   = EXCLUDED.severity,
      valid_from = EXCLUDED.valid_from,
      valid_to   = EXCLUDED.valid_to,
      fetched_at = now();
    v_n_upserted := v_n_upserted + 1;
  END LOOP;

  RETURN jsonb_build_object('fid', p_fid, 'upserted', v_n_upserted, 'retired', v_n_retired);
END;
$core$;

COMMENT ON FUNCTION public._orch_compute_weather_advisories_one IS
  'Per-beach worker: compute deterministic-weather + bacteria advisories for today+tomorrow.';

-- ─── Entry: _orch_w_compute_weather_advisories ────────────────────────

CREATE OR REPLACE FUNCTION public._orch_w_compute_weather_advisories(p jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $entry$
DECLARE
  v_states      TEXT[];
  v_only_fid    BIGINT;
  v_beach       RECORD;
  v_n_evaluated INT := 0;
  v_n_upserted  INT := 0;
  v_n_retired   INT := 0;
  v_one         JSONB;
BEGIN
  v_states := COALESCE(
    NULLIF(ARRAY(SELECT jsonb_array_elements_text(p->'states')), ARRAY[]::TEXT[]),
    ARRAY['CA','OR','WA','MD','UT']::TEXT[]
  );
  v_only_fid := (p->>'only_fid')::BIGINT;

  FOR v_beach IN
    SELECT fid, location_id
      FROM public.beaches_gold
     WHERE is_active
       AND location_id IS NOT NULL
       AND (
         (v_only_fid IS NOT NULL AND fid = v_only_fid)
         OR (v_only_fid IS NULL
             AND scoring_tier IN ('daily','hourly')
             AND state = ANY(v_states))
       )
     ORDER BY fid
  LOOP
    v_one := public._orch_compute_weather_advisories_one(v_beach.fid, v_beach.location_id);
    v_n_evaluated := v_n_evaluated + 1;
    v_n_upserted  := v_n_upserted  + COALESCE((v_one->>'upserted')::INT, 0);
    v_n_retired   := v_n_retired   + COALESCE((v_one->>'retired')::INT, 0);
  END LOOP;

  RAISE NOTICE 'weather_advisories: evaluated=% upserted=% retired=%',
    v_n_evaluated, v_n_upserted, v_n_retired;
END;
$entry$;

COMMENT ON FUNCTION public._orch_w_compute_weather_advisories IS
  'orch_jobs worker: compute deterministic weather + bacteria advisories across MVP+ beaches.';

-- ─── Update catalog: noop → sql_function ─────────────────────────────

UPDATE public.orch_jobs
   SET worker_kind   = 'sql_function',
       worker_target = 'public._orch_w_compute_weather_advisories',
       worker_payload = jsonb_build_object(
         'states', jsonb_build_array('CA','OR','WA','MD','UT')
       ),
       description = 'Compute deterministic weather + bacteria advisories from beach_day_hourly_scores and beach_day_recommendations. PL/pgSQL port of Dagster weather_advisories_refresh.',
       updated_at = now()
 WHERE job_name = 'daily_weather_advisories';

COMMIT;
