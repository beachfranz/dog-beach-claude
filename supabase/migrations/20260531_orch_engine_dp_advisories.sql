-- 20260531_orch_engine_dp_advisories.sql
--
-- PL/pgSQL port of scripts/compute_dog_park_advisories.py (280 lines).
-- Parallel of compute_weather_advisories.py but for dog parks:
--   - 8 metrics (no sand_temp / tide / crowd; same other metrics
--     including car_heat / car_cold)
--   - reads dog_park_day_hourly_scores
--   - v2_signal_status('dog_park', ...)
--   - writes to dog_park_advisory
--   - no bacteria
--   - adds static 'unfenced' advisory based on has_fence

BEGIN;

-- ─── DP spec table (parallel to orch_weather_advisory_spec) ──────────

CREATE TABLE IF NOT EXISTS public.orch_dp_advisory_spec (
  event_type        TEXT PRIMARY KEY,
  signal_key        TEXT NOT NULL,
  raw_col           TEXT NOT NULL,
  label             TEXT NOT NULL,
  icon              TEXT NOT NULL,
  dog_impact_class  TEXT NOT NULL,
  text_tmpl         TEXT NOT NULL,
  value_decimals    INT NOT NULL,
  value_suffix      TEXT NOT NULL,
  aggregate         TEXT NOT NULL CHECK (aggregate IN ('max','min'))
);

TRUNCATE public.orch_dp_advisory_spec;
INSERT INTO public.orch_dp_advisory_spec VALUES
  ('asphalt_status',   'asphalt_neg',     'asphalt_temp',  'Hot asphalt', E'\U0001F43E', 'paws_warning',
   'Parking-lot asphalt {observed}°F — booties for the walk in.',                         0, '°F', 'max'),
  ('uv_status',        'uv_neg',          'uv_index',      'High UV',     E'☀️',       'review_required',
   'UV peaks at {observed} — sunscreen for you, shade breaks for the pup.',               0, '',   'max'),
  ('wind_status',      'wind_harsh_neg',  'wind_speed',    'Strong wind', E'\U0001F4A8',     'blowing_sand',
   'Wind gusts {observed}mph — secure leashes when leaving; dusty conditions likely.',    0, 'mph','max'),
  ('rain_status',      'precip_chance',   'precip_chance', 'Rain',        E'\U0001F327️', 'review_required',
   'Rain likely ({observed}% chance) — bring a towel.',                                   0, '%',  'max'),
  ('temp_hot_status',  'feels_like_hot',  'feels_like',    'Heat',        E'\U0001F321️', 'paws_warning',
   'Hot (feels like {observed}°F) — heat stress risk; short visits + plenty of water.',   0, '°F', 'max'),
  ('temp_cold_status', 'feels_like_cold', 'feels_like',    'Cold',        E'❄️',       'cold_paws',
   'Cold (feels like {observed}°F) — short coats may need a jacket.',                     0, '°F', 'min'),
  ('car_heat_status',  'car_heat_neg',    'temp_air',      'Hot car',     E'\U0001F697',     'skip_car',
   '{observed}°F outside — don''t leave your dog in a parked car. Interior climbs to 100°F+ within 10 min, fatal heatstroke risk in 15.',
                                                                                          0, '°F', 'max'),
  ('car_cold_status',  'car_cold_neg',    'temp_air',      'Cold car',    E'\U0001F976',     'skip_car',
   '{observed}°F outside — don''t leave your dog in a parked car. Interior cools to ambient quickly; hypothermia risk for short coats.',
                                                                                          0, '°F', 'min');

COMMENT ON TABLE public.orch_dp_advisory_spec IS
  'DP-side spec table read by _orch_compute_dp_advisories_one. Parallel of orch_weather_advisory_spec; 8 metrics (no sand_temp/tide/crowd).';

-- ─── Per-DP core ──────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public._orch_compute_dp_advisories_one(p_fid bigint)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER AS $core$
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
  v_fence_val     BOOLEAN;
BEGIN
  SELECT array_agg(DISTINCT local_date ORDER BY local_date)
    INTO v_dates
    FROM public.dog_park_day_hourly_scores
   WHERE dog_park_fid = p_fid
     AND local_date BETWEEN (v_now AT TIME ZONE 'UTC')::date
                        AND ((v_now AT TIME ZONE 'UTC')::date + INTERVAL '1 day');

  IF v_dates IS NULL THEN
    -- Still check fence advisory below
    v_dates := ARRAY[]::DATE[];
  END IF;

  FOREACH v_date IN ARRAY v_dates LOOP
    FOR v_spec IN SELECT * FROM public.orch_dp_advisory_spec LOOP
      -- Worst rank-aware v2 status for the day
      SELECT public.v2_signal_status(
              'dog_park'::text, v_spec.signal_key,
              CASE v_spec.raw_col
                WHEN 'asphalt_temp'   THEN asphalt_temp::numeric
                WHEN 'uv_index'       THEN uv_index::numeric
                WHEN 'wind_speed'     THEN wind_speed::numeric
                WHEN 'precip_chance'  THEN precip_chance::numeric
                WHEN 'feels_like'     THEN feels_like::numeric
                WHEN 'temp_air'       THEN temp_air::numeric
              END
        )::text AS s INTO v_worst_status
        FROM public.dog_park_day_hourly_scores
       WHERE dog_park_fid = p_fid AND local_date = v_date
       ORDER BY CASE public.v2_signal_status(
              'dog_park'::text, v_spec.signal_key,
              CASE v_spec.raw_col
                WHEN 'asphalt_temp'   THEN asphalt_temp::numeric
                WHEN 'uv_index'       THEN uv_index::numeric
                WHEN 'wind_speed'     THEN wind_speed::numeric
                WHEN 'precip_chance'  THEN precip_chance::numeric
                WHEN 'feels_like'     THEN feels_like::numeric
                WHEN 'temp_air'       THEN temp_air::numeric
              END
        )::text
              WHEN 'no_go'    THEN 3
              WHEN 'caution'  THEN 2
              WHEN 'advisory' THEN 1
              ELSE 0 END DESC NULLS LAST
       LIMIT 1;

      IF v_worst_status IS NULL OR v_worst_status = 'clear' THEN CONTINUE; END IF;

      -- Triggered counts
      EXECUTE format($q$
        SELECT count(*) FILTER (
                 WHERE public.v2_signal_status('dog_park', %L, %I::numeric)::text
                       IN ('advisory','caution','no_go')),
               count(*) FILTER (
                 WHERE forecast_ts >= $1
                   AND public.v2_signal_status('dog_park', %L, %I::numeric)::text
                       IN ('advisory','caution','no_go'))
          FROM public.dog_park_day_hourly_scores
         WHERE dog_park_fid = $2 AND local_date = $3
      $q$, v_spec.signal_key, v_spec.raw_col, v_spec.signal_key, v_spec.raw_col)
        INTO v_n_triggered, v_n_future_trig
        USING v_now, p_fid, v_date;

      IF v_n_triggered = 0 THEN CONTINUE; END IF;

      v_advisory_key := format('det:%s_%s:%s', v_spec.event_type, v_worst_status, v_date);

      IF v_n_future_trig = 0 THEN
        DELETE FROM public.dog_park_advisory
         WHERE dog_park_fid = p_fid AND advisory_key = v_advisory_key;
        IF FOUND THEN v_n_retired := v_n_retired + 1; END IF;
        CONTINUE;
      END IF;

      v_severity := CASE v_worst_status
        WHEN 'no_go'    THEN 'severe'
        WHEN 'caution'  THEN 'moderate'
        WHEN 'advisory' THEN 'minor'
      END;

      EXECUTE format($q$
        SELECT %s(%I)::numeric, min(forecast_ts), max(forecast_ts)
          FROM public.dog_park_day_hourly_scores
         WHERE dog_park_fid = $1 AND local_date = $2
           AND forecast_ts >= $3
           AND public.v2_signal_status('dog_park', %L, %I::numeric)::text
               IN ('advisory','caution','no_go')
           AND %I IS NOT NULL
      $q$, v_spec.aggregate, v_spec.raw_col, v_spec.signal_key, v_spec.raw_col, v_spec.raw_col)
        INTO v_extreme_num, v_first_ts, v_last_ts
        USING p_fid, v_date, v_now;

      IF v_extreme_num IS NULL THEN CONTINUE; END IF;

      v_observed_str := CASE v_spec.value_decimals
        WHEN 0 THEN round(v_extreme_num)::TEXT
        ELSE   round(v_extreme_num, v_spec.value_decimals)::TEXT
      END;
      v_value_str := v_observed_str || v_spec.value_suffix;
      v_text := replace(v_spec.text_tmpl, '{observed}', v_observed_str);

      INSERT INTO public.dog_park_advisory (
        dog_park_fid, advisory_key, source, event_type, severity,
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
      ON CONFLICT (dog_park_fid, advisory_key) DO UPDATE SET
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

  -- v1-era sweep
  DELETE FROM public.dog_park_advisory
   WHERE dog_park_fid = p_fid
     AND source = 'deterministic_weather'
     AND (raw_data->>'scoring_version') IS DISTINCT FROM 'v2'
     AND fetched_at < now() - interval '1 minute';

  -- Static 'unfenced' advisory (or retirement)
  SELECT COALESCE(p.has_fence, g.has_fence)
    INTO v_fence_val
    FROM public.dog_parks_gold g
    LEFT JOIN public.dog_park_dog_policy p ON p.dog_park_fid = g.fid
   WHERE g.fid = p_fid;

  IF v_fence_val IS FALSE THEN
    INSERT INTO public.dog_park_advisory (
      dog_park_fid, advisory_key, source, event_type, severity,
      valid_from, valid_to, dog_impact_class, dog_impact_text,
      translation_source, label, value, icon, raw_data, fetched_at
    ) VALUES (
      p_fid, 'static:unfenced', 'static_attribute', 'unfenced_status', 'moderate',
      now(), now() + interval '1 year',
      'review_required',
      'This park has no perimeter fence. Recall-trained dogs only; consider a long line.',
      'rule', 'Unfenced', NULL, E'⚠️',
      jsonb_build_object('static', true, 'field', 'has_fence', 'value', false),
      now()
    )
    ON CONFLICT (dog_park_fid, advisory_key) DO UPDATE SET
      valid_from = EXCLUDED.valid_from,
      valid_to   = EXCLUDED.valid_to,
      fetched_at = now();
    v_n_upserted := v_n_upserted + 1;
  ELSE
    -- Fence present or unknown — retire prior unfenced advisory
    DELETE FROM public.dog_park_advisory
     WHERE dog_park_fid = p_fid AND advisory_key = 'static:unfenced';
    IF FOUND THEN v_n_retired := v_n_retired + 1; END IF;
  END IF;

  RETURN jsonb_build_object('fid', p_fid, 'upserted', v_n_upserted, 'retired', v_n_retired);
END;
$core$;

COMMENT ON FUNCTION public._orch_compute_dp_advisories_one IS
  'Per-DP worker: compute deterministic weather + static unfenced advisories for today+tomorrow.';

-- ─── Entry: _orch_w_compute_dp_advisories ─────────────────────────────

CREATE OR REPLACE FUNCTION public._orch_w_compute_dp_advisories(p jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $entry$
DECLARE
  v_states      TEXT[];
  v_only_fid    BIGINT;
  v_dp          RECORD;
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

  FOR v_dp IN
    SELECT fid
      FROM public.dog_parks_gold
     WHERE is_active
       AND (
         (v_only_fid IS NOT NULL AND fid = v_only_fid)
         OR (v_only_fid IS NULL
             AND is_scoreable = TRUE
             AND state = ANY(v_states))
       )
     ORDER BY fid
  LOOP
    v_one := public._orch_compute_dp_advisories_one(v_dp.fid);
    v_n_evaluated := v_n_evaluated + 1;
    v_n_upserted  := v_n_upserted  + COALESCE((v_one->>'upserted')::INT, 0);
    v_n_retired   := v_n_retired   + COALESCE((v_one->>'retired')::INT, 0);
  END LOOP;

  RAISE NOTICE 'dp_advisories: evaluated=% upserted=% retired=%',
    v_n_evaluated, v_n_upserted, v_n_retired;
END;
$entry$;

COMMENT ON FUNCTION public._orch_w_compute_dp_advisories IS
  'orch_jobs worker: compute deterministic weather + static unfenced advisories for dog parks across MVP+.';

-- ─── Update catalog: noop → sql_function ─────────────────────────────

UPDATE public.orch_jobs
   SET worker_kind   = 'sql_function',
       worker_target = 'public._orch_w_compute_dp_advisories',
       worker_payload = jsonb_build_object(
         'states', jsonb_build_array('CA','OR','WA','MD','UT')
       ),
       description = 'Compute deterministic weather + static unfenced advisories for dog parks. PL/pgSQL port of compute_dog_park_advisories.py.',
       updated_at = now()
 WHERE job_name = 'daily_dog_park_advisories';

COMMIT;
