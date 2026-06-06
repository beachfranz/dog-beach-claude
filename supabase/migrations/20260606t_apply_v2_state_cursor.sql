-- 20260606t_apply_v2_state_cursor.sql
--
-- Follow-up to 20260606s: per-state chunking inside a single wrapper call
-- wasn't enough — even with each per-state pass small, cumulative time
-- across 49 active states (424 CA parks, 229 TX, etc.) blew past
-- statement_timeout=2min on the OUTER orch_tick call.
--
-- Pivot: process ONE state per orch_tick fire, cycling through states
-- via a cursor table. Each call is fast (5-15s for CA, much less for
-- smaller states). With cadence still every 5 min, full cycle = 5min ×
-- 49 states = ~4 hours. That's slower than desired but reliable.
--
-- Cycle time can be cranked tighter by changing the cadence to every
-- minute (`cadence_param='1'`) without code changes — 49 min cycle.
-- Left at 5 min for now; cron volume + DB load impact is the trade-off.

-- Cursor table — one row per entity (beach + dog_park), tracks the last
-- state processed so the next call picks the next one alphabetically.
CREATE TABLE IF NOT EXISTS public._apply_v2_state_cursor (
  entity      text PRIMARY KEY,
  last_state  text,
  updated_at  timestamptz NOT NULL DEFAULT now()
);

INSERT INTO public._apply_v2_state_cursor (entity, last_state) VALUES
  ('beach',    NULL),
  ('dog_park', NULL)
ON CONFLICT (entity) DO NOTHING;

-- ─── Beach wrapper: pick next state, advance cursor ──────────────────────
CREATE OR REPLACE FUNCTION public._orch_w_apply_v2_best_window_beach(p jsonb)
RETURNS void LANGUAGE plpgsql AS $function$
DECLARE
  v_state_override   text;
  v_days_ahead       int;
  v_state_to_process text;
  v_last_state       text;
  v_b bigint; v_p bigint; v_h bigint; v_r bigint; v_w bigint;
BEGIN
  v_state_override := p->>'state';
  v_days_ahead     := coalesce(nullif(p->>'days_ahead', '')::int, 6);

  IF v_state_override IS NOT NULL THEN
    v_state_to_process := upper(v_state_override);
  ELSE
    -- Pick the alphabetically-next state after the cursor; wrap at end.
    SELECT last_state INTO v_last_state
      FROM public._apply_v2_state_cursor
     WHERE entity = 'beach';

    SELECT MIN(state) INTO v_state_to_process
      FROM public.beaches_gold
     WHERE is_active
       AND scoring_tier IN ('daily','hourly')
       AND state IS NOT NULL
       AND (v_last_state IS NULL OR state > v_last_state);

    -- Wrap: if no state > cursor, restart from the beginning.
    IF v_state_to_process IS NULL THEN
      SELECT MIN(state) INTO v_state_to_process
        FROM public.beaches_gold
       WHERE is_active
         AND scoring_tier IN ('daily','hourly')
         AND state IS NOT NULL;
    END IF;
  END IF;

  IF v_state_to_process IS NULL THEN
    RAISE NOTICE 'bulk_apply_beach: no states with scoreable beaches; skipping';
    RETURN;
  END IF;

  SELECT beaches_processed, fid_date_pairs, hour_scores_written, recs_written, windows_picked
    INTO v_b, v_p, v_h, v_r, v_w
    FROM public.apply_v2_best_window_to_beach_recommendations_bulk(
      v_state_to_process, NULL, NULL, v_days_ahead);

  -- Advance cursor only when not overridden (else override caller can repeat).
  IF v_state_override IS NULL THEN
    UPDATE public._apply_v2_state_cursor
       SET last_state = v_state_to_process, updated_at = now()
     WHERE entity = 'beach';
  END IF;

  RAISE NOTICE 'bulk_apply_beach: state=% days=% beaches=% pairs=% hours=% recs=% windows=%',
    v_state_to_process, v_days_ahead, v_b, v_p, v_h, v_r, v_w;
END;
$function$;

-- ─── Dog-park wrapper: pick next state, advance cursor ───────────────────
CREATE OR REPLACE FUNCTION public._orch_w_apply_v2_best_window_dog_park(p jsonb)
RETURNS void LANGUAGE plpgsql AS $function$
DECLARE
  v_state_override   text;
  v_days_ahead       int;
  v_state_to_process text;
  v_last_state       text;
  v_pp bigint; v_p bigint; v_h bigint; v_r bigint; v_w bigint;
BEGIN
  v_state_override := p->>'state';
  v_days_ahead     := coalesce(nullif(p->>'days_ahead', '')::int, 6);

  IF v_state_override IS NOT NULL THEN
    v_state_to_process := upper(v_state_override);
  ELSE
    SELECT last_state INTO v_last_state
      FROM public._apply_v2_state_cursor
     WHERE entity = 'dog_park';

    SELECT MIN(state) INTO v_state_to_process
      FROM public.dog_parks_gold
     WHERE is_active AND is_scoreable = true
       AND state IS NOT NULL
       AND (v_last_state IS NULL OR state > v_last_state);

    IF v_state_to_process IS NULL THEN
      SELECT MIN(state) INTO v_state_to_process
        FROM public.dog_parks_gold
       WHERE is_active AND is_scoreable = true
         AND state IS NOT NULL;
    END IF;
  END IF;

  IF v_state_to_process IS NULL THEN
    RAISE NOTICE 'bulk_apply_dp: no states with scoreable parks; skipping';
    RETURN;
  END IF;

  SELECT parks_processed, fid_date_pairs, hour_scores_written, recs_written, windows_picked
    INTO v_pp, v_p, v_h, v_r, v_w
    FROM public.apply_v2_best_window_to_recommendations_bulk(
      v_state_to_process, NULL, NULL, v_days_ahead);

  IF v_state_override IS NULL THEN
    UPDATE public._apply_v2_state_cursor
       SET last_state = v_state_to_process, updated_at = now()
     WHERE entity = 'dog_park';
  END IF;

  RAISE NOTICE 'bulk_apply_dp: state=% days=% parks=% pairs=% hours=% recs=% windows=%',
    v_state_to_process, v_days_ahead, v_pp, v_p, v_h, v_r, v_w;
END;
$function$;

-- Tighten cadence to every minute so the 49-state cycle completes in
-- ~49 min instead of ~4 hours. Each call is ~5-15s; total DB load ≈
-- 1 cursor-advance call per minute, well below background-task budget.
UPDATE public.orch_jobs
   SET cadence_param = '1', updated_at = now()
 WHERE job_name IN ('apply_v2_best_window_beach', 'apply_v2_best_window_dog_park');
