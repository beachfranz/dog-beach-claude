-- 20260622b_apply_workers_fid_chunk_cursor.sql
--
-- INCIDENT FIX (recovery-critical). The apply_v2_best_window_{beach,dog_park}
-- orch jobs are sql_function workers dispatched SYNCHRONOUSLY inside orch_tick.
-- They round-robined WHOLE STATES via _apply_v2_state_cursor.last_state and
-- called apply_v2_best_window_to_*_bulk(state, NULL, NULL, days) full-state.
-- For a big state (CA = 740 beaches x 7 days) that single UPDATE over
-- beach_day_hourly_scores (2.93M rows) exceeds the effective 120s
-- statement_timeout. The cursor only advances on success, so it wedged on CA
-- forever -> every orch_tick timed out -> all 37 jobs starved -> only ~25% of
-- beaches scored.
--
-- The original per-state design (20260606t) assumed "CA = 5-15s". CA outgrew
-- the 120s wall. Fix: shrink the work unit from "one state" to "one 50-fid
-- chunk". A 50-fid x 7-day call is ~5,600 hourly-row updates -- seconds, not
-- minutes. The cursor becomes a per-FID cursor (last_fid).
--
-- Paired per [[paired-functions-port-fixes-both-sides]]: both the beach worker
-- and the dog-park sibling get the identical change.
--
-- The bulk fns are UNCHANGED: their _scope CTEs already honor p_fids when
-- p_state IS NULL (20260618j:48-49 beach, 20260618g:43-44 dog-park).

BEGIN;

-- ── Cursor: add the authoritative per-fid cursor; reset for a clean cycle.
ALTER TABLE public._apply_v2_state_cursor
  ADD COLUMN IF NOT EXISTS last_fid bigint;

-- Clean-start the cycle. Also structurally bypasses the CA wedge: selection
-- now keys on last_fid, not last_state. last_state is kept only as a
-- human-readable breadcrumb going forward.
UPDATE public._apply_v2_state_cursor SET last_fid = NULL, last_state = NULL;

-- ───────────────────────── beach worker ─────────────────────────
CREATE OR REPLACE FUNCTION public._orch_w_apply_v2_best_window_beach(p jsonb)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
  v_state_override text;
  v_days_ahead     int;
  v_chunk          int;
  v_last_fid       bigint;
  v_fids           bigint[];
  v_max_fid        bigint;
  v_b bigint; v_p bigint; v_h bigint; v_r bigint; v_w bigint;
BEGIN
  v_state_override := p->>'state';
  v_days_ahead     := coalesce(nullif(p->>'days_ahead', '')::int, 6);
  v_chunk          := coalesce(nullif(p->>'chunk', '')::int, 50);

  -- Manual override: process one whole state on demand. Does NOT touch the
  -- cursor (preserves the historical override semantics from 20260606t).
  IF v_state_override IS NOT NULL THEN
    SELECT beaches_processed, fid_date_pairs, hour_scores_written, recs_written, windows_picked
      INTO v_b, v_p, v_h, v_r, v_w
      FROM public.apply_v2_best_window_to_beach_recommendations_bulk(
        upper(v_state_override), NULL, NULL, v_days_ahead);
    RAISE NOTICE 'bulk_apply_beach[override]: state=% days=% beaches=% pairs=% hours=% recs=% windows=%',
      upper(v_state_override), v_days_ahead, v_b, v_p, v_h, v_r, v_w;
    RETURN NULL;
  END IF;

  -- Per-fid-chunk cursor: the next v_chunk fids after last_fid. The WHERE
  -- predicates MUST mirror the bulk fn's _scope (20260618j:43-47) exactly --
  -- otherwise the cursor advances past beaches that produce zero work,
  -- silently skipping them every cycle.
  SELECT last_fid INTO v_last_fid
    FROM public._apply_v2_state_cursor WHERE entity = 'beach';

  SELECT array_agg(fid ORDER BY fid), max(fid)
    INTO v_fids, v_max_fid
  FROM (
    SELECT bg.fid
      FROM public.beaches_gold bg
     WHERE bg.is_active
       AND bg.scoring_tier IN ('daily','hourly')
       AND bg.location_id IS NOT NULL
       AND bg.open_time   IS NOT NULL
       AND bg.close_time  IS NOT NULL
       AND (v_last_fid IS NULL OR bg.fid > v_last_fid)
     ORDER BY bg.fid
     LIMIT v_chunk
  ) q;

  -- Wrap: nothing beyond the cursor -> restart from the lowest fid.
  IF v_fids IS NULL THEN
    SELECT array_agg(fid ORDER BY fid), max(fid)
      INTO v_fids, v_max_fid
    FROM (
      SELECT bg.fid
        FROM public.beaches_gold bg
       WHERE bg.is_active
         AND bg.scoring_tier IN ('daily','hourly')
         AND bg.location_id IS NOT NULL
         AND bg.open_time   IS NOT NULL
         AND bg.close_time  IS NOT NULL
       ORDER BY bg.fid
       LIMIT v_chunk
    ) q;
  END IF;

  IF v_fids IS NULL THEN
    RAISE NOTICE 'bulk_apply_beach: no scoreable beaches; skipping';
    RETURN NULL;
  END IF;

  SELECT beaches_processed, fid_date_pairs, hour_scores_written, recs_written, windows_picked
    INTO v_b, v_p, v_h, v_r, v_w
    FROM public.apply_v2_best_window_to_beach_recommendations_bulk(
      NULL, v_fids, NULL, v_days_ahead);

  UPDATE public._apply_v2_state_cursor
     SET last_fid   = v_max_fid,
         last_state = (SELECT state FROM public.beaches_gold WHERE fid = v_max_fid),
         updated_at = now()
   WHERE entity = 'beach';

  RAISE NOTICE 'bulk_apply_beach: chunk=% last_fid->% beaches=% pairs=% hours=% recs=% windows=%',
    v_chunk, v_max_fid, v_b, v_p, v_h, v_r, v_w;
  RETURN NULL;
END $function$;

-- ───────────────────────── dog-park worker (paired) ─────────────────────────
CREATE OR REPLACE FUNCTION public._orch_w_apply_v2_best_window_dog_park(p jsonb)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
  v_state_override text;
  v_days_ahead     int;
  v_chunk          int;
  v_last_fid       bigint;
  v_fids           bigint[];
  v_max_fid        bigint;
  v_pp bigint; v_p bigint; v_h bigint; v_r bigint; v_w bigint;
BEGIN
  v_state_override := p->>'state';
  v_days_ahead     := coalesce(nullif(p->>'days_ahead', '')::int, 6);
  v_chunk          := coalesce(nullif(p->>'chunk', '')::int, 50);

  IF v_state_override IS NOT NULL THEN
    SELECT parks_processed, fid_date_pairs, hour_scores_written, recs_written, windows_picked
      INTO v_pp, v_p, v_h, v_r, v_w
      FROM public.apply_v2_best_window_to_recommendations_bulk(
        upper(v_state_override), NULL, NULL, v_days_ahead);
    RAISE NOTICE 'bulk_apply_dp[override]: state=% days=% parks=% pairs=% hours=% recs=% windows=%',
      upper(v_state_override), v_days_ahead, v_pp, v_p, v_h, v_r, v_w;
    RETURN NULL;
  END IF;

  -- Predicates mirror the dog-park bulk _scope (20260618g:42): is_active AND
  -- is_scoreable. No extra NOT-NULL predicates on this side.
  SELECT last_fid INTO v_last_fid
    FROM public._apply_v2_state_cursor WHERE entity = 'dog_park';

  SELECT array_agg(fid ORDER BY fid), max(fid)
    INTO v_fids, v_max_fid
  FROM (
    SELECT g.fid
      FROM public.dog_parks_gold g
     WHERE g.is_active AND g.is_scoreable = true
       AND (v_last_fid IS NULL OR g.fid > v_last_fid)
     ORDER BY g.fid
     LIMIT v_chunk
  ) q;

  IF v_fids IS NULL THEN
    SELECT array_agg(fid ORDER BY fid), max(fid)
      INTO v_fids, v_max_fid
    FROM (
      SELECT g.fid
        FROM public.dog_parks_gold g
       WHERE g.is_active AND g.is_scoreable = true
       ORDER BY g.fid
       LIMIT v_chunk
    ) q;
  END IF;

  IF v_fids IS NULL THEN
    RAISE NOTICE 'bulk_apply_dp: no scoreable parks; skipping';
    RETURN NULL;
  END IF;

  SELECT parks_processed, fid_date_pairs, hour_scores_written, recs_written, windows_picked
    INTO v_pp, v_p, v_h, v_r, v_w
    FROM public.apply_v2_best_window_to_recommendations_bulk(
      NULL, v_fids, NULL, v_days_ahead);

  UPDATE public._apply_v2_state_cursor
     SET last_fid   = v_max_fid,
         last_state = (SELECT state FROM public.dog_parks_gold WHERE fid = v_max_fid),
         updated_at = now()
   WHERE entity = 'dog_park';

  RAISE NOTICE 'bulk_apply_dp: chunk=% last_fid->% parks=% pairs=% hours=% recs=% windows=%',
    v_chunk, v_max_fid, v_pp, v_p, v_h, v_r, v_w;
  RETURN NULL;
END $function$;

COMMIT;
