-- 20260622c_orch_tick_harden_and_prune.sql
--
-- Hardening follow-up to 20260622b. Three things:
--
-- 1. Make orch_tick's timeout budget actually effective. The function-level
--    `SET statement_timeout TO '10min'` (20260606r) does NOT take effect under
--    pg_cron -- statement_timeout is measured per TOP-LEVEL statement
--    (`SELECT orch_tick()`), and the function GUC override doesn't re-arm the
--    timer, so the DB default of 120s wins. Set it in-body via set_config (the
--    proven pattern from _orch_w_run_pipeline_maintenance, 20260601).
--
-- 2. Make the per-job dispatch EXCEPTION handler resilient. Because the
--    timeout is per top-level statement, once it fires inside a worker the
--    handler's own recovery UPDATE is ALSO past the deadline and re-raises,
--    escaping the tick (this is what killed the dispatcher on 2026-06-22).
--    Re-arm the budget at the top of the handler so the failure is recorded
--    and the loop continues to the next job.
--
--    (No per-worker statement_timeout cap: under per-top-level-statement
--    semantics a small cap would account cumulative tick time and would itself
--    wedge legitimately-slow workers. The 10min tick budget + resilient
--    handler + chunked apply workers (20260622b) are sufficient.)
--
-- 3. Remove the ~800ms/tick seq-scan. The closing
--    `UPDATE orch_jobs ... FROM (SELECT ... orch_runs WHERE intended_fire_at=v_now
--    AND status LIKE 'skipped_%')` block has no covering index and scanned the
--    whole (855K-row, never-pruned) orch_runs table every tick. Set
--    last_skip_reason inline in the loop instead (v_skip_reason is already in
--    scope) -- one PK-indexed update per skipped job, zero orch_runs scan.
--
-- Plus a prune worker + job so orch_runs stops growing unbounded (retain 14d).
-- The one-time backlog delete of the existing 855K rows is done manually
-- (batched) post-deploy, NOT here.

BEGIN;

CREATE OR REPLACE FUNCTION public.orch_tick()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET statement_timeout TO '10min'
AS $function$
DECLARE
  v_now            TIMESTAMPTZ := date_trunc('minute', now());
  v_lock_key       BIGINT := 626262626262;
  v_job            public.orch_jobs;
  v_request_id     BIGINT;
  v_status         TEXT;
  v_skip_reason    TEXT;
  v_summary        JSONB := '[]'::jsonb;
  v_n_evaluated    INT := 0;
  v_n_fired        INT := 0;
  v_n_skipped      INT := 0;
  v_n_shadowed     INT := 0;
  v_n_failed       INT := 0;
  v_n_reconciled   INT := 0;
BEGIN
  IF NOT pg_try_advisory_xact_lock(v_lock_key) THEN
    RETURN jsonb_build_object('result','skipped','reason','tick already running');
  END IF;

  -- EDIT 1: effective tick budget. The function-level SET above is a no-op
  -- under pg_cron (see header); set it in-body. statement_timeout is re-armed
  -- relative to the top-level statement start.
  PERFORM set_config('statement_timeout', '600000', true);  -- 10 min, txn-local

  -- Reconcile http responses from prior ticks. This surfaces 4xx/5xx
  -- responses that orch_dispatch fire-and-forget couldn't see.
  SELECT count(*) INTO v_n_reconciled
    FROM public._orch_reconcile_responses();

  -- Reaper: only sql_function workers can be "stuck". Edge functions are
  -- fire-and-forget; they have no callback to clear is_running, so we
  -- don't set it for them in the first place (see lower in this function).
  UPDATE public.orch_jobs
     SET is_running = FALSE,
         running_since = NULL,
         running_request_id = NULL,
         last_failed_at = now(),
         last_error = COALESCE(last_error, '') || ' | orch: reaped after 30min runtime',
         updated_at = now()
   WHERE is_running = TRUE
     AND worker_kind = 'sql_function'
     AND running_since IS NOT NULL
     AND running_since < now() - interval '30 minutes';

  FOR v_job IN
    SELECT j.*
      FROM public.orch_jobs j
     WHERE j.enabled = TRUE
     ORDER BY COALESCE(array_length(j.depends_on, 1), 0), j.job_name
  LOOP
    v_n_evaluated := v_n_evaluated + 1;
    v_status := NULL; v_skip_reason := NULL; v_request_id := NULL;

    IF v_job.is_running THEN
      v_status := 'skipped_already_running';
      v_skip_reason := format('running since %s', v_job.running_since);
    ELSIF NOT public._orch_should_fire(v_job, v_now) THEN
      v_status := 'skipped_cron_window';
    ELSIF v_job.skip_if_succeeded_within IS NOT NULL
       AND v_job.last_succeeded_at IS NOT NULL
       AND v_job.last_succeeded_at >= now() - v_job.skip_if_succeeded_within THEN
      v_status := 'skipped_cost';
      v_skip_reason := format('succeeded within %s (last %s)',
        v_job.skip_if_succeeded_within, v_job.last_succeeded_at);
    ELSIF NOT public._orch_deps_ok(v_job) THEN
      v_status := 'skipped_dependency';
      v_skip_reason := format('one or more depends_on not fresh within %s', v_job.depends_max_age);
    ELSIF NOT public._orch_work_pending(v_job) THEN
      v_status := 'skipped_cost';
      v_skip_reason := format('cost_check_fn %s returned false', v_job.cost_check_fn);
    ELSE
      IF v_job.shadow_mode THEN
        v_status := 'shadow';
        v_skip_reason := 'shadow_mode=true; not firing';
        v_n_shadowed := v_n_shadowed + 1;
      ELSE
        BEGIN
          v_request_id := public._orch_dispatch(v_job.job_name);
          v_status := 'fired';
          v_n_fired := v_n_fired + 1;
          -- For edge_function: fire-and-forget at dispatch time. The
          -- response gets reconciled later in _orch_reconcile_responses;
          -- if it was 4xx/5xx, that pass will demote last_succeeded_at
          -- and bump last_failed_at + last_error. We still set
          -- last_succeeded_at here optimistically so a healthy fire is
          -- recorded immediately for skip_if_succeeded_within math.
          -- For sql_function: dispatch is synchronous (function call to
          -- here), so completion means success.
          UPDATE public.orch_jobs
             SET is_running         = FALSE,
                 running_since      = NULL,
                 running_request_id = v_request_id,
                 last_attempted_at  = now(),
                 last_succeeded_at  = now(),
                 last_error         = NULL,
                 last_skip_reason   = NULL,
                 updated_at         = now()
           WHERE job_name = v_job.job_name;
        EXCEPTION WHEN OTHERS THEN
          -- EDIT 2: re-arm the budget before the recovery UPDATE. The timeout
          -- is per top-level statement, so once a worker times out we are past
          -- the deadline and this UPDATE would itself be cancelled and re-raise,
          -- escaping the tick (the 2026-06-22 dispatcher kill). Re-arming lets
          -- the failure be recorded and the loop continue.
          PERFORM set_config('statement_timeout', '600000', true);
          v_status := 'failed';
          v_n_failed := v_n_failed + 1;
          UPDATE public.orch_jobs
             SET last_attempted_at = now(),
                 last_failed_at    = now(),
                 last_error        = SQLERRM,
                 updated_at        = now()
           WHERE job_name = v_job.job_name;
        END;
      END IF;
    END IF;

    IF v_status LIKE 'skipped_%' THEN
      v_n_skipped := v_n_skipped + 1;
      -- EDIT 3: set last_skip_reason inline (v_skip_reason already computed).
      -- Replaces the post-loop UPDATE...FROM orch_runs scan that seq-scanned
      -- the whole orch_runs table (~800ms) every tick.
      UPDATE public.orch_jobs
         SET last_skip_reason = v_skip_reason, updated_at = now()
       WHERE job_name = v_job.job_name;
    END IF;

    INSERT INTO public.orch_runs (
      job_name, intended_fire_at, started_at, ended_at,
      status, skip_reason, worker_request_id
    ) VALUES (
      v_job.job_name, v_now, now(),
      CASE WHEN v_status LIKE 'skipped_%' OR v_status = 'shadow' THEN now() ELSE NULL END,
      v_status, v_skip_reason, v_request_id
    ) ON CONFLICT DO NOTHING;

    IF v_status IN ('fired','shadow','failed') THEN
      v_summary := v_summary || jsonb_build_object(
        'job', v_job.job_name,
        'status', v_status,
        'request_id', v_request_id,
        'skip_reason', v_skip_reason
      );
    END IF;
  END LOOP;

  -- (EDIT 3: the post-loop `UPDATE orch_jobs ... FROM (SELECT ... orch_runs
  --  WHERE intended_fire_at=v_now AND status LIKE 'skipped_%')` block was
  --  removed -- last_skip_reason is now set inline above.)

  RETURN jsonb_build_object(
    'tick_at',     v_now,
    'evaluated',   v_n_evaluated,
    'fired',       v_n_fired,
    'shadowed',    v_n_shadowed,
    'skipped',     v_n_skipped,
    'failed',      v_n_failed,
    'reconciled',  v_n_reconciled,
    'actions',     v_summary
  );
END;
$function$;

-- ───────────────────────── prune worker + job ─────────────────────────
CREATE OR REPLACE FUNCTION public._orch_w_prune_runs(p jsonb)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
  v_n bigint;
BEGIN
  WITH d AS (
    DELETE FROM public.orch_runs
     WHERE started_at < now() - coalesce(nullif(p->>'retain','')::interval, interval '14 days')
    RETURNING 1
  )
  SELECT count(*) INTO v_n FROM d;
  RAISE NOTICE 'prune_orch_runs: deleted % rows', v_n;
  RETURN NULL;
END $function$;

INSERT INTO public.orch_jobs (
  job_name, description,
  cadence_kind, cadence_param, cron_expr,
  depends_on, depends_max_age, cost_check_fn, skip_if_succeeded_within,
  worker_kind, worker_target, worker_payload, scope_states, enabled, shadow_mode
) VALUES (
  'prune_orch_runs',
  'Daily prune of orch_runs older than 14 days (the table was unbounded; see 20260622c).',
  'daily_at', '03:17', NULL,
  ARRAY[]::text[], NULL, NULL, NULL,
  'sql_function', 'public._orch_w_prune_runs', '{}'::jsonb,
  ARRAY[]::text[], TRUE, FALSE
)
ON CONFLICT (job_name) DO UPDATE SET
  description   = EXCLUDED.description,
  cadence_kind  = EXCLUDED.cadence_kind,
  cadence_param = EXCLUDED.cadence_param,
  worker_kind   = EXCLUDED.worker_kind,
  worker_target = EXCLUDED.worker_target,
  enabled       = EXCLUDED.enabled,
  shadow_mode   = EXCLUDED.shadow_mode,
  updated_at    = now();

COMMIT;
