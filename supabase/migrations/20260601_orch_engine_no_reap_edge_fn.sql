-- 20260601_orch_engine_no_reap_edge_fn.sql
--
-- Fix the false-positive reaping that causes hourly edge_function jobs
-- to show last_failed_at = (last_succeeded_at + 30 min) between cron
-- windows.
--
-- Lifecycle for an edge_function dispatch was:
--   :05  dispatch  -> is_running=true, last_succeeded_at=now()
--   :05+x  actual completion (no callback)
--   :35  reaper sees is_running=true for 30 min, marks last_failed_at=now()
--        + appends "reaped after 30min runtime" to last_error
--   :05 next hour  dispatch again, last_succeeded_at advances
--
-- Result: between :35 and the next :05 of every hour, the cron-audit
-- dashboard shows the job in FAILED state. The actual fires are fine
-- (last_succeeded_at advances) — it's a dashboard false-positive.
--
-- Fix: don't track is_running for edge_function dispatches. They're
-- fire-and-forget; the cron window + skip_if_succeeded_within already
-- prevent double-fire. Reaper still protects sql_function workers
-- where mid-function commit is impossible and overlap detection matters.
--
-- Also clears stale reaper-failures on the two affected hourly jobs so
-- the dashboard shows clean state immediately.

BEGIN;

CREATE OR REPLACE FUNCTION public.orch_tick()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $tick$
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
BEGIN
  IF NOT pg_try_advisory_xact_lock(v_lock_key) THEN
    RETURN jsonb_build_object('result','skipped','reason','tick already running');
  END IF;

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
          -- For edge_function: fire-and-forget, no is_running tracking.
          --   The reaper would false-positive every cycle (no callback
          --   to clear is_running). Cron window + skip_if_succeeded_within
          --   prevent double-fire.
          -- For sql_function: dispatch is synchronous (function call to here),
          --   so we know it succeeded. is_running stays false.
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

    IF v_status LIKE 'skipped_%' THEN v_n_skipped := v_n_skipped + 1; END IF;

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

  UPDATE public.orch_jobs j
     SET last_skip_reason = sub.skip_reason, updated_at = now()
    FROM (
      SELECT job_name, skip_reason
        FROM public.orch_runs
       WHERE intended_fire_at = v_now AND status LIKE 'skipped_%'
    ) sub
   WHERE j.job_name = sub.job_name;

  RETURN jsonb_build_object(
    'tick_at',     v_now,
    'evaluated',   v_n_evaluated,
    'fired',       v_n_fired,
    'shadowed',    v_n_shadowed,
    'skipped',     v_n_skipped,
    'failed',      v_n_failed,
    'actions',     v_summary
  );
END;
$tick$;

-- Clear stale "reaped" failures on the affected edge_function hourly jobs
-- so the dashboard reflects reality immediately.
UPDATE public.orch_jobs
   SET is_running    = FALSE,
       running_since = NULL,
       last_failed_at = NULL,
       last_error    = NULL,
       updated_at    = now()
 WHERE worker_kind = 'edge_function'
   AND last_error IS NOT NULL
   AND last_error LIKE '%reaped after 30min%';

COMMIT;
