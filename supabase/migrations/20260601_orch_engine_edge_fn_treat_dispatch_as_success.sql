-- 20260601_orch_engine_edge_fn_treat_dispatch_as_success.sql
--
-- Patch orch_tick to update last_succeeded_at when an edge_function
-- worker is DISPATCHED, not just when it would succeed (which we can't
-- observe without a callback).
--
-- Before: edge_function dispatch left last_succeeded_at untouched
-- because net.http_post is async — orch_tick has no way to know the
-- fn actually completed cleanly. This made depends_on chains break:
-- weather_grid_hourly is an edge_function; chunked refreshes depend on
-- it; chunked never fired because the dep never appeared fresh.
--
-- After: edge_function dispatch ALSO updates last_succeeded_at.
-- Failures are still surfaced via:
--   - The edge fn's own logs (Supabase dashboard)
--   - Stale data in the downstream tables (consumers see it)
--   - daily_pg_cron_audit + weekly_pipeline_health
--
-- A future real fix is callback-based success: have each long-running
-- edge fn POST to an orch-complete endpoint with the request_id and
-- outcome. For now this gets the bootstrap unblocked.
--
-- Surfaced 2026-06-01 during batch B1 cutover.

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

  UPDATE public.orch_jobs
     SET is_running = FALSE,
         running_since = NULL,
         running_request_id = NULL,
         last_failed_at = now(),
         last_error = COALESCE(last_error, '') || ' | orch: reaped after 30min runtime',
         updated_at = now()
   WHERE is_running = TRUE
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
          -- PATCH 2026-06-01: edge_function workers now update
          -- last_succeeded_at on dispatch (treating fire as success
          -- since net.http_post is async and we have no completion
          -- callback). Without this, depends_on chains never bootstrap.
          -- sql_function workers ran synchronously to here, so we know
          -- they succeeded.
          UPDATE public.orch_jobs
             SET is_running         = (v_job.worker_kind = 'edge_function'),
                 running_since      = CASE WHEN v_job.worker_kind = 'edge_function'
                                          THEN now() END,
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

-- Seed weather_grid_hourly.last_succeeded_at to unblock the chunked
-- refresh dep chain. The actual edge fn will fire at the next :00.
UPDATE public.orch_jobs
   SET last_succeeded_at = now() - interval '30 minutes',
       updated_at        = now()
 WHERE job_name = 'weather_grid_hourly';

COMMIT;
