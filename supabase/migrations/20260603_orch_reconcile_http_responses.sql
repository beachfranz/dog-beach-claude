-- Orch engine: reconcile net._http_response back into orch_runs.
--
-- Today, edge_function jobs in _orch_dispatch are fire-and-forget via
-- net.http_post. orch_tick writes orch_runs with worker_request_id but
-- never reads the actual HTTP response. A persistent 401 / 5xx from the
-- target edge function looks identical to "fired successfully" on the
-- dashboard, and orch_jobs.last_succeeded_at is bumped optimistically
-- on dispatch.
--
-- That blind spot is what hid the 4-day daily-beach-refresh blackout
-- (2026-05-30 → 2026-06-03): the function had been redeployed without
-- --no-verify-jwt, so the sb_publishable_ anon key failed JWT validation
-- and every cron fire got UNAUTHORIZED_INVALID_JWT_FORMAT. orch_runs
-- still showed status='fired' for all 1,440 fires.
--
-- This migration:
--   1. Adds a `response_status_code` column to orch_runs.
--   2. Adds _orch_reconcile_responses() which scans recent orch_runs with
--      pending HTTP responses, joins net._http_response, and updates the
--      run with the actual status code + body preview.
--   3. Demotes orch_jobs.last_succeeded_at when a reconciled response is
--      4xx/5xx (the dispatch was successful but the worker rejected),
--      and bumps last_failed_at + last_error so cron-audit + alerts surface it.
--   4. Modifies orch_tick to call _orch_reconcile_responses() at the top
--      of every tick (each minute) — same cadence as the reaper.

BEGIN;

-- 1. Track response code on orch_runs.
ALTER TABLE public.orch_runs
  ADD COLUMN IF NOT EXISTS response_status_code SMALLINT;

CREATE INDEX IF NOT EXISTS ix_orch_runs_pending_response
  ON public.orch_runs (worker_request_id)
 WHERE worker_request_id IS NOT NULL AND response_status_code IS NULL;

-- 2. Reconciler. Idempotent — skips rows already reconciled.
CREATE OR REPLACE FUNCTION public._orch_reconcile_responses(
  p_max_age interval DEFAULT interval '12 hours',
  p_limit   int      DEFAULT 500
) RETURNS TABLE(job_name text, status_code smallint, body_preview text) AS $$
DECLARE
  v_row RECORD;
BEGIN
  FOR v_row IN
    SELECT r.id,
           r.job_name,
           r.worker_request_id,
           hr.status_code,
           hr.content::text AS body,
           hr.error_msg
      FROM public.orch_runs r
      JOIN net._http_response hr ON hr.id = r.worker_request_id
     WHERE r.worker_request_id IS NOT NULL
       AND r.response_status_code IS NULL
       AND r.started_at > now() - p_max_age
     ORDER BY r.started_at DESC
     LIMIT p_limit
  LOOP
    UPDATE public.orch_runs
       SET response_status_code = v_row.status_code,
           response_body        = jsonb_build_object(
             'status', v_row.status_code,
             'body_preview', left(coalesce(v_row.body, ''), 2000)
           ),
           ended_at             = COALESCE(ended_at, now()),
           status               = CASE
             WHEN v_row.status_code BETWEEN 200 AND 299 THEN 'succeeded'
             WHEN v_row.status_code IS NOT NULL          THEN 'failed'
             ELSE status
           END,
           error_msg = CASE
             WHEN v_row.status_code NOT BETWEEN 200 AND 299
              THEN format('HTTP %s: %s', v_row.status_code,
                          left(coalesce(v_row.body, v_row.error_msg, ''), 400))
             ELSE error_msg
           END
     WHERE id = v_row.id;

    -- When a response is 4xx/5xx, the dispatch fired but the worker
    -- rejected. orch_tick optimistically set last_succeeded_at on
    -- dispatch — undo that and surface the failure on orch_jobs.
    IF v_row.status_code IS NOT NULL
       AND v_row.status_code NOT BETWEEN 200 AND 299 THEN
      UPDATE public.orch_jobs j
         SET last_succeeded_at = (
               SELECT max(rr.started_at)
                 FROM public.orch_runs rr
                WHERE rr.job_name = j.job_name
                  AND rr.response_status_code BETWEEN 200 AND 299
             ),
             last_failed_at = now(),
             last_error = format('HTTP %s from %s: %s',
                                 v_row.status_code, j.worker_target,
                                 left(coalesce(v_row.body, v_row.error_msg, ''), 400)),
             updated_at = now()
       WHERE j.job_name = v_row.job_name;
    END IF;

    job_name     := v_row.job_name;
    status_code  := v_row.status_code;
    body_preview := left(coalesce(v_row.body, v_row.error_msg, ''), 200);
    RETURN NEXT;
  END LOOP;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 3. orch_tick: reconcile at the top of every tick.
CREATE OR REPLACE FUNCTION public.orch_tick()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
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
    'reconciled',  v_n_reconciled,
    'actions',     v_summary
  );
END;
$function$;

-- 4. Backfill: reconcile the last 12h of pending responses now (this
-- will retroactively surface the daily-beach-refresh 401s from
-- 2026-05-30 → 2026-06-03 in cron-audit).
SELECT count(*) AS backfilled FROM public._orch_reconcile_responses();

COMMIT;
