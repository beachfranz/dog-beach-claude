-- 20260531_orch_engine_dispatch.sql
--
-- Orchestration tick logic. Companion to 20260531_orch_engine_schema.sql.
--
-- Five helpers + the public.orch_tick() entry point.
--   _orch_match_cron_field — 5-field cron component matcher (* / */N / N-M / N-M/S / N,M,L)
--   _orch_should_fire      — does cadence say "fire now"?
--   _orch_deps_ok          — are depends_on jobs all fresh enough?
--   _orch_work_pending     — does cost_check_fn say work is pending?
--   _orch_dispatch         — fire the worker (edge function or SQL fn)
--   orch_tick              — main loop with topo ordering + advisory lock

BEGIN;

-- ─── _orch_match_cron_field ───────────────────────────────────────────

CREATE OR REPLACE FUNCTION public._orch_match_cron_field(
  p_field TEXT,
  p_value INT,
  p_min INT,
  p_max INT
) RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE PARALLEL SAFE
AS $$
DECLARE
  v_tok    TEXT;
  v_start  INT;
  v_end    INT;
  v_step   INT;
BEGIN
  IF p_field IS NULL OR p_field = '*' THEN RETURN TRUE; END IF;
  FOREACH v_tok IN ARRAY string_to_array(p_field, ',') LOOP
    -- '*/N'  — every Nth starting at p_min
    IF v_tok ~ '^\*/\d+$' THEN
      v_step := (regexp_match(v_tok, '/(\d+)$'))[1]::INT;
      IF (p_value - p_min) % v_step = 0 THEN RETURN TRUE; END IF;
    -- 'N-M/S' — range with step
    ELSIF v_tok ~ '^\d+-\d+/\d+$' THEN
      v_start := (regexp_match(v_tok, '^(\d+)'))[1]::INT;
      v_end   := (regexp_match(v_tok, '-(\d+)/'))[1]::INT;
      v_step  := (regexp_match(v_tok, '/(\d+)$'))[1]::INT;
      IF p_value BETWEEN v_start AND v_end
         AND (p_value - v_start) % v_step = 0 THEN RETURN TRUE; END IF;
    -- 'N-M'  — inclusive range
    ELSIF v_tok ~ '^\d+-\d+$' THEN
      v_start := (regexp_match(v_tok, '^(\d+)'))[1]::INT;
      v_end   := (regexp_match(v_tok, '-(\d+)$'))[1]::INT;
      IF p_value BETWEEN v_start AND v_end THEN RETURN TRUE; END IF;
    -- 'N'   — exact match
    ELSIF v_tok ~ '^\d+$' THEN
      IF v_tok::INT = p_value THEN RETURN TRUE; END IF;
    END IF;
  END LOOP;
  RETURN FALSE;
END;
$$;

-- ─── _orch_should_fire ────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public._orch_should_fire(
  p_job public.orch_jobs,
  p_now TIMESTAMPTZ DEFAULT now()
) RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE PARALLEL SAFE
AS $$
DECLARE
  v_min    INT  := EXTRACT(minute FROM p_now)::INT;
  v_hour   INT  := EXTRACT(hour FROM p_now)::INT;
  v_dom    INT  := EXTRACT(day FROM p_now)::INT;
  v_mon    INT  := EXTRACT(month FROM p_now)::INT;
  v_dow    INT  := EXTRACT(isodow FROM p_now)::INT;  -- 1=Mon..7=Sun
  v_parts  TEXT[];
  v_dow_name TEXT;
  v_dow_target INT;
BEGIN
  CASE p_job.cadence_kind
    WHEN 'every_n_minutes' THEN
      RETURN v_min % COALESCE(p_job.cadence_param, '1')::INT = 0;

    WHEN 'hourly_at' THEN
      RETURN v_min = COALESCE(p_job.cadence_param, '0')::INT;

    WHEN 'daily_at' THEN
      v_parts := string_to_array(p_job.cadence_param, ':');
      RETURN v_hour = v_parts[1]::INT
         AND v_min  = v_parts[2]::INT;

    WHEN 'weekly_on_at' THEN
      v_parts := string_to_array(p_job.cadence_param, ':');
      v_dow_name := lower(v_parts[1]);
      v_dow_target := CASE v_dow_name
        WHEN 'mon' THEN 1 WHEN 'tue' THEN 2 WHEN 'wed' THEN 3
        WHEN 'thu' THEN 4 WHEN 'fri' THEN 5 WHEN 'sat' THEN 6
        WHEN 'sun' THEN 7
      END;
      RETURN v_dow_target = v_dow
         AND v_hour = v_parts[2]::INT
         AND v_min  = v_parts[3]::INT;

    WHEN 'monthly_on_at' THEN
      v_parts := string_to_array(p_job.cadence_param, ':');
      RETURN v_dom = v_parts[1]::INT
         AND v_hour = v_parts[2]::INT
         AND v_min  = v_parts[3]::INT;

    WHEN 'custom_cron' THEN
      v_parts := string_to_array(p_job.cron_expr, ' ');
      IF array_length(v_parts, 1) <> 5 THEN RETURN FALSE; END IF;
      RETURN public._orch_match_cron_field(v_parts[1], v_min,  0, 59)
         AND public._orch_match_cron_field(v_parts[2], v_hour, 0, 23)
         AND public._orch_match_cron_field(v_parts[3], v_dom,  1, 31)
         AND public._orch_match_cron_field(v_parts[4], v_mon,  1, 12)
         -- cron dow: 0=Sun..6=Sat; pg isodow: 1=Mon..7=Sun
         AND public._orch_match_cron_field(v_parts[5], v_dow % 7, 0, 6);
  END CASE;
  RETURN FALSE;
END;
$$;

-- ─── _orch_deps_ok ────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public._orch_deps_ok(p_job public.orch_jobs)
RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE PARALLEL SAFE
AS $$
DECLARE
  v_dep TEXT;
  v_dep_succ TIMESTAMPTZ;
BEGIN
  IF p_job.depends_on IS NULL OR array_length(p_job.depends_on, 1) IS NULL THEN
    RETURN TRUE;
  END IF;
  FOREACH v_dep IN ARRAY p_job.depends_on LOOP
    SELECT last_succeeded_at INTO v_dep_succ
      FROM public.orch_jobs WHERE job_name = v_dep;
    IF NOT FOUND THEN
      RETURN FALSE;  -- declared dep does not exist
    END IF;
    IF v_dep_succ IS NULL OR v_dep_succ < now() - p_job.depends_max_age THEN
      RETURN FALSE;  -- dep too stale
    END IF;
  END LOOP;
  RETURN TRUE;
END;
$$;

-- ─── _orch_work_pending ───────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public._orch_work_pending(p_job public.orch_jobs)
RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_result BOOLEAN;
BEGIN
  IF p_job.cost_check_fn IS NULL THEN RETURN TRUE; END IF;
  BEGIN
    EXECUTE format('SELECT %s($1)', p_job.cost_check_fn)
      USING p_job INTO v_result;
  EXCEPTION WHEN OTHERS THEN
    -- Cost check broken; default to firing so we don't accidentally pause
    -- the whole pipeline on a bad cost_check_fn deploy.
    RETURN TRUE;
  END;
  RETURN COALESCE(v_result, TRUE);
END;
$$;

-- ─── _orch_dispatch ───────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public._orch_dispatch(p_job_name TEXT)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
AS $disp$
DECLARE
  v_job        public.orch_jobs;
  v_url        TEXT;
  v_request_id BIGINT := NULL;
BEGIN
  SELECT * INTO v_job FROM public.orch_jobs WHERE job_name = p_job_name;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'orch_jobs has no row for %', p_job_name;
  END IF;

  IF v_job.worker_kind = 'edge_function' THEN
    v_url := format(
      'https://ehlzbwtrsxaaukurekau.supabase.co/functions/v1/%s',
      v_job.worker_target
    );
    SELECT net.http_post(
      url := v_url,
      headers := jsonb_build_object(
        'Content-Type',    'application/json',
        'Authorization',   'Bearer sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
        'apikey',          'sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
        'x-admin-secret',  '8JMgk1BEGN0FiEJiivP9_34w-dTJzKTu'
      ),
      body := v_job.worker_payload,
      timeout_milliseconds := 600000
    ) INTO v_request_id;
  ELSIF v_job.worker_kind = 'sql_function' THEN
    -- SQL workers must take a jsonb arg. Wrap legacy no-arg functions like:
    --   CREATE FUNCTION public._orch_x(jsonb) RETURNS void LANGUAGE sql AS ...
    EXECUTE 'SELECT ' || v_job.worker_target || '($1)'
      USING v_job.worker_payload;
  ELSIF v_job.worker_kind = 'noop' THEN
    NULL;
  END IF;

  RETURN v_request_id;
END;
$disp$;

-- ─── orch_tick (entry point) ──────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.orch_tick()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
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
  -- Advisory lock so two concurrent orch_tick calls don't both fire jobs.
  -- pg_try_advisory_xact_lock returns false if held; we exit cleanly.
  IF NOT pg_try_advisory_xact_lock(v_lock_key) THEN
    RETURN jsonb_build_object('result','skipped','reason','tick already running');
  END IF;

  -- Reap stuck jobs: anything is_running for >30 min is presumed dead.
  -- (edge functions return immediately via net.http_post; their long-running
  -- work proceeds out-of-band, but if we don't get a completion callback,
  -- this lets the next cycle pick the job back up.)
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

  -- Iterate enabled jobs in dependency-depth order. Shallow chains (1-2 deps
  -- deep) are handled by COALESCE(array_length(depends_on, 1), 0) sort.
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
          UPDATE public.orch_jobs
             SET is_running         = (v_job.worker_kind = 'edge_function'),
                 running_since      = CASE WHEN v_job.worker_kind = 'edge_function'
                                          THEN now() END,
                 running_request_id = v_request_id,
                 last_attempted_at  = now(),
                 last_succeeded_at  = CASE WHEN v_job.worker_kind <> 'edge_function'
                                          THEN now()
                                          ELSE last_succeeded_at END,
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

  -- Update last_skip_reason for jobs that were skipped this tick (useful
  -- for status dashboards — "why isn't X firing?").
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
$$;

COMMIT;
