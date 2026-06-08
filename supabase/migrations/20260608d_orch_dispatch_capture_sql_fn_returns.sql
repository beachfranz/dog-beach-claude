-- 20260608d — Option A part 2: dispatcher captures SQL-fn bigint returns
--
-- WHY: With 20260608c, every SQL-fn wrapper now returns bigint (NULL for
-- pure-SQL workers, the pg_net request_id for HTTP-firing workers).
-- The dispatcher's sql_function branch was discarding the return:
--   EXECUTE 'SELECT ' || worker_target || '($1)' USING worker_payload;
-- Adding INTO v_request_id makes the dispatcher capture it, so
-- orch_runs.worker_request_id gets populated for HTTP-firing wrappers and
-- _orch_reconcile_responses can finally join to net._http_response to
-- mark 4xx/5xx failures.
--
-- For pure-SQL wrappers that return NULL, the column stays NULL — same
-- shape the reconciler already expects for "nothing to reconcile."
--
-- Order matters: 20260608c (wrappers → bigint) MUST land before this
-- migration. Otherwise EXECUTE ... INTO bigint on a void-returning fn
-- would error.

CREATE OR REPLACE FUNCTION public._orch_dispatch(p_job_name text)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
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
    -- 20260608d: capture wrapper's bigint return (was: discarded).
    -- HTTP-firing wrappers return the pg_net request_id so the
    -- reconciler can join to net._http_response. Pure-SQL wrappers
    -- return NULL.
    EXECUTE 'SELECT ' || v_job.worker_target || '($1)'
      INTO v_request_id
      USING v_job.worker_payload;
  ELSIF v_job.worker_kind = 'noop' THEN
    NULL;
  END IF;

  RETURN v_request_id;
END;
$function$;
