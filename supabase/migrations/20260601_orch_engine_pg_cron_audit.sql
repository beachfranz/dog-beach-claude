-- 20260601_orch_engine_pg_cron_audit.sql
--
-- Daily pg_cron audit. Catches the silent-rot failure mode that masked
-- nightly_pipeline_maintenance_ca dying for 5+ consecutive nights without
-- alerting anyone. See [[silent-pg-cron-failures]] pin.
--
-- Audit rule: a pg_cron job is "rotting" if it has any failure in the
-- past N days AND there's no successful run AFTER the most recent
-- failure. A failure followed by a success is treated as transient
-- (idempotent retries pass) and not flagged.
--
-- Excludes orch_tick from the audit because:
--   - orch_tick fires every minute (very chatty)
--   - orch_tick failures are visible in orch_runs separately
--
-- worker_payload knobs:
--   {"window_days": 7}             — how far back to scan (default 7)
--   {"exclude": ["jobname", ...]}  — additional jobs to skip
--
-- Cadence: daily_at 12:35 UTC (right after codify_coverage_audit at 12:30).

BEGIN;

CREATE OR REPLACE FUNCTION public._orch_w_audit_pg_cron_failures(p jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $audit$
DECLARE
  v_window_days INT     := COALESCE((p->>'window_days')::INT, 7);
  v_exclude     TEXT[]  := COALESCE(
    ARRAY(SELECT jsonb_array_elements_text(p->'exclude')),
    ARRAY[]::TEXT[]
  ) || ARRAY['orch_tick']::TEXT[];  -- always exclude self
  v_failures    TEXT[]  := ARRAY[]::TEXT[];
  v_rec         RECORD;
BEGIN
  FOR v_rec IN
    WITH window_data AS (
      SELECT d.jobid, d.status, d.start_time, d.return_message
        FROM cron.job_run_details d
       WHERE d.start_time > now() - (v_window_days || ' days')::interval
    ),
    per_job AS (
      SELECT j.jobname,
             COUNT(*) FILTER (WHERE wd.status='failed')    AS fails,
             COUNT(*) FILTER (WHERE wd.status='succeeded') AS succs,
             MAX(wd.start_time) FILTER (WHERE wd.status='failed')    AS last_fail,
             MAX(wd.start_time) FILTER (WHERE wd.status='succeeded') AS last_succ,
             MAX(wd.return_message) FILTER (WHERE wd.status='failed') AS last_err
        FROM cron.job j
        LEFT JOIN window_data wd ON wd.jobid = j.jobid
       WHERE j.active = TRUE
         AND NOT (j.jobname = ANY(v_exclude))
       GROUP BY j.jobname
    )
    SELECT *
      FROM per_job
     WHERE fails > 0
       -- Rotting if no success after most recent failure
       AND (last_succ IS NULL OR last_succ < last_fail)
     ORDER BY last_fail DESC NULLS LAST
  LOOP
    v_failures := v_failures || format(
      '%s — %s fail / %s ok in %s days; last fail %s; err: %s',
      v_rec.jobname,
      v_rec.fails,
      v_rec.succs,
      v_window_days,
      v_rec.last_fail,
      LEFT(COALESCE(v_rec.last_err, '(no message)'), 200)
    );
  END LOOP;

  IF array_length(v_failures, 1) > 0 THEN
    RAISE EXCEPTION
      E'pg_cron audit: % rotting job(s) in past % days:\n  %',
      array_length(v_failures, 1),
      v_window_days,
      array_to_string(v_failures, E'\n  ');
  END IF;

  RAISE NOTICE 'pg_cron audit: all clear (% days, % jobs excluded)',
    v_window_days, array_length(v_exclude, 1);
END;
$audit$;

COMMENT ON FUNCTION public._orch_w_audit_pg_cron_failures IS
  'orch_jobs worker: audit cron.job_run_details for jobs with failures and no subsequent success. Catches silent rot. See [[silent-pg-cron-failures]].';

-- ─── Insert into orch_jobs catalog ────────────────────────────────────

INSERT INTO public.orch_jobs (
  job_name, description,
  cadence_kind, cadence_param,
  depends_on,
  worker_kind, worker_target, worker_payload,
  scope_states, enabled, shadow_mode
) VALUES (
  'daily_pg_cron_audit',
  'Daily — scan cron.job_run_details for pg_cron jobs that failed in the past 7 days without a subsequent success. Catches silent rot. See [[silent-pg-cron-failures]].',
  'daily_at', '12:35',
  ARRAY[]::TEXT[],
  'sql_function', 'public._orch_w_audit_pg_cron_failures',
  jsonb_build_object('window_days', 7),
  ARRAY['CA','OR','WA','MD','UT']::TEXT[],
  TRUE,
  TRUE   -- shadow_mode: flip off after verifying via dry-run
)
ON CONFLICT (job_name) DO UPDATE
   SET description    = EXCLUDED.description,
       cadence_kind   = EXCLUDED.cadence_kind,
       cadence_param  = EXCLUDED.cadence_param,
       worker_kind    = EXCLUDED.worker_kind,
       worker_target  = EXCLUDED.worker_target,
       worker_payload = EXCLUDED.worker_payload,
       updated_at     = now();

COMMIT;
