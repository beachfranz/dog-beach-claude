-- orch_jobs.depends_on health audit + data fix for today's silent blackout.
--
-- Findings 2026-06-05:
--   1. dog_park_refresh_chunked.depends_on = ['weather_grid_hourly']
--      hourly_beach_now_refresh.depends_on = ['weather_grid_hourly']
--      hourly_dog_park_now_refresh.depends_on = ['weather_grid_hourly']
--   2. weather_grid_hourly was disabled (enabled=false) when the hourly
--      grid loader was split into T1/T2/T3 tiers. The disabled job's
--      last_succeeded_at froze at 2026-06-03 15:00.
--   3. _orch_deps_ok checks `last_succeeded_at >= now() - depends_max_age`
--      (36h). With weather_grid_hourly frozen, the check returns false.
--   4. orch_tick silently skips these 3 jobs. last_attempted_at = 2026-06-05
--      03:00 (when depends_max_age first expired); last_ok = NULL for
--      dog_park_refresh_chunked.
--
-- beach_refresh_chunked was repointed earlier today (Franz, in safeword
-- pin) but the 3 sibling jobs were missed. Mirror today's encoded HARD
-- rule [[paired-functions-port-fixes-both-sides]] — fix all four together.
--
-- Part 1: repoint the 3 broken depends_on entries.

UPDATE public.orch_jobs
   SET depends_on = ARRAY['weather_grid_t1'],
       updated_at = now()
 WHERE job_name IN (
   'dog_park_refresh_chunked',
   'hourly_beach_now_refresh',
   'hourly_dog_park_now_refresh'
 )
   AND depends_on @> ARRAY['weather_grid_hourly'];

-- Part 2: build the health audit.
--
-- Walks every enabled orch_jobs row and validates each depends_on entry:
--   - dep job_name doesn't exist in orch_jobs → broken
--   - dep job_name exists but enabled=false → broken (will never succeed)
-- Reports findings as a semicolon-separated list in orch_jobs.last_error
-- for job_name='orch_deps_health_audit'. Sink matches other audits so
-- the dashboard surface stays uniform.
--
-- Note: does NOT alert on stale last_succeeded_at — the existing
-- _orch_deps_ok logic already drives dispatch decisions on freshness;
-- this audit's job is to catch STRUCTURAL breakage (broken refs +
-- disabled deps) which can't recover without manual intervention.

CREATE OR REPLACE FUNCTION public._orch_w_orch_deps_health_audit(p jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
  v_findings text;
  v_count    int;
BEGIN
  WITH dep_check AS (
    SELECT
      j.job_name AS depender,
      unnest(j.depends_on) AS dep
    FROM public.orch_jobs j
    WHERE j.enabled = true
      AND j.depends_on IS NOT NULL
      AND array_length(j.depends_on, 1) > 0
  ),
  broken AS (
    SELECT
      d.depender,
      d.dep,
      CASE
        WHEN target.job_name IS NULL THEN 'missing'
        WHEN target.enabled = false  THEN 'disabled'
      END AS reason
    FROM dep_check d
    LEFT JOIN public.orch_jobs target ON target.job_name = d.dep
    WHERE target.job_name IS NULL OR target.enabled = false
  )
  SELECT
    count(*),
    string_agg(
      format('%s→%s (%s)', depender, dep, reason),
      '; ' ORDER BY depender, dep
    )
  INTO v_count, v_findings
  FROM broken;

  IF v_count > 0 THEN
    UPDATE public.orch_jobs
       SET last_failed_at = now(),
           last_error = format(
             'orch_deps_health: %s broken depends_on entries — %s. These jobs are silently skipped by orch_tick. Fix via UPDATE orch_jobs SET depends_on = ARRAY[''<correct_dep>''] WHERE job_name = ''<depender>''.',
             v_count, v_findings
           ),
           updated_at = now()
     WHERE job_name = 'orch_deps_health_audit';
  ELSE
    UPDATE public.orch_jobs
       SET last_error = NULL,
           updated_at = now()
     WHERE job_name = 'orch_deps_health_audit'
       AND last_error LIKE 'orch_deps_health:%';
  END IF;
END;
$function$;

-- Part 3: register the audit as a recurring job. Hourly cadence (every
-- 60 min) — depends_on misalignment surfaces immediately when a renamed
-- job's old name lingers in dependents, so detection lag should be short.

INSERT INTO public.orch_jobs (
  job_name,
  cadence_kind,
  cadence_param,
  worker_kind,
  worker_target,
  enabled,
  depends_max_age,
  scope_states,
  shadow_mode
) VALUES (
  'orch_deps_health_audit',
  'every_n_minutes',
  '60',
  'sql_function',
  '_orch_w_orch_deps_health_audit',
  true,
  '24:00:00'::interval,
  NULL,
  false
)
ON CONFLICT (job_name) DO UPDATE SET
  worker_target = EXCLUDED.worker_target,
  cadence_param = EXCLUDED.cadence_param,
  enabled = EXCLUDED.enabled,
  updated_at = now();
