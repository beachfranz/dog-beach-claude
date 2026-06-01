-- 20260601_orch_engine_unshadow_safe_six.sql
--
-- Flip 6 orch_jobs from shadow_mode=TRUE to shadow_mode=FALSE. These are
-- the read-only audits + idempotent backstops that can fire without
-- coordinating a legacy pg_cron unschedule.
--
-- The other 13 jobs in the catalog stay in shadow_mode=TRUE because each
-- has either:
--   - A duplicating legacy pg_cron entry (double-fire risk)
--   - Just-patched code awaiting observed success
--   - LLM/$ side effects requiring explicit approval
--   - Manual review tradition (codify mutation)
--
-- See docs/orch_cutover_playbook.md for the per-job cutover recipe.

BEGIN;

UPDATE public.orch_jobs
   SET shadow_mode = FALSE,
       updated_at  = now()
 WHERE job_name IN (
   'daily_pg_cron_audit',                      -- audit, RAISEs on rot
   'daily_codify_coverage_audit',              -- audit, RAISEs below 80%
   'weekly_dog_park_data_quality_audit',       -- audit, RAISEs on conflicts
   'weekly_pipeline_health',                   -- audit, 8 assertions
   'daily_dog_park_advisories',                -- gap-fill: STOPPED in Dagster
   'weather_grid_inventory'                    -- idempotent UNION; no legacy
 );

-- Sanity: capture the state at unshadow time for the audit log
INSERT INTO public.orch_runs (
  job_name, intended_fire_at, started_at, ended_at,
  status, skip_reason
)
SELECT job_name, date_trunc('minute', now()), now(), now(),
       'shadow',
       format('manual unshadow at %s', now()::text)
  FROM public.orch_jobs
 WHERE job_name IN (
   'daily_pg_cron_audit','daily_codify_coverage_audit',
   'weekly_dog_park_data_quality_audit','weekly_pipeline_health',
   'daily_dog_park_advisories','weather_grid_inventory'
 )
ON CONFLICT DO NOTHING;

COMMIT;
