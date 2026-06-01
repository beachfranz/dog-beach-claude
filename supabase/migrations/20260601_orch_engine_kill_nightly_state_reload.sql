-- 20260601_orch_engine_kill_nightly_state_reload.sql
--
-- Kill the nightly per-state pipeline re-run. A catalog reload is a
-- LAUNCH-TIME action, not a clock-time one. Drift catching is event-
-- driven (process_geom_change_queue, every 5 min, already orch-live).
-- The enrichers / dedup inside run_full_pipeline_maintenance are
-- idempotent no-ops on a stable state — running them nightly burns
-- DB CPU for zero benefit, and silently rotted for 5+ days last week.
--
-- Removes:
--   pg_cron: nightly_pipeline_maintenance_ca
--   orch_jobs: nightly_pipeline_maintenance (was shadow-mode)
--
-- If a state needs a manual reload (CCC re-publish, big POI churn,
-- new state launch), invoke run_full_pipeline_maintenance(<state>)
-- by hand or use the launch script.

BEGIN;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'nightly_pipeline_maintenance_ca') THEN
    PERFORM cron.unschedule('nightly_pipeline_maintenance_ca');
    RAISE NOTICE 'unscheduled legacy pg_cron: nightly_pipeline_maintenance_ca';
  END IF;
END$$;

DELETE FROM public.orch_jobs WHERE job_name = 'nightly_pipeline_maintenance';

COMMIT;
