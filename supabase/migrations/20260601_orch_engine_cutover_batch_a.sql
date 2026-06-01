-- 20260601_orch_engine_cutover_batch_a.sql
--
-- First production cutover batch — 5 jobs. Two are Dagster-paired (no
-- legacy pg_cron, no double-fire risk). Three are pg_cron-paired with
-- low side effects of brief overlap, and we unschedule the legacy in
-- the same transaction.
--
-- All five worker functions were smoke-tested clean immediately before
-- this migration:
--   _orch_w_process_geom_change_queue (returned OK)
--   _orch_w_refresh_scoring_tier      (returned OK)
--   _orch_w_weekly_tide_refresh       (returned OK)
--   refresh-weather-grid edge fn      (smoke tested earlier today, 4ce8/72cf2d2)
--   _orch_w_compute_weather_advisories(smoke tested earlier today, fid 8344)

BEGIN;

-- ─── Tier 1: Dagster-paired (no legacy pg_cron) ──────────────────────

UPDATE public.orch_jobs
   SET shadow_mode = FALSE, updated_at = now()
 WHERE job_name IN ('weather_grid_hourly', 'daily_weather_advisories');

-- ─── Tier 2: pg_cron-paired (flip orch + unschedule legacy) ──────────

UPDATE public.orch_jobs
   SET shadow_mode = FALSE, updated_at = now()
 WHERE job_name IN ('process_geom_change_queue', 'daily_refresh_scoring_tier', 'weekly_tide_refresh');

-- Unschedule the legacy pg_cron entries that the orch_jobs rows are
-- now responsible for. If any of these aren't present (e.g., already
-- removed), unschedule raises — guard with EXISTS.
DO $$
DECLARE v_name TEXT;
BEGIN
  FOREACH v_name IN ARRAY ARRAY[
    'process_geom_change_queue',
    'daily_refresh_scoring_tier',
    'weekly_tide_refresh'
  ] LOOP
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = v_name) THEN
      PERFORM cron.unschedule(v_name);
      RAISE NOTICE 'unscheduled legacy pg_cron: %', v_name;
    END IF;
  END LOOP;
END$$;

COMMIT;
