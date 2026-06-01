-- 20260531_orch_engine_cron_entry.sql
--
-- pg_cron entry to fire orch_tick every minute. The orchestrator itself
-- decides what (if anything) actually runs based on the orch_jobs catalog.
--
-- Side note: the existing pg_cron jobs (beach_refresh_chunked,
-- dog_park_refresh_chunked, hourly-beach-now-refresh, etc.) are LEFT IN
-- PLACE. orch_jobs catalogs them with shadow_mode=TRUE so the orchestrator
-- only logs decisions; nothing double-fires. When you're ready to cut
-- over a job:
--   1. Verify orch_runs shows the orchestrator's decisions look right
--   2. Flip shadow_mode=FALSE for the job
--   3. Disable the equivalent pg_cron job: `SELECT cron.unschedule('<name>');`
--
-- Idempotency: the unschedule below removes any prior 'orch_tick' job so
-- re-running the migration is safe.

SELECT cron.unschedule('orch_tick')
 WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'orch_tick');

SELECT cron.schedule(
  'orch_tick',
  '* * * * *',          -- every minute
  $$SELECT public.orch_tick();$$
);
