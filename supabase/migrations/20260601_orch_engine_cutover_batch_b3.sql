-- 20260601_orch_engine_cutover_batch_b3.sql
--
-- Batch B3: flip verdict_cascade_nightly to live + unschedule the legacy
-- pg_cron entry. The legacy ran cleanly today at 03:00 UTC; tomorrow's
-- run (next scheduled fire) will be solely orch.
--
-- Worker wrapper is a one-line PERFORM around recompute_all_dogs_verdicts_by_origin()
-- which has been running nightly without issue.

BEGIN;

UPDATE public.orch_jobs
   SET shadow_mode = FALSE, updated_at = now()
 WHERE job_name = 'verdict_cascade_nightly';

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'verdict_cascade_nightly') THEN
    PERFORM cron.unschedule('verdict_cascade_nightly');
    RAISE NOTICE 'unscheduled legacy pg_cron: verdict_cascade_nightly';
  END IF;
END$$;

COMMIT;
