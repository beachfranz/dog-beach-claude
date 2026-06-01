-- 20260601_orch_engine_cutover_batch_b1.sql
--
-- Cutover batch B1: chunked refreshes + NOW jobs out of shadow + unscheduling
-- the chunked legacy pg_cron entries. The NOW legacy entries stay for one
-- more cycle so the orch fires at :05 / :10 can be observed before legacy
-- removal — see batch B2 (after 13:10 UTC).
--
-- All four jobs depend on weather_grid_hourly (which itself is live as of
-- batch A). Brief overlap with legacy chunked pg_cron (~1 tick = 2 min)
-- is wasteful but harmless — the edge function has its own internal
-- staleness gate.

BEGIN;

UPDATE public.orch_jobs
   SET shadow_mode = FALSE, updated_at = now()
 WHERE job_name IN (
   'beach_refresh_chunked',
   'dog_park_refresh_chunked',
   'hourly_beach_now_refresh',
   'hourly_dog_park_now_refresh'
 );

-- Unschedule chunked legacy NOW (fast verification: orch fires in 2 min)
DO $$
DECLARE v_name TEXT;
BEGIN
  FOREACH v_name IN ARRAY ARRAY[
    'beach_refresh_chunked',
    'dog_park_refresh_chunked'
  ] LOOP
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = v_name) THEN
      PERFORM cron.unschedule(v_name);
      RAISE NOTICE 'unscheduled legacy pg_cron: %', v_name;
    END IF;
  END LOOP;
END$$;

-- NOTE: hourly-beach-now-refresh + hourly_dog_park_now_refresh legacy
-- entries are LEFT IN PLACE. They fire at :00 and :05; the orch versions
-- fire at :05 and :10. Brief overlap is fine. Unschedule them in batch
-- B2 after the orch fires succeed.

COMMIT;
