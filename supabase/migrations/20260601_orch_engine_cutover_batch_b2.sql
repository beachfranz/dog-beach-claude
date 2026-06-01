-- 20260601_orch_engine_cutover_batch_b2.sql
--
-- Batch B2: unschedule the legacy hourly-NOW pg_cron entries. The orch
-- versions are live as of batch B1 (49067be). One fire confirmed live:
--
--   12:10 UTC — hourly_dog_park_now_refresh fired with request_id 664
--               (edge_function dispatch succeeded)
--
-- hourly_beach_now_refresh hasn't fired yet this hour (next 13:05 UTC)
-- but the edge dispatch path is the same shape — confirmed by DP NOW
-- above + the every-2-min chunked refreshes firing cleanly.

BEGIN;

DO $$
DECLARE v_name TEXT;
BEGIN
  FOREACH v_name IN ARRAY ARRAY[
    'hourly-beach-now-refresh',     -- note: dash, not underscore (legacy)
    'hourly_dog_park_now_refresh'
  ] LOOP
    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = v_name) THEN
      PERFORM cron.unschedule(v_name);
      RAISE NOTICE 'unscheduled legacy pg_cron: %', v_name;
    ELSE
      RAISE NOTICE 'no legacy pg_cron entry %; nothing to do', v_name;
    END IF;
  END LOOP;
END$$;

COMMIT;
