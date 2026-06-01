-- 20260601_orch_engine_quarterly_operators.sql
--
-- Adds a quarterly job that refreshes operators across MVP+ states via
-- the populate_operators_for_state procedure. Companion to
-- 20260601_orch_engine_skip_broken_operators_in_maintenance.sql which
-- removed the broken nightly call.
--
-- Why pg_cron directly (not orch_jobs):
--   populate_operators_for_state is a PROCEDURE with multiple internal
--   `commit;` statements (one per pass). It cannot be CALLed from inside
--   a function (functions run in a transaction; commits forbidden).
--   The orch_jobs sql_function worker is a function, so it can't drive
--   this procedure. pg_cron runs each job in a top-level session, so
--   the procedure's commits work there. We make a wrapper procedure
--   that loops MVP+ states and the cron entry CALLs the wrapper.

BEGIN;

CREATE OR REPLACE PROCEDURE public._orch_populate_operators_multistate(
  p_states TEXT[]
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $proc$
DECLARE
  v_state          TEXT;
  v_cities         INT;
  v_towns          INT;
  v_counties       INT;
  v_federal        INT;
  v_state_count    INT;
  v_district       INT;
  v_ngo            INT;
  v_pvt            INT;
  v_tribal         INT;
  v_joint          INT;
  v_bia            INT;
  v_osm            INT;
  v_bep            INT;
  v_linkage        INT;
  v_consolidated   INT;
BEGIN
  FOREACH v_state IN ARRAY p_states LOOP
    RAISE NOTICE 'populate_operators_for_state: %', v_state;
    CALL public.populate_operators_for_state(
      v_state,
      v_cities, v_towns, v_counties, v_federal,
      v_state_count, v_district, v_ngo, v_pvt, v_tribal, v_joint,
      v_bia, v_osm, v_bep, v_linkage, v_consolidated
    );
    -- Commit between states so a later failure doesn't roll back earlier
    -- states' work.
    COMMIT;
  END LOOP;
END;
$proc$;

COMMENT ON PROCEDURE public._orch_populate_operators_multistate IS
  'Loop populate_operators_for_state over a list of state codes. Called quarterly via pg_cron — see quarterly_populate_operators job.';

-- Unschedule any prior version of this cron entry to make re-runs idempotent
SELECT cron.unschedule('quarterly_populate_operators')
 WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'quarterly_populate_operators');

-- Quarterly: 1st of Jan, Apr, Jul, Oct at 06:00 UTC.
-- Operators don't change often; this is a freshness floor, not real-time.
SELECT cron.schedule(
  'quarterly_populate_operators',
  '0 6 1 1,4,7,10 *',
  $$CALL public._orch_populate_operators_multistate(ARRAY['CA','OR','WA','MD','UT']::TEXT[])$$
);

COMMIT;
