-- Repoint beach_refresh_chunked dependency: weather_grid_hourly → weather_grid_t1.
--
-- The legacy `weather_grid_hourly` job was disabled when the tiered weather
-- refresh (T1/T2/T3) shipped in commit ed5f89b on 2026-06-04. But
-- beach_refresh_chunked.depends_on was never updated.
--
-- Today (~2026-06-04 16:30 UTC) the dep still tests "fresh" because the
-- disabled job's last_succeeded_at (2026-06-03 15:00) is within the 36h
-- depends_max_age window. But at 2026-06-05 03:00 UTC, the window ages out
-- and _orch_deps_ok will return FALSE permanently — silently blocking
-- daily-beach-refresh forever, same shape as the 2026-06-03 JWT blackout.
--
-- weather_grid_t1 is the right replacement: every 5 min, most active, most
-- often-fresh of the three tiers. Picking T1 over T2/T3 because:
--   T1 = 48h horizon (covers near-term forecast that daily-refresh consumes)
--   T2 = 96h horizon (currently hitting IDLE_TIMEOUT — separate issue)
--   T3 = 168h horizon (slowest cadence, hourly_at)

BEGIN;

UPDATE public.orch_jobs
   SET depends_on = ARRAY['weather_grid_t1'],
       updated_at = now()
 WHERE job_name = 'beach_refresh_chunked'
   AND depends_on = ARRAY['weather_grid_hourly'];

DO $$
DECLARE v_dep text; v_dep_ok boolean;
BEGIN
  SELECT depends_on[1] INTO v_dep
    FROM public.orch_jobs WHERE job_name = 'beach_refresh_chunked';

  SELECT (last_succeeded_at IS NOT NULL
          AND last_succeeded_at >= now() - interval '36 hours') INTO v_dep_ok
    FROM public.orch_jobs WHERE job_name = v_dep;

  RAISE NOTICE 'beach_refresh_chunked now depends on: %', v_dep;
  RAISE NOTICE 'Dep is fresh (within 36h): %', v_dep_ok;
END $$;

COMMIT;
