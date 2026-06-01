-- 20260601_orch_engine_dep_sequencing_fixes.sql
--
-- Four catalog corrections found during the dep/sequencing review:
--
--   1. hourly_beach_now_refresh + hourly_dog_park_now_refresh read from
--      weather_grid_hourly (via weather_for_point RPC). Declare the dep
--      so a broken grid pipeline doesn't silently push stale NOW data.
--
--   2. Stagger the three hourly jobs so they don't collide at :00:
--        weather_grid_hourly      :00  (unchanged — grid first)
--        hourly_beach_now_refresh :05  (was :00 — would race grid)
--        hourly_dog_park_now_refresh :10 (was :05 — give beach NOW headroom)
--      Each is ~10-30s of work; 5-minute spacing eliminates overlap.
--
--   3. weekly_codify_gap_clone had depends_on = [daily_codify_coverage_audit]
--      but the audit RAISES when coverage drops below 80%, and gap_clone
--      is the tool that fixes regressions. The dep created a deadlock —
--      a sustained regression blocked the rescue. Remove the dep.
--
--   4. monthly_dog_park_coverage would double-fire: orch_jobs at
--      'monthly_on_at 1:08:00' AND the workflow's own
--      `schedule: '0 8 1 * *'` both trigger on the 1st of the month.
--      The .github/workflows/dp_coverage.yml schedule: block is removed in
--      a companion commit; this migration is structural only.

BEGIN;

-- Issue 1+2: hourly_beach_now_refresh — add weather_grid_hourly dep + move to :05
UPDATE public.orch_jobs
   SET cadence_param = '5',
       depends_on    = ARRAY['weather_grid_hourly']::TEXT[],
       updated_at    = now()
 WHERE job_name = 'hourly_beach_now_refresh';

-- Issue 1+2: hourly_dog_park_now_refresh — add weather_grid_hourly dep + move to :10
UPDATE public.orch_jobs
   SET cadence_param = '10',
       depends_on    = ARRAY['weather_grid_hourly']::TEXT[],
       updated_at    = now()
 WHERE job_name = 'hourly_dog_park_now_refresh';

-- Issue 3: weekly_codify_gap_clone — clear the deadlock-inducing dep
UPDATE public.orch_jobs
   SET depends_on = ARRAY[]::TEXT[],
       updated_at = now()
 WHERE job_name = 'weekly_codify_gap_clone';

COMMIT;
