-- 20260601_orch_engine_dp_coverage_quarterly.sql
--
-- Change monthly_dog_park_coverage to quarterly cadence (was
-- monthly_on_at '1:08:00'; now custom_cron '0 8 1 1,4,7,10 *' = 1st of
-- Jan/Apr/Jul/Oct at 08:00 UTC).
--
-- Background: the 8-op pipeline costs ~$25-75 in Anthropic LLM per
-- fire across MVP+ (5 states × ~$5-15 each). Operator-posted policy
-- doesn't drift fast enough to justify monthly — quarterly is the
-- freshness floor. Manual fires via workflow_dispatch cover urgent
-- needs.
--
-- See pin [[dp-coverage-cadence]] for the reasoning trail.
--
-- The job_name stays 'monthly_dog_park_coverage' for catalog stability
-- (the FK from orch_runs doesn't have ON UPDATE CASCADE). The truth
-- about cadence lives in cadence_kind + cron_expr + description.

BEGIN;

UPDATE public.orch_jobs
   SET cadence_kind  = 'custom_cron',
       cadence_param = NULL,
       cron_expr     = '0 8 1 1,4,7,10 *',
       description   = 'QUARTERLY (1st of Jan/Apr/Jul/Oct at 08:00 UTC) DP coverage refresh. 8-op pipeline via GitHub Actions. ~$25-75 LLM + ~1h wall per fire across MVP+. job_name reads "monthly" for historical PK stability; cadence is quarterly. See [[dp-coverage-cadence]].',
       updated_at    = now()
 WHERE job_name = 'monthly_dog_park_coverage';

COMMIT;
