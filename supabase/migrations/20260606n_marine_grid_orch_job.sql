-- 20260606n_marine_grid_orch_job.sql
--
-- Final piece of the marine-grid-reference-layer. Wires the refresh-marine-grid
-- edge fn into orch_jobs so it runs hourly automatically, then chains the
-- existing hourly_marine_advisories job to depend on it.
--
-- After this migration, the marine pipeline is self-sustaining:
--   :15 (every 60m)   refresh_marine_grid           → calls edge fn, refreshes
--                                                     stale cells in marine_grid_hourly
--   :20 (every 60m)   hourly_marine_advisories      → reads from marine_grid_hourly,
--                                                     writes beach_advisory rows
--
-- Ordering: marine_grid at :15, advisories 5 min later at :20. The advisory
-- job's depends_on=[refresh_marine_grid] means orch won't fire advisories
-- until the loader has actually succeeded recently — so the first cycle
-- after this migration will skip advisories until refresh_marine_grid
-- runs cleanly at the next :15.

INSERT INTO public.orch_jobs (
  job_name, description, cadence_kind, cadence_param, cron_expr,
  depends_on, depends_max_age, cost_check_fn, skip_if_succeeded_within,
  worker_kind, worker_target, worker_payload, scope_states,
  enabled, shadow_mode
) VALUES (
  'refresh_marine_grid',
  'Hourly: refresh marine_grid_hourly via Open-Meteo Marine API. Loader for the marine-grid-reference-layer; sibling of weather_grid_t1.',
  'hourly_at',
  '15',
  NULL,
  ARRAY[]::text[],
  NULL,
  NULL,
  NULL,
  'edge_function',
  'refresh-marine-grid',
  '{"limit": 400, "forecast_days": 7}'::jsonb,
  NULL,
  TRUE,
  FALSE
)
ON CONFLICT (job_name) DO UPDATE SET
  description   = EXCLUDED.description,
  cadence_kind  = EXCLUDED.cadence_kind,
  cadence_param = EXCLUDED.cadence_param,
  worker_kind   = EXCLUDED.worker_kind,
  worker_target = EXCLUDED.worker_target,
  worker_payload = EXCLUDED.worker_payload,
  enabled       = EXCLUDED.enabled;

-- Chain advisories to the loader + shift cadence to :20 so we fire AFTER
-- the loader at :15 (was :14 from 20260606h).
UPDATE public.orch_jobs
   SET depends_on   = ARRAY['refresh_marine_grid']::text[],
       cadence_param = '20',
       description  = 'Hourly: refresh beach_advisory marine_threshold rows via refresh_marine_advisories() SQL fn reading from marine_grid_hourly. Depends on refresh_marine_grid so we don''t produce stale advisories.'
 WHERE job_name = 'hourly_marine_advisories';
