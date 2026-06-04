-- Fix worker_kind on the three tier orch_jobs entries.
--
-- The 20260604_weather_grid_tiered_orch_jobs.sql migration set worker_kind
-- to 'edge_function' but pointed worker_target at SQL wrappers
-- (_orch_w_weather_grid_t1/t2/t3). The orch dispatcher's edge_function
-- path expects the worker_target to be the edge function NAME and
-- builds the HTTP call from worker_payload directly. The SQL wrappers
-- are redundant.
--
-- Repoint each tier job to call refresh-weather-grid directly with its
-- tier in worker_payload. Drop the unused wrappers.

BEGIN;

UPDATE public.orch_jobs
   SET worker_target = 'refresh-weather-grid',
       worker_payload = '{"tier":"t1","limit":600}'::jsonb,
       updated_at = now()
 WHERE job_name = 'weather_grid_t1';

UPDATE public.orch_jobs
   SET worker_target = 'refresh-weather-grid',
       worker_payload = '{"tier":"t2","limit":600}'::jsonb,
       updated_at = now()
 WHERE job_name = 'weather_grid_t2';

UPDATE public.orch_jobs
   SET worker_target = 'refresh-weather-grid',
       worker_payload = '{"tier":"t3","limit":600}'::jsonb,
       updated_at = now()
 WHERE job_name = 'weather_grid_t3';

DROP FUNCTION IF EXISTS public._orch_w_weather_grid_t1(jsonb);
DROP FUNCTION IF EXISTS public._orch_w_weather_grid_t2(jsonb);
DROP FUNCTION IF EXISTS public._orch_w_weather_grid_t3(jsonb);

COMMIT;
