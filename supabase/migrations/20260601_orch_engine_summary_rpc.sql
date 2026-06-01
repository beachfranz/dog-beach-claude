-- 20260601_orch_engine_summary_rpc.sql
--
-- Aggregate RPC for the cron-audit dashboard: count orch_runs by status
-- in the past N hours. Skipped rows are included so the dashboard can
-- show the full picture.

CREATE OR REPLACE FUNCTION public._orch_summary_last_hours(p_hours int)
RETURNS TABLE(status text, n bigint)
LANGUAGE sql STABLE PARALLEL SAFE AS $$
  SELECT status, count(*) AS n
    FROM public.orch_runs
   WHERE started_at >= now() - (p_hours || ' hours')::interval
   GROUP BY status
   ORDER BY n DESC;
$$;

COMMENT ON FUNCTION public._orch_summary_last_hours IS
  'Status counts over orch_runs in the past N hours. Used by admin-orch-status edge function.';
