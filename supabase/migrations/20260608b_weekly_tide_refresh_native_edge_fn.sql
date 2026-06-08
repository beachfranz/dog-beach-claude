-- 20260608b — weekly_tide_refresh: convert to native edge_function worker
--
-- WHY: The orch SQL-fn worker pattern wraps net.http_post inside a
-- pl/pgsql function and uses PERFORM, which discards the returned
-- request_id. _orch_dispatch's sql_function branch then EXECUTEs the
-- wrapper without an INTO clause — so orch_runs.worker_request_id stays
-- NULL, and _orch_reconcile_responses can never join to net._http_response
-- to mark 4xx/5xx failures.
--
-- Result: weekly_tide_refresh fired Sunday 06-07 08:00 UTC, the queued
-- POST landed something other than 2xx (or was delayed by ~18.5h for
-- unknown reasons), orch_jobs.last_succeeded_at was set on dispatch and
-- never rolled back. Grid stayed cold until 06-08 02:39 UTC. 1,074
-- beaches across 13 states (HI fully dark) got NULL tide for the full
-- 7-day score window via the cold-grid race that 20260608a's tide-warm
-- picker gate now prevents.
--
-- Fix here is the upstream half: switch the job to a native
-- worker_kind='edge_function' so the dispatcher's existing edge_fn
-- branch captures the request_id into orch_runs.worker_request_id.
-- The reconciler then sees the response, rolls back last_succeeded_at
-- on 4xx/5xx, surfaces the error on orch_jobs.last_error.
--
-- Body matches what _fire_weekly_tide_refresh() was POSTing.
-- timeout_milliseconds = 600_000 (10 min, matching the dispatcher's
-- default for edge_fn workers — generous vs. the loader's actual ~30s).
--
-- _fire_weekly_tide_refresh() is KEPT — it's documented in the noaa
-- skill as the manual-fire convenience + listed in
-- scripts/audit_artifacts.py ACTIVE_FUNCTIONS. Now redundant for the
-- cron path but still useful for ad-hoc operator invocation.
--
-- _orch_w_weekly_tide_refresh(jsonb) is DROPPED — it was the indirection
-- that hid the request_id from orch_runs. No other callers.
--
-- Companion follow-up tracked at [[orch-sql-fn-workers-blind-to-http-responses]]:
-- _orch_w_beach_refresh_chunked and _orch_w_dog_park_refresh_chunked
-- have the same pg_net blindness, but their 2-min cadence self-heals
-- failures within minutes. Lower priority. A broader sweep would
-- generalize _orch_dispatch's sql_function branch to capture bigint
-- returns + make every wrapper RETURNS bigint.

UPDATE public.orch_jobs
   SET worker_kind    = 'edge_function',
       worker_target  = 'refresh-tide-stations',
       worker_payload = '{"force_full": true, "limit": 500}'::jsonb,
       updated_at     = now()
 WHERE job_name = 'weekly_tide_refresh';

DROP FUNCTION IF EXISTS public._orch_w_weekly_tide_refresh(jsonb);
