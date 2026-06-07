-- 20260606r_orch_tick_statement_timeout.sql
--
-- Override statement_timeout for public.orch_tick().
--
-- Root cause (today): pg_cron's `orch_tick` was hitting the default
-- 120s statement_timeout on every mod-5 minute tick. The inline
-- dispatch of multiple every_n_minutes='5' sql_function workers
-- (refresh_beach_hourly_scores_bulk, apply_v2_best_window_beach,
-- apply_v2_best_window_dog_park, etc.) combined to exceed 120s. The
-- whole tick txn rolled back, advisory lock released, no orch_runs
-- rows landed. Result: A0 + the apply_v2 family appeared "never
-- fired" in orch_jobs.last_attempted_at while in fact orch_tick
-- itself was crashing before ever reaching them.
--
-- Confirmed via cron.job_run_details: ticks at 18:25 and 18:30 both
-- reported status='failed' dur_s=120 in a 12-min sample. Mod-2 ticks
-- (every minute except mod-5) all succeeded in <1s.
--
-- Fix: raise statement_timeout to 10min INSIDE orch_tick only. The
-- 30-minute lock-reaper inside orch_tick itself still catches any
-- truly stuck sql_function worker; this just stops a single heavy
-- mod-5 tick from being killed by the default 2-min cutoff.
--
-- Why ALTER FUNCTION SET (not a tx-local SET): the GUC applies to
-- every invocation of orch_tick (pg_cron + manual), without
-- depending on the caller setting it.
--
-- Why 10min specifically: matches the orch contract that sql_function
-- workers complete in a single tick; gives ample headroom for the
-- 5-min cadence stack to grow before the reaper kicks in. If a tick
-- takes >10min, that's a real bug, not a config issue.

ALTER FUNCTION public.orch_tick() SET statement_timeout = '10min';

COMMENT ON FUNCTION public.orch_tick IS
  'Dispatch loop fired every minute by pg_cron. Iterates orch_jobs, '
  'fires those whose cadence window matches now() AND deps are fresh '
  'AND cost_check_fn returns true. sql_function workers dispatch '
  'inline; edge_function workers fire-and-forget. statement_timeout '
  'bumped to 10min via ALTER FUNCTION SET (20260606r) to survive '
  'mod-5 minute ticks where multiple every-5-min sql_function '
  'workers run inline.';
