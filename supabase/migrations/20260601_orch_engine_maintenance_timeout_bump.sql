-- 20260601_orch_engine_maintenance_timeout_bump.sql
--
-- Bump statement_timeout inside _orch_w_run_pipeline_maintenance to 30 min
-- per state. populate_arena_group_id and populate_arena_extras don't accept
-- p_state and run all-states spatial CTEs that exceed the default 60s timeout
-- as arena tables grow. Real fix is plumbing p_state into those functions
-- so the CTE can filter — pinned for follow-up. For now the bump unblocks
-- nightly maintenance.
--
-- See pin [[silent-pg-cron-failures]] for the diagnostic trail.

BEGIN;

CREATE OR REPLACE FUNCTION public._orch_w_run_pipeline_maintenance(p jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $wrap$
DECLARE
  v_states TEXT[];
  v_state  TEXT;
BEGIN
  -- Raise statement_timeout for the duration of this transaction.
  -- populate_arena_group_id runs a global-scope spatial CTE that exceeds
  -- the default 60s once arena tables grow. Bumping here is a mitigation
  -- until the all-states CTEs accept p_state.
  PERFORM set_config('statement_timeout', '1800000', true);  -- 30 min

  IF p ? 'states' THEN
    v_states := ARRAY(SELECT jsonb_array_elements_text(p->'states'));
  ELSIF p ? 'state' THEN
    v_states := ARRAY[p->>'state']::TEXT[];
  ELSE
    v_states := ARRAY['CA','OR','WA','MD','UT']::TEXT[];
  END IF;

  FOREACH v_state IN ARRAY v_states LOOP
    RAISE NOTICE 'nightly_pipeline_maintenance: running for %', v_state;
    PERFORM public.run_full_pipeline_maintenance(v_state);
  END LOOP;
END;
$wrap$;

COMMIT;
