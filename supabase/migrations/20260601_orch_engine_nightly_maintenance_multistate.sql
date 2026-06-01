-- 20260601_orch_engine_nightly_maintenance_multistate.sql
--
-- Issue #5 fix from the catalog review: nightly_pipeline_maintenance had
-- scope_states = ['CA'] only. The underlying run_full_pipeline_maintenance
-- function is mostly state-INDEPENDENT (promote_poi_landing,
-- promote_osm_landing, refresh_arena_names, refire_missing_bep,
-- enqueue_missing_extractions, publish_stale_fids are all global) — but
-- it does take p_state and threads it through run_pipeline_for_state,
-- which IS fully per-state (operators populate, address-city enrichment,
-- address-from-POI, name-source, late-stage dedup). With scope='CA' only,
-- OR/WA/MD/UT silently skipped their per-state enrichment.
--
-- Approach A from the review: loop in the worker wrapper. Pros:
--   - One catalog row, simple semantics
--   - The 5x repetition of global functions is idempotent and a queue
--     drainer (refire_missing_bep(150) effectively becomes 5x150=750 per
--     night — helps not hurts)
--   - Minimal code change vs splitting the function

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
  -- Payload conventions:
  --   {"states":["CA","OR"]}   — explicit list; preferred
  --   {"state":"CA"}           — legacy single-state; kept for backwards compat
  --   {}                       — default to MVP+
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

-- Update the catalog row to MVP+ scope and multi-state payload
UPDATE public.orch_jobs
   SET worker_payload = '{"states":["CA","OR","WA","MD","UT"]}'::jsonb,
       scope_states   = ARRAY['CA','OR','WA','MD','UT']::TEXT[],
       description    = 'Run full pipeline maintenance for each MVP+ state. Wrapper loops calls to run_full_pipeline_maintenance(state); global functions inside fire 5x but they are idempotent + drain queues faster, which is desirable.',
       updated_at     = now()
 WHERE job_name = 'nightly_pipeline_maintenance';

COMMIT;
