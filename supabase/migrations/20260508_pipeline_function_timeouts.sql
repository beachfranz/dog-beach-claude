-- 20260508_pipeline_function_timeouts.sql
--
-- run_pipeline_for_state does a lot of work (clustering all arena rows +
-- populate_arena_extras + late_stage_dedup + queue processing + enqueue).
-- The default 8s statement_timeout for non-service_role contexts kills it.
-- Edge functions invoke this via supabase-js which can run as either role
-- depending on how the client is constructed; pin a generous timeout
-- on the function itself so the launcher works regardless.

begin;

alter function public.run_pipeline_for_state(text, bigint[])
  set statement_timeout = '180s';

alter function public.populate_arena_group_id()
  set statement_timeout = '120s';

alter function public.populate_arena_extras()
  set statement_timeout = '120s';

alter function public.run_late_stage_dedup()
  set statement_timeout = '120s';

alter function public.repair_noaa_for_state(text)
  set statement_timeout = '60s';

alter function public.ingest_osm_elements(jsonb, text)
  set statement_timeout = '120s';

commit;
