-- 20260508_run_full_pipeline_maintenance.sql
--
-- Phase 6: top-level pipeline maintenance orchestrator + nightly cron schedule.
--
-- Order:
--   1. Promote new POI landing rows to arena
--   2. Promote new OSM landing rows to arena
--   3. Refresh arena.name from any OSM landing updates
--   4. run_pipeline_for_state — cluster + extras + promote + dedup + drain queue
--
-- Idempotent end-to-end; every step skips already-done work.

begin;

create or replace function public.run_full_pipeline_maintenance(p_state text default 'CA')
returns table(
  poi_promoted_to_arena bigint,
  osm_promoted_to_arena bigint,
  names_refreshed bigint,
  arena_grouped bigint,
  arena_extras_grouped bigint,
  promoted_to_gold bigint,
  dedup_kills bigint,
  bep_rows_migrated bigint,
  queue_processed bigint,
  ran_at timestamptz
)
language plpgsql
security definer
as $function$
declare
  v_poi_to_arena bigint := 0;
  v_osm_to_arena bigint := 0;
  v_names_ref    bigint := 0;
  v_pf_rec record;
  v_pi_rec record;
  v_po_rec record;
  v_nr_rec record;
begin
  -- 1. POI landing → arena
  select * into v_pi_rec from public.promote_poi_landing_to_arena();
  v_poi_to_arena := coalesce(v_pi_rec.promoted, 0);

  -- 2. OSM landing → arena
  select * into v_po_rec from public.promote_osm_landing_to_arena();
  v_osm_to_arena := coalesce(v_po_rec.promoted, 0);

  -- 3. Name refresh
  select * into v_nr_rec from public.refresh_arena_names_from_osm_landing();
  v_names_ref := coalesce(v_nr_rec.arena_rows_updated, 0);

  -- 4. Full pipeline (cluster + extras + promote + dedup + queue)
  select * into v_pf_rec from public.run_pipeline_for_state(p_state);

  return query select
    v_poi_to_arena, v_osm_to_arena, v_names_ref,
    coalesce(v_pf_rec.arena_grouped, 0),
    coalesce(v_pf_rec.arena_extras_grouped, 0),
    coalesce(v_pf_rec.promoted, 0),
    coalesce(v_pf_rec.dedup_kills, 0),
    coalesce(v_pf_rec.bep_rows_migrated, 0),
    coalesce(v_pf_rec.queue_processed, 0),
    now();
end;
$function$;

grant execute on function public.run_full_pipeline_maintenance(text) to anon, authenticated, service_role;

-- pg_cron: nightly maintenance at 4am UTC. Unschedule any existing duplicate first.
do $$
begin
  perform cron.unschedule('nightly_pipeline_maintenance_ca');
exception when others then null;
end $$;

select cron.schedule(
  'nightly_pipeline_maintenance_ca',
  '0 4 * * *',
  $cron$select public.run_full_pipeline_maintenance('CA');$cron$
);

commit;
