-- 20260508_run_pipeline_for_state.sql
--
-- Step 4b: orchestrator function that runs the full bulletproofing chain
-- in one call. Suitable for state-load operations and nightly cron.
--
-- For OR (or any new state) loading, the workflow becomes:
--   1. Load PAD-US for the state (ad-hoc — scripts/load_pad_us.py --state OR)
--   2. Load arena rows for the state (ad-hoc — also a script)
--   3. SELECT public.run_pipeline_for_state('OR');
--      → clusters → extras → promotes → dedups → drains geom queue
--
-- For ongoing maintenance, schedule via pg_cron nightly. The function is
-- idempotent (each step skips already-processed work).

begin;

create or replace function public.run_pipeline_for_state(
  p_state text default 'CA',
  p_fids bigint[] default null
)
returns table(
  arena_grouped bigint,
  arena_extras_grouped bigint,
  promoted bigint,
  rows_already_in_gold bigint,
  dedup_kills bigint,
  bep_rows_migrated bigint,
  queue_processed bigint,
  ran_at timestamptz
)
language plpgsql
security definer
as $function$
declare
  v_fids bigint[];
  v_cluster_grp bigint := 0;
  v_extras_grp  bigint := 0;
  v_promoted    bigint := 0;
  v_existing    bigint := 0;
  v_kills       bigint := 0;
  v_bep_mig     bigint := 0;
  v_queue       bigint := 0;
  v_pf_rec      record;
  v_pe_rec      record;
  v_pg_rec      record;
  v_dd_rec      record;
  v_qq_rec      record;
begin
  -- 1. Cluster (state-agnostic; touches all arena rows)
  select * into v_pf_rec from public.populate_arena_group_id();
  v_cluster_grp := coalesce(v_pf_rec.relation_grouped, 0)
                 + coalesce(v_pf_rec.name_clustered, 0)
                 + coalesce(v_pf_rec.poi_matched, 0);

  -- 2. Clustering extras (intra-OSM trigram + intra-POI + cross-source)
  select * into v_pe_rec from public.populate_arena_extras();
  v_extras_grp := coalesce(v_pe_rec.intra_osm_trigram, 0)
                + coalesce(v_pe_rec.intra_poi, 0)
                + coalesce(v_pe_rec.cross_source_name, 0);

  -- 3. Determine target fids for promotion
  if p_fids is not null and array_length(p_fids, 1) > 0 then
    v_fids := p_fids;
  else
    -- Gather: arena rows in state that aren't yet in gold (or are in gold)
    -- Caller should batch large state-loads; this gathers everything.
    select array_agg(a.fid) into v_fids
      from public.arena a
     where a.is_active = true
       and (
         exists(
           select 1 from public.beaches_gold g
            where g.fid = a.fid and g.state = p_state
         )
         or
         (a.county_fips is not null and exists(
           select 1 from public.counties c
            where c.geoid = a.county_fips
              and c.state_fp = case p_state when 'CA' then '06'
                                            when 'OR' then '41'
                                            when 'WA' then '53' end
         ))
       );
  end if;

  -- 4. Promote (handles populators, resolvers, consensus, promote-canonical)
  if v_fids is not null and array_length(v_fids, 1) > 0 then
    select * into v_pg_rec from public.promote_to_gold(v_fids, false);
    v_promoted := coalesce(v_pg_rec.rows_promoted, 0);
    v_existing := coalesce(v_pg_rec.rows_already_in_gold, 0);
  end if;

  -- 5. Late-stage dedup with BEP migration
  select * into v_dd_rec from public.run_late_stage_dedup();
  v_kills   := coalesce(v_dd_rec.kills, 0);
  v_bep_mig := coalesce(v_dd_rec.bep_rows_migrated, 0);

  -- 6. Drain geom-change queue
  select * into v_qq_rec from public.process_geom_change_queue(100);
  v_queue := coalesce(v_qq_rec.fids_processed, 0);

  return query select
    v_cluster_grp, v_extras_grp, v_promoted, v_existing,
    v_kills, v_bep_mig, v_queue, now();
end;
$function$;

grant execute on function public.run_pipeline_for_state(text, bigint[]) to anon, authenticated, service_role;

-- Optional cron job for nightly maintenance. Disabled by default — uncomment
-- to enable. Runs at 4am UTC (after daily-beach-refresh at 9am UTC).
-- select cron.schedule(
--   'nightly_pipeline_maintenance_ca',
--   '0 4 * * *',
--   'select public.run_pipeline_for_state(''CA'');'
-- );

commit;
