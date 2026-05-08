-- 20260508_chunked_promote_in_pipeline.sql
--
-- run_pipeline_for_state's promote_to_gold call passed all v_fids at
-- once. The per-fid populator loop inside promote_to_gold (10 populator
-- + 5 resolver calls × N fids) doesn't scale past ~150 fids before
-- hitting statement_timeout. WA's first launch hit this wall hard:
-- 433 fids × ~125ms/call × 15 calls = ~810s, blowing past 900s timeout.
--
-- Chunk the promote into batches of 50 inside run_pipeline_for_state
-- so each promote_to_gold call processes a manageable slice. Tested
-- in Python wrapper: 11 batches of 50, each completing in 1.6–5.5s.

begin;

drop function if exists public.run_pipeline_for_state(text, bigint[]);
create or replace function public.run_pipeline_for_state(
  p_state text default 'CA',
  p_fids  bigint[] default null
)
returns table(
  arena_grouped bigint,
  arena_extras_grouped bigint,
  promoted bigint,
  rows_already_in_gold bigint,
  dedup_kills bigint,
  bep_rows_migrated bigint,
  queue_processed bigint,
  extractions_enqueued bigint,
  poi_reactivated bigint,
  ran_at timestamptz
)
language plpgsql
security definer
as $function$
declare
  v_fids bigint[];
  v_chunk bigint[];
  v_chunk_size int := 50;
  v_chunk_offset int := 0;
  v_cluster_grp bigint := 0;
  v_extras_grp bigint := 0;
  v_promoted bigint := 0;
  v_existing bigint := 0;
  v_kills bigint := 0;
  v_bep_mig bigint := 0;
  v_queue bigint := 0;
  v_eq bigint := 0;
  v_poi bigint := 0;
  v_pf_rec record; v_pe_rec record; v_pg_rec record;
  v_dd_rec record; v_qq_rec record; v_eq_rec record; v_poi_rec record;
begin
  -- Phase 0: reactivate POI for this state
  select * into v_poi_rec from public.reactivate_poi_landing_for_state(p_state);
  v_poi := coalesce(v_poi_rec.reactivated_count, 0);

  perform public.promote_poi_landing_to_arena();

  select * into v_pf_rec from public.populate_arena_group_id();
  v_cluster_grp := coalesce(v_pf_rec.relation_grouped, 0)
                 + coalesce(v_pf_rec.name_clustered, 0)
                 + coalesce(v_pf_rec.poi_matched, 0);

  select * into v_pe_rec from public.populate_arena_extras();
  v_extras_grp := coalesce(v_pe_rec.intra_osm_trigram, 0)
                + coalesce(v_pe_rec.intra_poi, 0)
                + coalesce(v_pe_rec.cross_source_name, 0);

  if p_fids is not null and array_length(p_fids, 1) > 0 then
    v_fids := p_fids;
  else
    select array_agg(a.fid) into v_fids
      from public.arena a
     where a.is_active = true
       and not exists (select 1 from public.beaches_gold g where g.fid = a.fid)
       and a.county_fips is not null
       and public._infer_state_from_county_fips(a.county_fips) = p_state;
  end if;

  -- ★ Chunked promote — 50 fids at a time
  if v_fids is not null and array_length(v_fids, 1) > 0 then
    while v_chunk_offset < array_length(v_fids, 1) loop
      v_chunk := v_fids[v_chunk_offset + 1 : v_chunk_offset + v_chunk_size];
      select * into v_pg_rec from public.promote_to_gold(v_chunk, false, false);
      v_promoted := v_promoted + coalesce(v_pg_rec.rows_promoted, 0);
      v_existing := v_existing + coalesce(v_pg_rec.rows_already_in_gold, 0);
      v_chunk_offset := v_chunk_offset + v_chunk_size;
    end loop;
  end if;

  select * into v_dd_rec from public.run_late_stage_dedup();
  v_kills := coalesce(v_dd_rec.kills, 0);
  v_bep_mig := coalesce(v_dd_rec.bep_rows_migrated, 0);

  select * into v_qq_rec from public.process_geom_change_queue(100);
  v_queue := coalesce(v_qq_rec.fids_processed, 0);

  select * into v_eq_rec from public.enqueue_missing_extractions();
  v_eq := coalesce(v_eq_rec.queue_total, 0);

  return query select v_cluster_grp, v_extras_grp, v_promoted, v_existing,
                       v_kills, v_bep_mig, v_queue, v_eq, v_poi, now();
end;
$function$;

alter function public.run_pipeline_for_state(text, bigint[])
  set statement_timeout = '1800s';

commit;
