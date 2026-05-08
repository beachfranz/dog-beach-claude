-- 20260508_refire_missing_bep.sql
--
-- Preventive fix surfaced by the 50-row simulation: 214 statewide beaches
-- have ZERO BEP because they came in via the 2026-05-01 all_heads_v1 bulk
-- promote (which didn't run populators) and were never re-enriched.
--
-- Add refire_missing_bep() that finds gold rows missing governance BEP
-- and runs refire_bep_cascade on them. Wire into run_full_pipeline_
-- maintenance so the nightly cron heals any drift.
--
-- Idempotent. Safe to call repeatedly. Default cap of 100 fids/run keeps
-- under the Supabase statement timeout for any single invocation.

begin;

create or replace function public.refire_missing_bep(p_max_fids int default 100)
returns table(missing_count bigint, fids_refired bigint)
language plpgsql
security definer
as $function$
declare
  v_missing bigint := 0;
  v_refired bigint := 0;
  v_fids bigint[];
  v_rec record;
begin
  -- Total missing (for the report)
  select count(*) into v_missing
    from public.beaches_gold g
   where g.is_active
     and not exists (
       select 1 from public.beach_enrichment_provenance b
        where b.gold_fid = g.fid
          and b.field_group = 'governance'
     );

  -- Pick up to p_max_fids
  select array_agg(g.fid) into v_fids
    from (
      select g.fid
        from public.beaches_gold g
       where g.is_active
         and not exists (
           select 1 from public.beach_enrichment_provenance b
            where b.gold_fid = g.fid
              and b.field_group = 'governance'
         )
       order by g.fid
       limit p_max_fids
    ) g;

  if v_fids is not null and array_length(v_fids, 1) > 0 then
    select * into v_rec from public.refire_bep_cascade(v_fids);
    v_refired := coalesce(v_rec.fids_processed, 0);
  end if;

  return query select v_missing, v_refired;
end;
$function$;

grant execute on function public.refire_missing_bep(int) to anon, authenticated, service_role;

-- Wire into run_full_pipeline_maintenance: add Step 5 (refire missing BEP)
-- between Phase-3 name-refresh and the cluster/extras/promote/dedup chain
-- so any drift gets healed alongside other maintenance.
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
  bep_missing_refired bigint,
  ran_at timestamptz
)
language plpgsql
security definer
as $function$
declare
  v_poi_to_arena bigint := 0;
  v_osm_to_arena bigint := 0;
  v_names_ref    bigint := 0;
  v_missing_bep  bigint := 0;
  v_pf_rec record;
  v_pi_rec record;
  v_po_rec record;
  v_nr_rec record;
  v_mb_rec record;
begin
  select * into v_pi_rec from public.promote_poi_landing_to_arena();
  v_poi_to_arena := coalesce(v_pi_rec.promoted, 0);

  select * into v_po_rec from public.promote_osm_landing_to_arena();
  v_osm_to_arena := coalesce(v_po_rec.promoted, 0);

  select * into v_nr_rec from public.refresh_arena_names_from_osm_landing();
  v_names_ref := coalesce(v_nr_rec.arena_rows_updated, 0);

  -- ★ NEW: heal any gold rows that came in without BEP populators run.
  select * into v_mb_rec from public.refire_missing_bep(100);
  v_missing_bep := coalesce(v_mb_rec.fids_refired, 0);

  select * into v_pf_rec from public.run_pipeline_for_state(p_state);

  return query select
    v_poi_to_arena, v_osm_to_arena, v_names_ref,
    coalesce(v_pf_rec.arena_grouped, 0),
    coalesce(v_pf_rec.arena_extras_grouped, 0),
    coalesce(v_pf_rec.promoted, 0),
    coalesce(v_pf_rec.dedup_kills, 0),
    coalesce(v_pf_rec.bep_rows_migrated, 0),
    coalesce(v_pf_rec.queue_processed, 0),
    v_missing_bep,
    now();
end;
$function$;

commit;
