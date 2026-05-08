-- 20260508_publish_stale_fids.sql
--
-- Wire Phase D (publish) into the orchestrator. Finds active gold fids
-- whose BEP has been updated more recently than their beach_dog_policy
-- was last curated, and re-runs consensus + promote_canonical for them.
--
-- Catches the lag between async extraction (Phase C drains in worker)
-- and consumer-surface publish (Phase D needs the new BEP). Eventually
-- consistent — extractions completed today get published tomorrow.

begin;

create or replace function public.publish_stale_fids(p_max_fids int default 200)
returns table(stale_count bigint, fids_published bigint)
language plpgsql
security definer
as $function$
declare
  v_stale bigint := 0;
  v_pub bigint := 0;
  v_fids bigint[];
begin
  -- Stale = active gold fid where any BEP row updated_at > last beach_dog_policy.curated_at
  -- (Or no beach_dog_policy row at all yet)
  with stale as (
    select g.fid
      from public.beaches_gold g
     where g.is_active
       and exists (
         select 1 from public.beach_enrichment_provenance bep
          where bep.gold_fid = g.fid
            and bep.field_group in ('dogs','practical','governance','access')
            and (bep.updated_at > coalesce(
                   (select bdp.curated_at from public.beach_dog_policy bdp
                     where bdp.arena_group_id = g.fid),
                   '2000-01-01'::timestamptz
                 ))
       )
  )
  select count(*) into v_stale from stale;

  select array_agg(fid) into v_fids
    from (select fid from (
      select g.fid
        from public.beaches_gold g
       where g.is_active
         and exists (
           select 1 from public.beach_enrichment_provenance bep
            where bep.gold_fid = g.fid
              and bep.field_group in ('dogs','practical','governance','access')
              and (bep.updated_at > coalesce(
                     (select bdp.curated_at from public.beach_dog_policy bdp
                       where bdp.arena_group_id = g.fid),
                     '2000-01-01'::timestamptz
                   ))
         )
       order by g.fid
       limit p_max_fids
    ) _) _;

  if v_fids is not null and array_length(v_fids, 1) > 0 then
    perform public.publish_canonical_for_fids(v_fids);
    v_pub := array_length(v_fids, 1);
  end if;

  return query select v_stale, v_pub;
end;
$function$;

grant execute on function public.publish_stale_fids(int) to anon, authenticated, service_role;

-- Add Phase D step to run_full_pipeline_maintenance
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
  extractions_dogs_enqueued bigint,
  extractions_park_url_enqueued bigint,
  extraction_queue_pending bigint,
  stale_fids_published bigint,
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
  v_dogs_eq bigint := 0;
  v_purl_eq bigint := 0;
  v_q_total bigint := 0;
  v_pub bigint := 0;
  v_pf_rec record;
  v_pi_rec record;
  v_po_rec record;
  v_nr_rec record;
  v_mb_rec record;
  v_eq_rec record;
  v_ps_rec record;
begin
  select * into v_pi_rec from public.promote_poi_landing_to_arena();
  v_poi_to_arena := coalesce(v_pi_rec.promoted, 0);

  select * into v_po_rec from public.promote_osm_landing_to_arena();
  v_osm_to_arena := coalesce(v_po_rec.promoted, 0);

  select * into v_nr_rec from public.refresh_arena_names_from_osm_landing();
  v_names_ref := coalesce(v_nr_rec.arena_rows_updated, 0);

  select * into v_mb_rec from public.refire_missing_bep(150);
  v_missing_bep := coalesce(v_mb_rec.fids_refired, 0);

  select * into v_pf_rec from public.run_pipeline_for_state(p_state);

  select * into v_eq_rec from public.enqueue_missing_extractions();
  v_dogs_eq := coalesce(v_eq_rec.dogs_enqueued, 0);
  v_purl_eq := coalesce(v_eq_rec.park_url_enqueued, 0);
  v_q_total := coalesce(v_eq_rec.queue_total, 0);

  -- ★ Phase D: publish any stale fids (BEP updated since last consumer-promote)
  select * into v_ps_rec from public.publish_stale_fids(200);
  v_pub := coalesce(v_ps_rec.fids_published, 0);

  return query select
    v_poi_to_arena, v_osm_to_arena, v_names_ref,
    coalesce(v_pf_rec.arena_grouped, 0),
    coalesce(v_pf_rec.arena_extras_grouped, 0),
    coalesce(v_pf_rec.promoted, 0),
    coalesce(v_pf_rec.dedup_kills, 0),
    coalesce(v_pf_rec.bep_rows_migrated, 0),
    coalesce(v_pf_rec.queue_processed, 0),
    v_missing_bep,
    v_dogs_eq, v_purl_eq, v_q_total,
    v_pub,
    now();
end;
$function$;

commit;
