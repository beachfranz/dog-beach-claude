-- 20260508_state_launch_pipeline_polish.sql
--
-- Three fixes surfaced by the OR launch validation:
--
-- (1) promote_osm_landing_to_arena: spatial-backfill county_fips/county_name
--     from the counties table when osm_landing tags don't carry them.
--     Overpass nodes have lat/lon but no county tag, so prior 5 OR seeds
--     and tonight's 20 new rows all landed with null county_fips, which
--     made run_pipeline_for_state skip them.
--
-- (2) run_pipeline_for_state: call enqueue_missing_extractions() at the
--     end so per-state launches actually queue extractions for the
--     extraction queue worker to drain. Previously only
--     run_full_pipeline_maintenance did this.
--
-- (3) Backfill the existing OR seeds + new rows with the spatial county
--     join so the pipeline can re-process them on the next pass.

begin;

-- (1) New promote_osm_landing_to_arena: COALESCE county info from spatial
--     join with counties when tags-derived county_geoid is null.
create or replace function public.promote_osm_landing_to_arena()
returns table(promoted bigint, already_in_arena bigint, candidates bigint)
language plpgsql
security definer
as $function$
declare
  v_promoted bigint := 0;
  v_existing bigint := 0;
  v_candidates bigint := 0;
begin
  perform set_config('app.arena_clustering_active', 'true', true);

  select count(*) into v_candidates
    from (
      select distinct on (type, id) type, id
        from public.osm_landing ol
       where ol.tags->>'natural' = 'beach'
         and ol.is_active is true
         and ol.geom_full is not null
       order by type, id, fetched_at desc
    ) _;

  with latest_osm as (
    select distinct on (type, id)
           type, id, name, tags, geom_full, geometry,
           lat, lon, cpad_unit_id, county_geoid, county_name
      from public.osm_landing
     where tags->>'natural' = 'beach'
       and is_active is true
       and geom_full is not null
     order by type, id, fetched_at desc
  ),
  candidates as (
    select l.*,
           coalesce(l.name, l.tags->>'name', '') as resolved_name,
           case
             when l.type in ('way', 'relation') then ST_PointOnSurface(l.geom_full)
             else l.geom_full
           end as nav_geom
      from latest_osm l
     where not exists (
       select 1 from public.arena a
        where a.source_code = 'osm'
          and a.source_id = 'osm/' || l.type || '/' || l.id::text
     )
  ),
  candidates_with_county as (
    -- Spatial-backfill county from counties table when tags don't carry it.
    -- LEFT LATERAL so unmatched (e.g. offshore) rows still flow through.
    select c.*,
           coalesce(c.county_geoid, cc.geoid) as eff_county_geoid,
           coalesce(c.county_name,  cc.name)  as eff_county_name
      from candidates c
      left join lateral (
        select co.geoid, co.name
          from public.counties co
         where st_contains(co.geom, c.nav_geom)
         limit 1
      ) cc on true
  ),
  applied as (
    insert into public.arena (
      name, lat, lon, county_fips, county_name,
      source_code, source_id, cpad_unit_id, geom,
      is_active, nav_lat, nav_lon, nav_source
    )
    select c.resolved_name,
           ST_Y(c.nav_geom),
           ST_X(c.nav_geom),
           c.eff_county_geoid,
           c.eff_county_name,
           'osm',
           'osm/' || c.type || '/' || c.id::text,
           c.cpad_unit_id,
           c.nav_geom,
           true,
           ST_Y(c.nav_geom),
           ST_X(c.nav_geom),
           case when c.type in ('way', 'relation') then 'osm_point_on_surface'
                else 'osm_node' end
      from candidates_with_county c
     where c.resolved_name <> ''
     returning fid
  )
  select count(*) into v_promoted from applied;

  update public.arena
     set group_id = fid
   where group_id is null;

  v_existing := v_candidates - v_promoted;

  perform set_config('app.arena_clustering_active', 'false', true);

  return query select v_promoted, v_existing, v_candidates;
end;
$function$;

-- (2) run_pipeline_for_state: add enqueue_missing_extractions step + return count
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
  ran_at timestamptz
)
language plpgsql
security definer
as $function$
declare
  v_fids bigint[];
  v_cluster_grp bigint := 0;
  v_extras_grp bigint := 0;
  v_promoted bigint := 0;
  v_existing bigint := 0;
  v_kills bigint := 0;
  v_bep_mig bigint := 0;
  v_queue bigint := 0;
  v_eq bigint := 0;
  v_pf_rec record; v_pe_rec record; v_pg_rec record;
  v_dd_rec record; v_qq_rec record; v_eq_rec record;
begin
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
       and exists (
         select 1 from public.counties c
          where c.geoid = a.county_fips
            and c.state_fp = case p_state
                               when 'CA' then '06'
                               when 'OR' then '41'
                               when 'WA' then '53'
                             end
       );
  end if;

  if v_fids is not null and array_length(v_fids, 1) > 0 then
    select * into v_pg_rec from public.promote_to_gold(v_fids, false);
    v_promoted := coalesce(v_pg_rec.rows_promoted, 0);
    v_existing := coalesce(v_pg_rec.rows_already_in_gold, 0);
  end if;

  select * into v_dd_rec from public.run_late_stage_dedup();
  v_kills := coalesce(v_dd_rec.kills, 0);
  v_bep_mig := coalesce(v_dd_rec.bep_rows_migrated, 0);

  select * into v_qq_rec from public.process_geom_change_queue(100);
  v_queue := coalesce(v_qq_rec.fids_processed, 0);

  -- ★ Enqueue extractions for any newly-promoted (or still-missing) gold rows
  select * into v_eq_rec from public.enqueue_missing_extractions();
  v_eq := coalesce(v_eq_rec.queue_total, 0);

  return query select
    v_cluster_grp, v_extras_grp,
    v_promoted, v_existing,
    v_kills, v_bep_mig,
    v_queue, v_eq, now();
end;
$function$;

-- (3) Backfill counties on existing arena rows that have null county_fips
--     but have a geom that can be spatially joined.
update public.arena a
   set county_fips = c.geoid,
       county_name = coalesce(a.county_name, c.name)
  from public.counties c
 where a.county_fips is null
   and a.geom is not null
   and a.is_active = true
   and st_contains(c.geom, a.geom);

commit;
