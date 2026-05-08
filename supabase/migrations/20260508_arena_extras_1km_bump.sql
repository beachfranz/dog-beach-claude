-- 20260508_arena_extras_1km_bump.sql
--
-- Option A from tonight's audit: bump Strategy 7 (intra-POI) and Strategy 8
-- (cross-source POI -> OSM) distance gates from 500m to 1km.
--
-- Catches San Simeon Creek Beach + San Simeon Beach (583m apart in arena).
-- Sim 0.73 trigram passes; was missed only because of distance threshold.
--
-- 1km is still tight enough that a same-county trigram-≥0.7 pair is
-- virtually always the same beach. The dog-mismatch guard in
-- _dedup_pair_safe still blocks the X / X Dog Beach trap.

begin;

drop function if exists public.populate_arena_extras();

create function public.populate_arena_extras()
returns table(intra_osm_trigram bigint, intra_poi bigint, cross_source_name bigint)
language plpgsql
security definer
as $function$
declare
  v_intra_osm bigint := 0;
  v_intra_poi bigint := 0;
  v_cross bigint := 0;
begin
  perform set_config('app.arena_clustering_active', 'true', true);

  -- STRATEGY 6: Trigram intra-OSM (5km, sim ≥ 0.7)
  with eligible as (
    select fid, lower(trim(name)) as name_lc, name, county_fips, geom
      from public.arena
     where source_code = 'osm' and is_active = true
       and name is not null and trim(name) <> ''
       and county_fips is not null and geom is not null
       and group_id = fid
  ),
  pairs as (
    select b.fid as loser, a.fid as winner
      from eligible a
      join eligible b
        on a.county_fips = b.county_fips and a.fid < b.fid
       and st_dwithin(a.geom::geography, b.geom::geography, 5000)
       and public._dedup_pair_safe(a.name, b.name, 0.7)
       and a.name_lc <> b.name_lc
  ),
  pick as (select distinct on (loser) loser, winner from pairs order by loser, winner),
  applied as (
    update public.arena a set group_id = p.winner
      from pick p
     where a.fid = p.loser and a.group_id = a.fid
    returning 1
  )
  select count(*) into v_intra_osm from applied;

  -- STRATEGY 7: Intra-POI (1km, sim ≥ 0.7) -- BUMPED from 500m
  with eligible as (
    select fid, lower(trim(name)) as name_lc, name, county_fips, geom
      from public.arena
     where source_code = 'poi' and is_active = true
       and name is not null and trim(name) <> ''
       and county_fips is not null and geom is not null
       and group_id = fid
  ),
  pairs as (
    select b.fid as loser, a.fid as winner
      from eligible a
      join eligible b
        on a.county_fips = b.county_fips and a.fid < b.fid
       and st_dwithin(a.geom::geography, b.geom::geography, 1000)  -- 500 -> 1000
       and public._dedup_pair_safe(a.name, b.name, 0.7)
  ),
  pick as (select distinct on (loser) loser, winner from pairs order by loser, winner),
  applied as (
    update public.arena a set group_id = p.winner
      from pick p
     where a.fid = p.loser and a.group_id = a.fid
    returning 1
  )
  select count(*) into v_intra_poi from applied;

  -- STRATEGY 8: Cross-source POI -> OSM by name (1km, sim ≥ 0.7) -- BUMPED
  with poi_singletons as (
    select fid, lower(trim(name)) as name_lc, name, county_fips, geom
      from public.arena
     where source_code = 'poi' and is_active = true and group_id = fid
       and name is not null and trim(name) <> ''
       and county_fips is not null and geom is not null
  ),
  osm_targets as (
    select fid, group_id, lower(trim(name)) as name_lc, name, county_fips, geom
      from public.arena
     where source_code = 'osm' and is_active = true
       and name is not null and trim(name) <> ''
       and county_fips is not null and geom is not null
  ),
  candidates as (
    select p.fid as poi_fid, o.group_id as target_group,
           similarity(p.name_lc, o.name_lc) as sim,
           st_distance(p.geom::geography, o.geom::geography) as meters
      from poi_singletons p
      join osm_targets o
        on o.county_fips = p.county_fips
       and st_dwithin(p.geom::geography, o.geom::geography, 1000)  -- 500 -> 1000
       and public._dedup_pair_safe(p.name, o.name, 0.7)
  ),
  pick as (
    select distinct on (poi_fid) poi_fid, target_group
      from candidates
     order by poi_fid, sim desc, meters asc
  ),
  applied as (
    update public.arena a set group_id = p.target_group
      from pick p
     where a.fid = p.poi_fid and a.group_id = a.fid
    returning 1
  )
  select count(*) into v_cross from applied;

  perform set_config('app.arena_clustering_active', 'false', true);

  return query select v_intra_osm, v_intra_poi, v_cross;
end;
$function$;

grant execute on function public.populate_arena_extras() to anon, authenticated, service_role;

commit;
