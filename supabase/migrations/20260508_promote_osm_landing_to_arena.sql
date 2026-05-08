-- 20260508_promote_osm_landing_to_arena.sql
--
-- Phase 2 of landing→arena codification: OSM source.
--
-- Filter: tags->>'natural' = 'beach' AND is_active. Same idempotency as
-- POI: only insert rows whose (type, id) pair isn't already in arena.
--
-- For nav coords: use ST_PointOnSurface for polygon types (way/relation),
-- direct lat/lon for nodes. ST_PointOnSurface guarantees the point is
-- inside the polygon (unlike centroid which can fall outside concave shapes).

begin;

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
    -- Latest fetch per (type, id)
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
             -- Polygons: point-on-surface (always inside the polygon)
             when l.type in ('way', 'relation') then ST_PointOnSurface(l.geom_full)
             -- Nodes: just the point
             else l.geom_full
           end as nav_geom
      from latest_osm l
     where not exists (
       select 1 from public.arena a
        where a.source_code = 'osm'
          and a.source_id = 'osm/' || l.type || '/' || l.id::text
     )
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
           c.county_geoid,
           c.county_name,
           'osm',
           'osm/' || c.type || '/' || c.id::text,
           c.cpad_unit_id,
           c.nav_geom,                    -- arena.geom is Point-typed
           true,
           ST_Y(c.nav_geom),
           ST_X(c.nav_geom),
           case when c.type in ('way', 'relation') then 'osm_point_on_surface'
                else 'osm_node' end
    from candidates c
    where c.resolved_name <> ''  -- skip un-named OSM beaches (low signal)
    returning fid
  )
  select count(*) into v_promoted from applied;

  -- New rows have group_id = NULL by default since we didn't pass it.
  -- Update them to be singletons (group_id = fid).
  update public.arena
     set group_id = fid
   where group_id is null;

  v_existing := v_candidates - v_promoted;

  perform set_config('app.arena_clustering_active', 'false', true);

  return query select v_promoted, v_existing, v_candidates;
end;
$function$;

grant execute on function public.promote_osm_landing_to_arena() to anon, authenticated, service_role;

commit;
