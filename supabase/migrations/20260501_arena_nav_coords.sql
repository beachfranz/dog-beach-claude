-- arena.nav_lat / nav_lon — navigation point per row.
--
--   POI rows:           same as (lat, lon) — original geocode/CSV coord
--   OSM polygon rows:   ST_PointOnSurface(geom_full from osm_landing)
--                       — guaranteed inside the polygon, fixes the
--                         offshore-centroid problem on crescent beaches
--
-- nav_source records which path populated the coords, for debugging.
--
-- Address backfill happens separately in a Python script that calls
-- Google Maps reverse-geocode against (nav_lat, nav_lon) — only for
-- OSM rows whose place_type starts with 'C' (incorporated city).
-- Plus-code-only results are dropped.

alter table public.arena
  add column if not exists nav_lat    double precision,
  add column if not exists nav_lon    double precision,
  add column if not exists nav_source text;


create or replace function public.populate_arena_nav_coords()
returns table (poi_set bigint, osm_set bigint, missing bigint)
language plpgsql
security definer
as $function$
declare
  v_poi bigint := 0;
  v_osm bigint := 0;
  v_miss bigint := 0;
begin
  -- POI: copy existing lat/lon
  with applied as (
    update public.arena
       set nav_lat = lat,
           nav_lon = lon,
           nav_source = 'poi_geom'
     where source_code = 'poi'
       and lat is not null and lon is not null
    returning 1
  )
  select count(*) into v_poi from applied;

  -- OSM: ST_PointOnSurface from latest osm_landing.geom_full per (type, id)
  with latest as (
    select distinct on (type, id) type, id, geom_full
      from public.osm_landing
     where geom_full is not null
     order by type, id, fetched_at desc
  ),
  applied as (
    update public.arena a
       set nav_lat = ST_Y(ST_PointOnSurface(l.geom_full)),
           nav_lon = ST_X(ST_PointOnSurface(l.geom_full)),
           nav_source = 'osm_point_on_surface'
      from latest l
     where a.source_code = 'osm'
       and a.source_id = 'osm/' || l.type || '/' || l.id::text
    returning 1
  )
  select count(*) into v_osm from applied;

  select count(*) into v_miss
    from public.arena where nav_lat is null;

  return query select v_poi, v_osm, v_miss;
end;
$function$;
