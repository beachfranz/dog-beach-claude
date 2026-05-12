-- v3 of beach coord audit: add waterway-proximity check to triage "far from
-- beach polygon" into "near inland water (likely lake/river beach)" vs
-- "no water reference at all (likely coord error)".
--
-- osm_landing has 240K+ waterway=stream and 21K waterway=river polylines,
-- but only ~39 natural=water polygons. Streams/rivers serve as a proxy for
-- "this beach sits near inland water" since lakes typically have rivers
-- flowing in/out.

create or replace function public.beach_coord_audit(p_state text default null)
returns table(
  fid bigint,
  name text,
  state text,
  county_name text,
  lat double precision,
  lon double precision,
  in_beach_polygon boolean,
  nearest_beach_polygon_m int,
  nearest_waterway_m int,
  quality text
)
language sql
stable
security definer
set search_path to 'public'
as $$
  with target as (
    select g.fid, coalesce(g.display_name_override, g.name) as name,
           g.state, g.county_name, g.lat, g.lon
      from public.beaches_gold g
     where g.is_active and g.is_scoreable and g.lat is not null
       and (p_state is null or g.state = p_state)
  ),
  pt as (
    select t.fid,
           st_setsrid(st_makepoint(t.lon, t.lat), 4326) as g,
           st_setsrid(st_makepoint(t.lon, t.lat), 4326)::geography as gg
      from target t
  ),
  in_poly as (
    select t.fid,
           exists(
             select 1 from public.osm_landing ol
              where ol.is_active and ol.tags->>'natural' = 'beach'
                and ol.geom_full is not null
                and st_geometrytype(ol.geom_full) in ('ST_Polygon','ST_MultiPolygon')
                and st_contains(ol.geom_full, pt.g)
           ) as inside
      from target t join pt using (fid)
  ),
  nearest_beach as (
    select t.fid,
           (select round(st_distance(pt.gg, ol.geom_full::geography))::int
              from public.osm_landing ol
             where ol.is_active and ol.tags->>'natural' = 'beach'
               and ol.geom_full is not null
             order by ol.geom_full::geography <-> pt.gg
             limit 1) as dist_m
      from target t join pt using (fid)
  ),
  nearest_water as (
    select t.fid,
           (select round(st_distance(pt.gg, ol.geom_full::geography))::int
              from public.osm_landing ol
             where ol.is_active
               and (ol.tags->>'waterway' in ('river','stream','canal')
                    or ol.tags->>'natural' in ('water','bay')
                    or (ol.tags ? 'water' and ol.tags->>'water' != 'no'))
               and ol.geom_full is not null
             order by ol.geom_full::geography <-> pt.gg
             limit 1) as dist_m
      from target t join pt using (fid)
  )
  select t.fid, t.name, t.state, t.county_name, t.lat, t.lon,
         ip.inside as in_beach_polygon,
         nb.dist_m as nearest_beach_polygon_m,
         nw.dist_m as nearest_waterway_m,
         case
           when ip.inside then 'in_beach_polygon'
           when nb.dist_m is not null and nb.dist_m <= 200 then 'near_beach_polygon'
           when nb.dist_m is not null and nb.dist_m <= 1000 then 'kind_of_close'
           when nw.dist_m is not null and nw.dist_m <= 200 then 'lake_or_river_beach'
           when nb.dist_m is not null and nb.dist_m <= 5000 then 'no_beach_polygon'
           else 'very_far'
         end as quality
    from target t
    join in_poly ip using (fid)
    join nearest_beach nb using (fid)
    join nearest_water nw using (fid)
   order by t.state, t.county_name, t.name;
$$;
