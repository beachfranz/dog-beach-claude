-- Phase 1 v2 of beach coord validation: rebuild on beach-polygon distance.
--
-- v1 (earlier today) used OSM natural=coastline distance, but our osm_landing
-- only has 146 coastline rows (load_state.py filters to beach/dog/landscape
-- features — coastline never specifically ingested). The "way_off" rate was
-- 34-67% per state which is meaningless without real coastline data.
--
-- v2: skip coastline; lean on the 20,339 beach polygons + 1,691 dog parks
-- we DO have. Quality bands:
--
--   "in_beach_polygon"   centroid inside OSM natural=beach polygon → confirmed
--   "near_beach_polygon" within 200m of nearest OSM beach polygon  → plausible
--   "kind_of_close"      201-1000m from nearest beach polygon       → possibly long beach
--   "no_beach_polygon"   >1km from any OSM beach polygon            → suspicious
--   "very_far"           >5km from any OSM beach polygon            → likely wrong

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
              where ol.is_active
                and ol.tags->>'natural' = 'beach'
                and ol.geom_full is not null
                and st_geometrytype(ol.geom_full) in ('ST_Polygon','ST_MultiPolygon')
                and st_contains(ol.geom_full, pt.g)
           ) as inside
      from target t
      join pt using (fid)
  ),
  nearest as (
    select t.fid,
           (select round(st_distance(pt.gg, ol.geom_full::geography))::int
              from public.osm_landing ol
             where ol.is_active
               and ol.tags->>'natural' = 'beach'
               and ol.geom_full is not null
             order by ol.geom_full::geography <-> pt.gg
             limit 1) as dist_m
      from target t
      join pt using (fid)
  )
  select t.fid, t.name, t.state, t.county_name, t.lat, t.lon,
         ip.inside as in_beach_polygon,
         nr.dist_m as nearest_beach_polygon_m,
         case
           when ip.inside then 'in_beach_polygon'
           when nr.dist_m is null then 'no_beach_polygon'
           when nr.dist_m <= 200 then 'near_beach_polygon'
           when nr.dist_m <= 1000 then 'kind_of_close'
           when nr.dist_m <= 5000 then 'no_beach_polygon'
           else 'very_far'
         end as quality
    from target t
    join in_poly ip using (fid)
    join nearest nr using (fid)
   order by t.state, t.county_name, t.name;
$$;
