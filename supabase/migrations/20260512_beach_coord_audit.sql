-- Phase 1 of beach coord validation: compare each beach centroid to known
-- OSM coastline + OSM natural=beach polygons, surface a quality flag.
--
-- Catches single-source ingest errors that dedup can't see. See
-- project_beach_coord_validation.md.
--
-- Quality bands:
--   "in_beach_polygon"   centroid is INSIDE an OSM natural=beach polygon → very strong positive
--   "near_coast"         centroid is within 200m of OSM coastline → strong positive
--   "kind_of_close"      201-1000m from coastline → plausible (long beach centroid)
--   "inland_ish"         1001-5000m from coastline → suspicious, likely wrong
--   "way_off"            >5km from coastline → almost certainly wrong
--   "no_coast_reference" no OSM coastline within 10km — lake/inland beach maybe
--
-- Caveats:
--   - Long beaches: centroid can be 100s of meters inland of the coastline
--     because polygon centroids drift toward the center of mass. We accept
--     up to 1000m as "kind_of_close" instead of immediately flagging.
--   - Lake beaches (Lake Union, Lake Washington): not on OSM coastline.
--     Get "no_coast_reference" — this is informational, not necessarily bad.
--   - Pier/jetty/cape-tip beaches: legitimately offshore. Polygon
--     containment is the right test for them.

create or replace function public.beach_coord_audit(p_state text default null)
returns table(
  fid bigint,
  name text,
  state text,
  county_name text,
  lat double precision,
  lon double precision,
  in_beach_polygon boolean,
  coast_distance_m int,
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
  coast_dist as (
    select t.fid,
           (select round(st_distance(pt.gg, ol.geom_full::geography))::int
              from public.osm_landing ol
             where ol.is_active
               and ol.tags->>'natural' = 'coastline'
               and ol.geom_full is not null
             order by ol.geom_full::geography <-> pt.gg
             limit 1) as dist_m
      from target t
      join pt using (fid)
  )
  select t.fid, t.name, t.state, t.county_name, t.lat, t.lon,
         ip.inside as in_beach_polygon,
         cd.dist_m as coast_distance_m,
         case
           when ip.inside then 'in_beach_polygon'
           when cd.dist_m is null then 'no_coast_reference'
           when cd.dist_m <= 200 then 'near_coast'
           when cd.dist_m <= 1000 then 'kind_of_close'
           when cd.dist_m <= 5000 then 'inland_ish'
           else 'way_off'
         end as quality
    from target t
    join in_poly ip using (fid)
    join coast_dist cd using (fid)
   order by t.state, t.county_name, t.name;
$$;

comment on function public.beach_coord_audit(text) is
  'Phase 1 coord validation: per-beach quality flag from OSM coastline
   distance + beach-polygon containment. Pass a state code to restrict.';
