-- RPC: return CPAD polygons within p_meters of a beach point, with
-- distance + containment flag + GeoJSON for client-side rendering.
-- Used by admin/location-editor.html to overlay CPAD context on
-- the loaded beach's map view.
--
-- Sorted smallest-to-largest by distance so the most-specific
-- polygons render on top in Leaflet's z-stack.

create or replace function public.cpad_units_near_beach(
  p_fid    integer,
  p_meters integer default 300
)
returns table (
  unit_id      integer,
  unit_name    text,
  mng_agncy    text,
  mng_ag_lev   text,
  park_url     text,
  agncy_web    text,
  distance_m   integer,
  contains_pt  boolean,
  geom_geojson jsonb
)
language sql stable as $$
  with target as (
    select geom from public.locations_stage where fid = p_fid
  )
  select
    c.unit_id,
    c.unit_name,
    c.mng_agncy,
    c.mng_ag_lev,
    c.park_url,
    c.agncy_web,
    st_distance(c.geom::geography, t.geom::geography)::int            as distance_m,
    st_contains(c.geom, t.geom::geometry)                             as contains_pt,
    st_asgeojson(st_simplify(c.geom, 0.00005))::jsonb                 as geom_geojson
  from target t
  join public.cpad_units c
    on st_dwithin(c.geom::geography, t.geom::geography, p_meters)
  order by st_area(c.geom::geography) asc, distance_m asc;
$$;

grant execute on function public.cpad_units_near_beach(integer, integer) to anon, authenticated;
