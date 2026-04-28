-- RPC: return every CPAD polygon within p_meters of ANY active
-- locations_stage beach. Used by admin/location-editor.html to render
-- a global CPAD context layer (always-on, not per-beach).
--
-- Polygons simplified for transport — 0.0003 degrees (~33m at our
-- latitudes). Cuts ~5MB raw to ~1.6MB while keeping shape recognisable
-- at zoom 10+. Smallest-area-first so the most-specific polygons
-- render on top in Leaflet's z-stack.

create or replace function public.cpad_units_near_active_beaches(
  p_meters integer default 200
)
returns table (
  unit_id      integer,
  unit_name    text,
  mng_agncy    text,
  mng_ag_lev   text,
  park_url     text,
  agncy_web    text,
  geom_geojson jsonb
)
language sql stable as $$
  with relevant as (
    select distinct c.unit_id
    from public.locations_stage s
    join public.cpad_units c
      on st_dwithin(c.geom::geography, s.geom::geography, p_meters)
    where s.is_active = true
  )
  select
    c.unit_id, c.unit_name, c.mng_agncy, c.mng_ag_lev,
    c.park_url, c.agncy_web,
    st_asgeojson(st_simplify(c.geom, 0.0003))::jsonb as geom_geojson
  from relevant r
  join public.cpad_units c on c.unit_id = r.unit_id
  order by st_area(c.geom::geography) desc;  -- big first; small drawn last → on top
$$;

grant execute on function public.cpad_units_near_active_beaches(integer) to anon, authenticated;
