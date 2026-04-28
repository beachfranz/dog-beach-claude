-- Coastal-jurisdictions matview, parallel to cpad_units_coastal.
-- TIGER place polygons within 200m of any active beach, simplified for
-- transport. ~129 polygons, ~290KB GeoJSON.

create materialized view if not exists public.jurisdictions_coastal as
  with relevant as (
    select distinct j.id
    from public.locations_stage s
    join public.jurisdictions j
      on st_dwithin(j.geom::geography, s.geom::geography, 200)
    where s.is_active = true
  )
  select
    j.id, j.name, j.place_type, j.county, j.state,
    j.fips_state, j.fips_place,
    st_asgeojson(st_simplify(j.geom, 0.0003))::jsonb as geom_geojson
  from relevant r
  join public.jurisdictions j on j.id = r.id;

create unique index if not exists jurisdictions_coastal_pkey
  on public.jurisdictions_coastal (id);

grant select on public.jurisdictions_coastal to anon, authenticated;

create or replace function public.jurisdictions_near_active_beaches()
returns table (
  id           integer,
  name         text,
  place_type   text,
  county       text,
  state        text,
  fips_state   text,
  fips_place   text,
  geom_geojson jsonb
)
language sql stable security definer as $$
  select id, name, place_type, county, state, fips_state, fips_place, geom_geojson
  from public.jurisdictions_coastal;
$$;

grant execute on function public.jurisdictions_near_active_beaches() to anon, authenticated;
