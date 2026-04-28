-- RPC: one row per beach_locations entry inside p_counties, joined
-- to the policy of its smallest containing CPAD unit (if any).
-- Powers a beach-level overlay of the CPAD-unit dog-policy data.

create or replace function public.beach_level_dogs_geojson(
  p_counties text[]
) returns table (
  origin_key            text,
  beach_name            text,
  county                text,
  cpad_unit_id          integer,
  cpad_unit_name        text,
  agency_name           text,
  layer                 text,
  dogs_allowed          text,
  default_rule          text,
  leash_required        boolean,
  area_sand             text,
  area_water            text,
  area_picnic_area      text,
  area_parking_lot      text,
  area_trails           text,
  area_campground       text,
  designated_dog_zones  text,
  prohibited_areas      text,
  source_quote          text,
  url_used              text,
  lat                   float8,
  lng                   float8
) language sql stable security definer as $$
  with bl_in_counties as (
    select bl.origin_key, bl.name, c.name as cnty, bl.geom
      from public.beach_locations bl
      join public.counties c on st_intersects(c.geom, bl.geom)
     where c.name = any(p_counties)
  )
  select
    bl.origin_key,
    bl.name as beach_name,
    bl.cnty,
    p.cpad_unit_id,
    p.unit_name as cpad_unit_name,
    p.agency_name,
    cu.layer,
    p.dogs_allowed,
    p.default_rule,
    p.leash_required,
    p.area_sand, p.area_water, p.area_picnic_area,
    p.area_parking_lot, p.area_trails, p.area_campground,
    p.designated_dog_zones, p.prohibited_areas,
    p.source_quote, p.url_used,
    st_y(bl.geom)::float8 as lat,
    st_x(bl.geom)::float8 as lng
  from bl_in_counties bl
  left join lateral (
    select cu2.unit_id, cu2.layer
      from public.cpad_units cu2
     where st_contains(cu2.geom, bl.geom)
     order by st_area(cu2.geom) asc
     limit 1
  ) cu on true
  left join public.cpad_unit_dogs_policy p on p.cpad_unit_id = cu.unit_id;
$$;

grant execute on function public.beach_level_dogs_geojson(text[]) to anon, authenticated;
