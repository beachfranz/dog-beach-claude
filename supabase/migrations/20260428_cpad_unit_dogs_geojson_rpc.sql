-- RPC: returns one row per CPAD unit (within p_counties) that has a
-- cpad_unit_dogs_policy row, with all policy fields + simplified
-- polygon as GeoJSON. Powers admin/cpad-unit-policy-map.html.

create or replace function public.cpad_unit_dogs_geojson(
  p_counties text[]
) returns table (
  cpad_unit_id          integer,
  unit_name             text,
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
  extraction_model      text,
  extraction_confidence numeric,
  geom_json             jsonb
) language sql stable security definer as $$
  -- cpad_unit_dogs_policy is pre-filtered to the LA-OC-SD universe
  -- (only those units have policy rows). We just join cpad_units for
  -- geom + layer + county tag, and narrow by county column directly —
  -- avoids the runtime spatial join that was timing out.
  select
    p.cpad_unit_id, p.unit_name, p.agency_name, cu.layer,
    p.dogs_allowed, p.default_rule, p.leash_required,
    p.area_sand, p.area_water, p.area_picnic_area,
    p.area_parking_lot, p.area_trails, p.area_campground,
    p.designated_dog_zones, p.prohibited_areas,
    p.source_quote, p.url_used, p.extraction_model, p.extraction_confidence,
    st_asgeojson(st_simplify(cu.geom, 0.0005))::jsonb as geom_json
  from public.cpad_unit_dogs_policy p
  join public.cpad_units cu on cu.unit_id = p.cpad_unit_id
  where cu.county = any(p_counties);
$$;

grant execute on function public.cpad_unit_dogs_geojson(text[]) to anon, authenticated;
