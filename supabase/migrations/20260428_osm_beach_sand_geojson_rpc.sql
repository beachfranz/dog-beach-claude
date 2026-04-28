-- Returns OSM beach + sand polygons for a given county as simplified
-- GeoJSON. Powers admin/oc-beach-sand-polygons.html.

create or replace function public.osm_beach_sand_geojson(
  p_county text default 'Orange'
) returns table (
  osm_type        text,
  osm_id          bigint,
  name            text,
  feature_type    text,
  operator_id     bigint,
  operator_name   text,
  geom_json       jsonb
) language sql stable security definer as $$
  select o.osm_type, o.osm_id, o.name, o.feature_type,
         o.operator_id, op.canonical_name,
         st_asgeojson(st_simplifypreservetopology(o.geom_full, 0.0001))::jsonb
    from public.osm_features o
    left join public.operators op on op.id = o.operator_id
   where o.feature_type in ('beach','sand','dog_friendly_beach')
     and o.geom_full is not null
     and o.county_name_tiger = p_county
     and (o.admin_inactive is null or o.admin_inactive = false);
$$;

grant execute on function public.osm_beach_sand_geojson(text) to anon, authenticated;
