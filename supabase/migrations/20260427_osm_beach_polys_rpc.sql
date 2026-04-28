-- Returns OSM beach polygons (geom_full) in a given CA county, ready
-- to render as a Leaflet GeoJSON layer. county_name_tiger is the
-- TIGER county filter; only beach + dog_friendly_beach features are
-- returned (parks/dog parks are skipped).

create or replace function public.osm_beach_polygons_in_county(p_county text)
returns table(
  osm_type     text,
  osm_id       bigint,
  name         text,
  feature_type text,
  geom_geojson jsonb
)
language sql stable security definer as $$
  select osm_type, osm_id, name, feature_type,
         st_asgeojson(geom_full)::jsonb as geom_geojson
  from public.osm_features
  where feature_type in ('beach','dog_friendly_beach')
    and geom_full is not null
    and county_name_tiger = p_county
$$;

grant execute on function public.osm_beach_polygons_in_county(text)
  to anon, authenticated;
