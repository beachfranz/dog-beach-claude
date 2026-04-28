-- RPC for radius-based map views: returns beach_locations + access
-- features within p_radius_m of (lat,lng), with one layer discriminator
-- column. Used by per-beach context maps.

create or replace function public.nearby_coastal_features(
  p_lat float8,
  p_lng float8,
  p_radius_m float8 default 3000
)
returns table (
  layer              text,    -- 'beach' | 'access'
  origin_key         text,
  name               text,
  feature_type       text,
  origin_source      text,
  operator_canonical text,
  dogs_verdict       text,
  description        text,
  lat                float8,
  lng                float8,
  geojson            jsonb
)
language sql stable security definer
as $$
  -- Beaches
  select 'beach'::text,
         b.origin_key,
         b.name,
         b.feature_type,
         b.origin_source,
         op.canonical_name,
         null::text                                  as dogs_verdict,
         null::text                                  as description,
         st_y(b.geom)                                as lat,
         st_x(b.geom)                                as lng,
         coalesce(
           st_asgeojson(coalesce(b.geom_full, b.geom))::jsonb,
           st_asgeojson(b.geom)::jsonb
         )                                           as geojson
  from public.beach_locations b
  left join public.operators op on op.id = b.operator_id
  where st_dwithin(b.geom::geography,
                   st_setsrid(st_makepoint(p_lng, p_lat), 4326)::geography,
                   p_radius_m)

  union all

  -- Access features
  select 'access'::text,
         a.origin_key,
         a.name,
         a.feature_type,
         a.origin_source,
         op.canonical_name,
         a.dogs_verdict,
         a.description,
         st_y(a.geom),
         st_x(a.geom),
         st_asgeojson(a.geom)::jsonb
  from public.beach_access_features a
  left join public.operators op on op.id = a.operator_id
  where st_dwithin(a.geom::geography,
                   st_setsrid(st_makepoint(p_lng, p_lat), 4326)::geography,
                   p_radius_m);
$$;

grant execute on function public.nearby_coastal_features(float8, float8, float8)
  to anon, authenticated;
