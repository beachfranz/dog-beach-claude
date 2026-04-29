-- Passthrough for public.osm_features. Marts reference this via ref().

select * from {{ source('public', 'osm_features') }}
