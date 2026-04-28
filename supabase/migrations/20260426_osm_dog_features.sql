-- Generalize osm_dog_parks → osm_dog_features. The table now also
-- holds dog-friendly parks (leisure=park with dog=yes/leashed/unleashed)
-- and dog-friendly beaches (natural=beach with dog=*), discriminated by
-- feature_type. Mirrors the polymorphic pattern in geo_entity_response.
--
-- feature_type values:
--   'dog_park'           — leisure=dog_park (dedicated off-leash zones)
--   'dog_friendly_park'  — leisure=park with explicit dog tag
--   'dog_friendly_beach' — natural=beach with explicit dog tag
--
-- dog_status values:
--   'unleashed' — off-leash explicitly allowed (or implied for dog_park)
--   'leashed'   — dogs allowed, leash required
--   'yes'       — dogs allowed, no leash detail given
--   null        — unknown / not tagged

alter table public.osm_dog_parks
  rename to osm_dog_features;

alter table public.osm_dog_features
  add column if not exists feature_type text,
  add column if not exists dog_status   text;

-- Backfill the existing 833 rows: every one is a dedicated dog park,
-- which is implicitly off-leash.
update public.osm_dog_features
   set feature_type = 'dog_park',
       dog_status   = coalesce(dog_status, 'unleashed')
 where feature_type is null;

alter table public.osm_dog_features
  alter column feature_type set not null;

alter table public.osm_dog_features
  drop constraint if exists osm_dog_features_feature_type_check;
alter table public.osm_dog_features
  add  constraint osm_dog_features_feature_type_check
    check (feature_type in ('dog_park','dog_friendly_park','dog_friendly_beach'));

alter table public.osm_dog_features
  drop constraint if exists osm_dog_features_dog_status_check;
alter table public.osm_dog_features
  add  constraint osm_dog_features_dog_status_check
    check (dog_status is null or dog_status in ('yes','leashed','unleashed'));

-- Rename indexes to match new table name.
alter index if exists osm_dog_parks_geom_gix rename to osm_dog_features_geom_gix;
alter index if exists osm_dog_parks_tags_gin rename to osm_dog_features_tags_gin;
alter index if exists osm_dog_parks_name_idx rename to osm_dog_features_name_idx;
alter index if exists osm_dog_parks_city_idx rename to osm_dog_features_city_idx;

create index if not exists osm_dog_features_type_idx
  on public.osm_dog_features (feature_type);
