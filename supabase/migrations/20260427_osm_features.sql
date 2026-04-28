-- Generalize osm_dog_features → osm_features. The table now also
-- holds plain leisure=park features (dog policy unknown), making it the
-- single landing place for CA-scoped OSM ingest. New feature_type:
--
--   'park' — leisure=park with no explicit dog tag (most CA parks).
--            dog_status will be null for these rows.
--
-- Future expansions slot in by adding a new feature_type value + a new
-- Overpass query in the loader script.

alter table public.osm_dog_features rename to osm_features;

alter index if exists osm_dog_features_geom_gix rename to osm_features_geom_gix;
alter index if exists osm_dog_features_tags_gin rename to osm_features_tags_gin;
alter index if exists osm_dog_features_name_idx rename to osm_features_name_idx;
alter index if exists osm_dog_features_city_idx rename to osm_features_city_idx;
alter index if exists osm_dog_features_type_idx rename to osm_features_type_idx;

alter table public.osm_features
  drop constraint if exists osm_dog_features_feature_type_check;
alter table public.osm_features
  drop constraint if exists osm_features_feature_type_check;
alter table public.osm_features
  add  constraint osm_features_feature_type_check
    check (feature_type in ('dog_park','dog_friendly_park','dog_friendly_beach','park'));

-- Same for the dog_status check (renamed implicitly with the table).
alter table public.osm_features
  drop constraint if exists osm_dog_features_dog_status_check;
alter table public.osm_features
  drop constraint if exists osm_features_dog_status_check;
alter table public.osm_features
  add  constraint osm_features_dog_status_check
    check (dog_status is null or dog_status in ('yes','leashed','unleashed'));
