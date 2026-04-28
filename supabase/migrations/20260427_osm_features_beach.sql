-- Add 'beach' to the osm_features.feature_type check constraint so we
-- can land all CA natural=beach features (1,533 total) alongside the
-- 7 dog-tagged ones already in 'dog_friendly_beach'. Loader's dedupe
-- logic protects the more-specific classification.

alter table public.osm_features
  drop constraint if exists osm_features_feature_type_check;
alter table public.osm_features
  add  constraint osm_features_feature_type_check
    check (feature_type in
      ('dog_park','dog_friendly_park','dog_friendly_beach','park','beach'));
