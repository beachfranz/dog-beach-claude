-- Snap-to-polygon prep:
--
-- 1. osm_features.geom_full holds the actual polygon geometry for OSM
--    beach features (refetched separately via Overpass `out geom`).
--    Stays null for park/dog_park feature_types — we don't need
--    polygons for those.
-- 2. geom_original on all three sources preserves the source's original
--    point before any snap operation. Lets us reverse the snap and
--    re-do it if logic changes, without re-fetching.

alter table public.osm_features
  add column if not exists geom_full geometry(Geometry, 4326),
  add column if not exists geom_original geometry(Point, 4326);

create index if not exists osm_features_geom_full_gix
  on public.osm_features using gist (geom_full);

alter table public.ccc_access_points
  add column if not exists geom_original geometry(Point, 4326);

alter table public.us_beach_points
  add column if not exists geom_original geometry(Point, 4326);
