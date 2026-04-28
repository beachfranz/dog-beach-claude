-- OpenStreetMap leisure=dog_park elements scoped to California.
-- Loaded from the Overpass API by scripts/one_off/fetch_osm_dog_parks_ca.py.
-- ~833 elements as of 2026-04-26: 755 way (polygon), 63 node (point),
-- 15 relation (multipolygon).
--
-- Primary key is (osm_type, osm_id) because OSM ids are only unique
-- within their element type (a node and a way can share id 1234).
--
-- Geometry stored as a single Point (the element center) — Overpass
-- "out center" gives us that for ways/relations without inflating the
-- payload with full polygons. If we later need exact polygons (e.g.,
-- "is this beach inside a dog park?"), we'll add geom_full as a
-- separate geometry(Geometry, 4326) column.

create table if not exists public.osm_dog_parks (
  osm_type      text   not null check (osm_type in ('node','way','relation')),
  osm_id        bigint not null,

  name          text,                          -- tags->>'name', null if unnamed
  latitude      double precision,              -- center lat (EPSG:4326)
  longitude     double precision,              -- center lng (EPSG:4326)
  geom          geometry(Point, 4326),         -- center as PostGIS point

  tags          jsonb,                         -- complete OSM tag dict

  -- Convenience extracts (denormalized from tags for query perf and
  -- readability). Re-derived on every upsert from the source tags.
  fee           text,
  fence         text,
  surface       text,
  opening_hours text,
  website       text,
  city          text,

  loaded_at     timestamptz default now(),

  primary key (osm_type, osm_id)
);

create index if not exists osm_dog_parks_geom_gix on public.osm_dog_parks using gist (geom);
create index if not exists osm_dog_parks_tags_gin on public.osm_dog_parks using gin  (tags);
create index if not exists osm_dog_parks_name_idx on public.osm_dog_parks (name);
create index if not exists osm_dog_parks_city_idx on public.osm_dog_parks (city);
