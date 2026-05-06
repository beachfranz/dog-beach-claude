-- 20260507_osm_amenities.sql
--
-- OSM amenity nodes/ways near beaches. Loaded from Overpass API via
-- scripts/external_sources.py registry. Feeds a beach_amenities emitter
-- that votes has_parking / has_restrooms / has_showers / has_picnic_area
-- / has_drinking_water / has_food via spatial-near-beach intersect.

begin;

create table if not exists public.osm_amenities (
  osm_id        bigint  not null,        -- positive for nodes/ways/relations
  osm_kind      text    not null check (osm_kind in ('node','way','relation')),
  amenity_type  text    not null,        -- 'parking','toilets','shower',... (canonical taxonomy below)
  raw_amenity   text,                    -- original tag value (parking_space, fast_food, etc.)
  raw_tags      jsonb,
  name          text,
  state         text default 'CA',
  geom          geometry(Point, 4326),
  loaded_at     timestamptz default now(),
  primary key (osm_id, osm_kind)
);

create index if not exists osm_amenities_geom_idx
  on public.osm_amenities using gist (geom);
create index if not exists osm_amenities_type_idx
  on public.osm_amenities (amenity_type);
create index if not exists osm_amenities_state_idx
  on public.osm_amenities (state);

comment on table public.osm_amenities is
  'OSM amenity nodes/ways. amenity_type is our canonical taxonomy:
   parking | toilets | shower | drinking_water | picnic_area |
   food | lifeguard | fire_pit. raw_amenity preserves the source tag.';

commit;
