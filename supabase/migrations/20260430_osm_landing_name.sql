-- Promote tags->>'name' to a top-level generated column on osm_landing.
--
-- OSM has many name variants (name, name:en, alt_name, loc_name,
-- short_name, official_name, int_name). Picking 'name' as the
-- canonical means: when downstream code asks for "the name", it gets
-- the primary multilingual-default name OSM editors use.

alter table public.osm_landing
  add column if not exists name text
    generated always as (tags->>'name') stored;

create index if not exists osm_landing_name_trgm_idx
  on public.osm_landing using gin (name gin_trgm_ops);
