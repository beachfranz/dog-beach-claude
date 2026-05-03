-- Bridge: beaches.location_id → arena.group_id.
--
-- Lets the consumer-facing beaches table query arena_beach_metadata for
-- extracted policy/amenity data. Mapping done by hand (8 CA beaches);
-- 5 OR beaches stay null because arena is CA-only.
--
-- Known overlap: Huntington Dog Beach and Bolsa Chica State Beach both
-- map to arena group 8606 — arena's spatial grouping merged them since
-- HB Dog Beach is the southern section of the Bolsa Chica polygon.
-- They'll inherit identical extracted metadata; if user-perception
-- distinction matters for those, we'll need either:
--   a) re-run arena dedup with tighter spatial separation, or
--   b) maintain manual `dogs_allowed` etc. on beaches that override
--      the catalog's metadata (the consumer-curated layer already
--      handles this for the 5 dog-beach SoCal cases)

alter table public.beaches
  add column if not exists arena_group_id bigint;

create index if not exists beaches_arena_group_idx
  on public.beaches(arena_group_id);

comment on column public.beaches.arena_group_id is
  'Arena pipeline canonical group identifier. Join to arena_beach_metadata.arena_group_id for extracted policy/amenity data. NULL for OR beaches (arena is CA-only).';

-- ── Manual mappings (verified 2026-05-01 by name + lat/lng proximity) ──
update public.beaches set arena_group_id = 8606 where location_id = 'bolsa-chica-state-beach';
update public.beaches set arena_group_id = 8715 where location_id = 'coronado-dog-beach';
update public.beaches set arena_group_id = 8560 where location_id = 'del-mar-dog-beach';
update public.beaches set arena_group_id = 8606 where location_id = 'huntington-dog-beach';      -- merged into Bolsa Chica group; see header note
update public.beaches set arena_group_id = 8901 where location_id = 'huntington-city-beach';
update public.beaches set arena_group_id = 8453 where location_id = 'huntington-state-beach';
update public.beaches set arena_group_id = 8358 where location_id = 'ocean-beach-dog-beach';
update public.beaches set arena_group_id = 8727 where location_id = 'rosies-dog-beach';
