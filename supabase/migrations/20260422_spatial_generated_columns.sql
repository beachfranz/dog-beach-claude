-- Spatial infrastructure: auto-populated geography columns on beaches
-- and beaches_staging_new, with GIST indexes for server-side KNN / radius
-- queries at 10k+ location scale.
--
-- Why: JS-side Haversine on the full beaches list is fine at 13 records
-- but breaks down as we approach the 10k-beach / 100k-park target. A GIST
-- index on geography lets us answer "nearest N to user" in ms instead of
-- transferring the whole table to the client.
--
-- Why generated columns instead of triggers: declarative. The lat/lng
-- columns remain the canonical source; location is computed from them
-- and can never drift. Today's audit caught exactly that drift on
-- huntington-dog-beach (lat/lng was corrected, location column wasn't).
-- Generated columns make that class of bug impossible.
--
-- Why STORED (not VIRTUAL): GIST indexes require stored data, and
-- PostgreSQL 15 doesn't support virtual generated columns anyway.
--
-- Applied to both beaches (live) and beaches_staging_new (pipeline-side)
-- because staging-side proximity queries (neighbor-inherit, dedup) will
-- need the index once staging volume grows.

begin;

-- ── beaches ────────────────────────────────────────────────────────────
-- The existing location column was manually populated; 5 of 13 rows had
-- it set, and one (huntington-dog-beach) was stale after a coordinate
-- correction. Drop + recreate as generated so it always mirrors lat/lng.

drop index if exists public.beaches_location_gist;
alter table public.beaches drop column if exists location;

alter table public.beaches
  add column location geography(Point, 4326)
  generated always as (
    ST_SetSRID(
      ST_MakePoint(longitude::float8, latitude::float8),
      4326
    )::geography
  ) stored;

create index beaches_location_gist
  on public.beaches using gist (location);

-- ── beaches_staging_new ────────────────────────────────────────────────
-- No existing location column. Add fresh.

alter table public.beaches_staging_new
  add column location geography(Point, 4326)
  generated always as (
    ST_SetSRID(
      ST_MakePoint(longitude, latitude),
      4326
    )::geography
  ) stored;

create index beaches_staging_new_location_gist
  on public.beaches_staging_new using gist (location);

commit;
