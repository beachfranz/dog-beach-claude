-- Path 3b-3 (final): retire location_id as the PK on scoring tables.
--
-- After this migration:
--   beach_day_hourly_scores PK    = (arena_group_id, forecast_ts)
--   beach_day_recommendations PK  = (arena_group_id, local_date)
--   location_id remains as a nullable column for the BC deprecation
--   window so old reads that still filter by location_id keep working.
--
-- Pre-flight (verified 2026-05-02):
--   - 240 hourly + 10 rec rows have NULL arena_group_id (all inactive
--     sunset-beach data). Deleted by this migration.
--   - 0 (arena_group_id, forecast_ts) duplicates and 0 (arena_group_id,
--     local_date) duplicates.
--
-- After this commit, edge functions can switch upserts to use
-- arena_group_id as the conflict key. public.beaches becomes
-- droppable once nothing else needs the slug.

begin;

-- 1. Drop the orphan rows (no arena_group_id → no spine entry, can't
--    survive the NOT NULL gate)
delete from public.beach_day_hourly_scores  where arena_group_id is null;
delete from public.beach_day_recommendations where arena_group_id is null;

-- 2. Make arena_group_id NOT NULL on both
alter table public.beach_day_hourly_scores
  alter column arena_group_id set not null;

alter table public.beach_day_recommendations
  alter column arena_group_id set not null;

-- 3. Swap the PK on beach_day_hourly_scores
alter table public.beach_day_hourly_scores
  drop constraint if exists beach_day_hourly_scores_pkey;

alter table public.beach_day_hourly_scores
  add constraint beach_day_hourly_scores_pkey
  primary key (arena_group_id, forecast_ts);

-- 4. Swap the PK on beach_day_recommendations
alter table public.beach_day_recommendations
  drop constraint if exists beach_day_recommendations_pkey;

alter table public.beach_day_recommendations
  add constraint beach_day_recommendations_pkey
  primary key (arena_group_id, local_date);

-- 5. Allow location_id to be null (so future scoring rows for catalog
--    beaches without a public.beaches row are possible)
alter table public.beach_day_hourly_scores
  alter column location_id drop not null;

alter table public.beach_day_recommendations
  alter column location_id drop not null;

-- 6. Preserve uniqueness on (location_id, ...) where location_id is set
--    — protects against accidental duplicate writes during BC window.
create unique index if not exists beach_day_hourly_scores_loc_uidx
  on public.beach_day_hourly_scores (location_id, forecast_ts)
  where location_id is not null;

create unique index if not exists beach_day_recommendations_loc_uidx
  on public.beach_day_recommendations (location_id, local_date)
  where location_id is not null;

commit;
