-- Path 3a step 5: dual-key the scoring tables.
--
-- Adds arena_group_id (nullable, no FK yet) to beach_day_hourly_scores
-- and beach_day_recommendations, backfilled from public.beaches via
-- location_id. Edge functions in step 7 will read/write either key
-- during the deprecation window. FK to beaches_gold + drop of
-- location_id happens in 3b cutover.

begin;

-- 5a. Add columns
alter table public.beach_day_hourly_scores
  add column if not exists arena_group_id bigint;

alter table public.beach_day_recommendations
  add column if not exists arena_group_id bigint;

-- 5b. Backfill from public.beaches
update public.beach_day_hourly_scores h
   set arena_group_id = b.arena_group_id
  from public.beaches b
 where b.location_id = h.location_id
   and b.arena_group_id is not null;

update public.beach_day_recommendations r
   set arena_group_id = b.arena_group_id
  from public.beaches b
 where b.location_id = r.location_id
   and b.arena_group_id is not null;

-- 5c. Indexes (queries will increasingly use arena_group_id)
create index if not exists beach_day_hourly_scores_arena_idx
  on public.beach_day_hourly_scores (arena_group_id);

create index if not exists beach_day_recommendations_arena_idx
  on public.beach_day_recommendations (arena_group_id);

commit;
