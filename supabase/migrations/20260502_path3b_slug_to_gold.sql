-- Path 3b-3 (final): move location_id (legacy slug) onto beaches_gold,
-- then drop public.beaches.
--
-- After path 3a/3b/3b-3.x, public.beaches is read only for the slug.
-- Putting it on beaches_gold removes the last reason to JOIN.
--
-- Pre-flight (verified 2026-05-02): every active row in public.beaches
-- has arena_group_id set (5 OR seeds + 8 CA matched + Mission + Fiesta).
-- The 1 inactive sunset-beach row has arena_group_id=NULL → its slug
-- doesn't migrate.

begin;

-- 1. Add the slug column on the spine
alter table public.beaches_gold
  add column if not exists location_id text;

create unique index if not exists beaches_gold_location_id_uidx
  on public.beaches_gold (location_id)
  where location_id is not null;

-- 2. Backfill from public.beaches via arena_group_id
update public.beaches_gold g
   set location_id = b.location_id
  from public.beaches b
 where b.arena_group_id = g.fid
   and b.location_id is not null;

commit;

-- Verify (informational):
-- SELECT count(*) FILTER (WHERE location_id IS NOT NULL) FROM public.beaches_gold;
-- expect: 15 (the curated set).
