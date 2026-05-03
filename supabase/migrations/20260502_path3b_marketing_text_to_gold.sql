-- Path 3b-3: move marketing text from public.beaches → beaches_gold.
--
-- These fields (website, description, parking_text) are the last
-- public.beaches-only attributes the consumer-facing edge functions
-- read. Moving them onto the spine drops the LEFT JOIN to
-- public.beaches in 4 edge functions and makes public.beaches truly
-- redundant (only the legacy slug remains).
--
-- Backfill: copy from public.beaches via arena_group_id. Beaches in
-- gold without a public.beaches row stay null (most of them — 749 of
-- 764 today don't have curated marketing text yet).

begin;

alter table public.beaches_gold
  add column if not exists website        text,
  add column if not exists description    text,
  add column if not exists parking_text   text;

update public.beaches_gold g
   set website      = b.website,
       description  = b.description,
       parking_text = b.parking_text
  from public.beaches b
 where b.arena_group_id = g.fid;

commit;

-- Verify (informational):
-- SELECT count(*) FILTER (WHERE website IS NOT NULL)      AS with_website,
--        count(*) FILTER (WHERE description IS NOT NULL)  AS with_description,
--        count(*) FILTER (WHERE parking_text IS NOT NULL) AS with_parking
--   FROM public.beaches_gold;
-- expect ~14-15 of each (the curated set).
