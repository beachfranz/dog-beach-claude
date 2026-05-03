-- Retarget the two FKs that currently point at us_beach_points to point at
-- beaches_gold instead. Run AFTER beaches_gold is populated for at least
-- the states whose beaches have extractions / gold-set rows.
--
-- This is the one-way door. Do NOT apply until:
--   1. beaches_gold has all 35 currently-curated CA beaches (verify by
--      joining beach_policy_extractions.fid against beaches_gold.fid)
--   2. arena population has been re-run for CA at least once with the
--      new logic and is stable
--   3. Anyone reading us_beach_points has been migrated (beach_locations,
--      beach_access_source, dbt staging, etc.) — see audit
--
-- Safety check before applying — should return 0:
--   SELECT count(*)
--     FROM public.beach_policy_extractions e
--    WHERE NOT EXISTS (SELECT 1 FROM public.beaches_gold g WHERE g.fid = e.fid);
--   SELECT count(*)
--     FROM public.beach_policy_gold_set g
--    WHERE NOT EXISTS (SELECT 1 FROM public.beaches_gold bg WHERE bg.fid = g.fid);
-- If either returns > 0, those rows reference a fid that exists in
-- us_beach_points but NOT in beaches_gold. Either backfill those into
-- beaches_gold or accept dropping the FK altogether.

begin;

-- 1. beach_policy_extractions ─────────────────────────────────────────
alter table public.beach_policy_extractions
  drop constraint if exists beach_policy_extractions_fid_fkey;

alter table public.beach_policy_extractions
  add constraint beach_policy_extractions_fid_fkey
  foreign key (fid) references public.beaches_gold(fid)
  on delete restrict;

-- 2. beach_policy_gold_set ────────────────────────────────────────────
alter table public.beach_policy_gold_set
  drop constraint if exists beach_policy_gold_set_fid_fkey;

alter table public.beach_policy_gold_set
  add constraint beach_policy_gold_set_fid_fkey
  foreign key (fid) references public.beaches_gold(fid)
  on delete restrict;

commit;
