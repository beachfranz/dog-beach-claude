-- 20260503_harmony_phase8_4_beaches_gold_review.sql
--
-- Slice 4 of "harmony phase 8 — curator on canonical tables"
-- (project_curator_on_canonical_tables.md).
--
-- Adds curator review workflow columns to beaches_gold. Mirrors the
-- locations_stage vocabulary exactly (needs_review / verified / flagged)
-- so the curator's existing mental model carries forward.
--
-- No backfill in this slice. The 164 rows in locations_stage with
-- review_status='needs_review' stay there; migrating them would require
-- a legacy_fid → beaches_gold.fid mapping that doesn't exist yet
-- (see docs/harmony-7-3-audit.md).

begin;

alter table public.beaches_gold
  add column if not exists review_status text,
  add column if not exists review_notes  text;

-- Postgres doesn't support `IF NOT EXISTS` on ADD CONSTRAINT — guard with DO block.
do $$
begin
  if not exists (
    select 1 from pg_constraint
     where conname = 'beaches_gold_review_status_check'
       and conrelid = 'public.beaches_gold'::regclass
  ) then
    alter table public.beaches_gold
      add constraint beaches_gold_review_status_check
      check (review_status is null
             or review_status = any (array['needs_review','verified','flagged']));
  end if;
end $$;

create index if not exists beaches_gold_review_status_idx
  on public.beaches_gold (review_status)
  where review_status is not null;

comment on column public.beaches_gold.review_status is
  'Curator review workflow state: needs_review | verified | flagged. Set via admin-update-beaches-gold. Mirrors locations_stage vocabulary.';

commit;
