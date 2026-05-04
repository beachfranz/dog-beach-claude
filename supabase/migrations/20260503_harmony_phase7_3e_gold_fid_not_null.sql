-- 20260503_harmony_phase7_3e_gold_fid_not_null.sql
--
-- Phase 7.3 part 3a final cleanup. Now that 100% of beach_enrichment_provenance
-- rows have gold_fid set, lock it in:
--   * Add NOT NULL constraint
--   * Replace the partial unique index (... WHERE gold_fid IS NOT NULL) with
--     a regular full unique index — same semantics now that gold_fid is
--     always set, but cleaner and slightly more efficient
--   * Same for the partial regular index

begin;

-- 1. NOT NULL constraint
alter table public.beach_enrichment_provenance
  alter column gold_fid set not null;

-- 2. Replace partial unique index with full unique index
drop index if exists public.beach_enrichment_provenance_gold_fid_fg_src_uq;
create unique index beach_enrichment_provenance_gold_fid_fg_src_uq
  on public.beach_enrichment_provenance (gold_fid, field_group, source);

-- 3. Replace partial gold_fid index with full
drop index if exists public.beach_enrichment_provenance_gold_fid_idx;
create index beach_enrichment_provenance_gold_fid_idx
  on public.beach_enrichment_provenance (gold_fid);

comment on column public.beach_enrichment_provenance.gold_fid is
  'beaches_gold.fid — required, the canonical beach key. Legacy fid column dropped 2026-05-03 in harmony phase 7.3 part 3a.';

commit;
