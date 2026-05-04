-- 20260503_harmony_evidence_fid_dual_namespace.sql
--
-- Phase 2 of the harmony branch migration (see docs/harmony.md).
--
-- Adds gold_fid as the new beaches_gold.fid-keyed discriminator on
-- beach_enrichment_provenance, alongside the legacy fid column.
--
-- Both fid namespaces coexist during the migration. Existing populators
-- (populate_from_cpad et al.) continue writing to legacy fid unchanged.
-- New populators (phase 3+) will write gold_fid instead.
--
-- Legacy fid range: up to 999,000,001 (POI/OSM/CCC source IDs).
-- gold_fid range:   93 → 9,716 (beaches_gold.fid; ~764 active rows today).
--
-- No FK constraint on gold_fid — beaches_gold rows can change during
-- catalog-side dedup work and we don't want evidence-write blocked on
-- referential integrity. FK in phase 7 cleanup.

begin;

alter table public.beach_enrichment_provenance
  add column if not exists gold_fid bigint;

create index if not exists beach_enrichment_provenance_gold_fid_idx
  on public.beach_enrichment_provenance (gold_fid)
  where gold_fid is not null;

create unique index if not exists beach_enrichment_provenance_gold_fid_fg_src_uq
  on public.beach_enrichment_provenance (gold_fid, field_group, source)
  where gold_fid is not null;

comment on column public.beach_enrichment_provenance.gold_fid is
  'Post-path-3 spine fid (beaches_gold.fid). Coexists with legacy fid during the harmony migration (see docs/harmony.md). Eventually gold_fid becomes canonical and legacy fid is dropped.';

commit;
