-- 20260503_harmony_phase7_3c_drop_legacy_fid.sql
--
-- Phase 7.3 part 2: drop the legacy fid column on beach_enrichment_provenance
-- + drop the legacy ingest pipeline functions that reference it.
--
-- Verification (per docs/harmony-7-3-audit.md):
--   * 100% of park_url evidence preserved byte-identical in park_url_extractions
--   * 96% of research/old_school_llm dogs evidence preserved byte-identical
--     in policy_research_extractions; 99% URL-preserved
--   * Spatial-derived sources (cpad/tiger_places/csp_parks/etc.) all
--     regenerable from current source layers via the gold populators
--   * 4 manual rows for Huntington Beach Dog Beach: re-attached to
--     gold_fid=9717 (added to arena/beaches_gold via seed_arena_beach.py
--     this session) + canonical entries written to beach_dog_policy +
--     beach_amenities by hand
--
-- Active-code dependency check: only `_gold` populators are referenced
-- in scripts/dagster + supabase/functions. Legacy populators are
-- referenced in 1 test script + 1 comment in v2-state-parks-dog-policy
-- (acceptable; tests will need to be retired separately).
--
-- This migration is reversible via the archive table (legacy_fid + id
-- mapping is preserved).

begin;

-- 1. Archive the legacy fid mapping. Future archeology can join id back
--    to figure out the original locations_stage source.
create table if not exists public.beach_enrichment_provenance_legacy_fid_archive as
select id, fid as legacy_fid, gold_fid, source, field_group, source_url, updated_at
  from public.beach_enrichment_provenance
 where fid is not null;

comment on table public.beach_enrichment_provenance_legacy_fid_archive is
  'Archive of (id → legacy fid) mapping captured before DROP COLUMN. Created 2026-05-03 by harmony phase 7.3 part 2. Use to reverse-engineer "what was the original locations_stage row for evidence id X?" if ever needed.';

-- 2. Drop legacy ingest pipeline functions — they all reference the
--    fid column or the legacy locations_stage spine and would error
--    on next call if left in place.
drop function if exists public.populate_all(integer);
drop function if exists public.populate_from_cpad(integer);
drop function if exists public.populate_from_csp_parks(integer);
drop function if exists public.populate_from_jurisdictions(integer);
drop function if exists public.populate_from_military_bases(integer);
drop function if exists public.populate_from_nps_places(integer);
drop function if exists public.populate_from_park_operators(integer);
drop function if exists public.populate_from_park_url(integer);
drop function if exists public.populate_from_private_land_zones(integer);
drop function if exists public.populate_from_research(integer);
drop function if exists public.populate_from_tribal_lands(integer);
drop function if exists public.populate_governance_from_name(integer);
drop function if exists public.populate_dogs_from_governing_body(integer);
drop function if exists public.populate_layer1_geographic(integer);
drop function if exists public._emit_evidence_from_park_url(integer);
drop function if exists public._rank_park_url_evidence(integer, text);
drop function if exists public._resolve_dogs(integer);
drop function if exists public._resolve_governance(integer);
drop function if exists public._resolve_practical(integer);
drop function if exists public._resolve_research_evidence(integer);
drop function if exists public._resolve_tiger_vs_operator(integer);
drop function if exists public._promote_dogs_to_stage(integer);
drop function if exists public._promote_governance_to_stage(integer);
drop function if exists public._promote_practical_to_stage(integer);
drop function if exists public._compute_review_flags(integer);
drop function if exists public.flag_dogs_consistency(integer);
drop function if exists public.pick_canonical_evidence(integer, text);
drop function if exists public.resolve_access(integer);
drop function if exists public.resolve_dogs(integer);
drop function if exists public.resolve_governance(integer);
drop function if exists public.resolve_practical(integer);

-- 3. Drop the FK to locations_stage. The column drop won't cascade-drop
--    the FK automatically because referential constraints aren't tracked
--    as DROP-on-column dependencies.
alter table public.beach_enrichment_provenance
  drop constraint if exists beach_enrichment_provenance_fid_fkey;

-- 4. Drop the legacy fid column. CASCADE auto-drops the 3 fid-only indexes
--    (bep_fid_group_idx, bep_one_canonical_per_group, bep_one_per_fid_group_source_url).
alter table public.beach_enrichment_provenance
  drop column if exists fid cascade;

commit;
