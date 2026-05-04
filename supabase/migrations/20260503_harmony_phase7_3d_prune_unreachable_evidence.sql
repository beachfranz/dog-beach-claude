-- 20260503_harmony_phase7_3d_prune_unreachable_evidence.sql
--
-- Phase 7.3 part 3a follow-up cleanup. After dropping the legacy `fid`
-- column, 1,911 evidence rows have NEITHER fid (column gone) NOR
-- gold_fid (no spatial backfill mapped them). They cannot be joined
-- to any beach. Pure dead weight.
--
-- Pre-drop these rows fell into two original buckets per the audit:
--   * 784 "safe to drop — gold-side duplicate exists" (gold-side row
--     already carries the same data)
--   * 1,131 "true orphans" (locations_stage rows without arena match;
--     the underlying source data is preserved in policy_research_extractions
--     / park_url_extractions / can be re-derived from spatial joins)
--
-- The id → legacy_fid mapping is preserved in
-- beach_enrichment_provenance_legacy_fid_archive (5,532 rows) so future
-- archeology is still possible.
--
-- DELETE is safe and reversible only via the archive table — but since
-- nothing currently joins these rows to anything, "reversal" would mean
-- re-deriving them from the archive + sources, which is the same as
-- re-running the gold populators on whatever new arena rows emerge.

begin;

delete from public.beach_enrichment_provenance
 where gold_fid is null;

commit;
