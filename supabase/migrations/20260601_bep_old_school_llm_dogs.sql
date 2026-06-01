-- 20260601_bep_old_school_llm_dogs.sql
--
-- Follow-up to 20260601_bep_source_cleanup.sql. Drop the 715
-- old_school_llm rows in field_group='dogs'. Sole-source audit:
--
--   89 sole-source orphan fids:
--     23 not in beaches_gold (gold dropped, BEP wasn't cleaned)
--     66 in beaches_gold but is_active=FALSE
--     0  active beaches
--
-- Consumer surface impact: zero. Multi-source rows (715 - 89 = 626)
-- are inert at tier 6 when better sources speak.
--
-- IMPORTANT scope limit: field_group='dogs' only. For field_group=
-- 'practical', 88 ACTIVE beaches are sole-sourced for old_school_llm
-- — bulk-delete would orphan them. Practical-scope cleanup needs a
-- different approach (per-beach review or replacement extractor).

BEGIN;

DELETE FROM public.beach_enrichment_provenance
 WHERE source = 'old_school_llm'
   AND field_group = 'dogs';

COMMIT;
