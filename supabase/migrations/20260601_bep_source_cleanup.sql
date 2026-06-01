-- 20260601_bep_source_cleanup.sql
--
-- Closes out [[deferred-bep-source-cleanup]] for the safe-to-delete
-- portion. Pin is being retired.
--
-- Scope: field_group='dogs' rows where source is a known-dead
-- transformation step or feedback-loop emitter.
--
-- Pre-flight sole-source audit (2026-06-01):
--   json_explode + zone_rules_derived_v1 in field_group='dogs'
--     -> 0 sole-source beaches (audited via NOT EXISTS join)
--
-- NOT included: old_school_llm. Sole-source check showed 89 beaches
-- where old_school_llm is the only dog-policy BEP source. Per-beach
-- review needed; bulk-delete would orphan those 89 beaches. The
-- resolver already ignores old_school_llm at tier 6 when better
-- sources speak, so leaving it in place is safe.
--
-- NOT included: unified_v1 + text_repass_v1. The pin called them
-- "superseded extractor passes" but they are still current emitters:
--   unified_v1     <- scripts/extract_for_beach.py:64
--   text_repass_v1 <- scripts/repass_zone_rules.py
-- Not retired; do not delete.

BEGIN;

DELETE FROM public.beach_enrichment_provenance
 WHERE source IN ('json_explode', 'zone_rules_derived_v1')
   AND field_group = 'dogs';

COMMIT;
