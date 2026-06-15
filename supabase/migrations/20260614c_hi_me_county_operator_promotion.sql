-- 20260614c_hi_me_county_operator_promotion.sql
--
-- Promote 13 existing TIGER-seeded county operators from the extraction
-- pool (is_canonical=false) into the consumer-surface registry
-- (is_canonical=true). The resolver function `_resolve_beach_operator`
-- filters on `op.is_canonical = true` at every tier, so non-canonical
-- operator rows are silently dropped from the cascade — leaving HI 94%
-- orphaned and ME 97% orphaned at session-end 2026-06-14.
--
-- All 13 rows have `origin_source='tiger_counties'`, parent_operator_id
-- IS NULL, and were seeded programmatically (not LLM-derived), so the
-- usual "non-canonical means unvetted LLM candidate" semantic doesn't
-- apply — they're real TIGER county polygons that were never explicitly
-- promoted. This migration finishes the seeding step.
--
-- Scope:
--   HI: Hawaii, Honolulu, Kalawao, Kauai, Maui (5 counties)
--   ME: Androscoggin, Aroostook, Kennebec, Oxford, Penobscot,
--       Piscataquis, Sagadahoc, Waldo (8 inland counties)
--
-- Sibling migration 20260614d creates the 8 missing ME COASTAL county
-- operators (Cumberland, Franklin, Hancock, Knox, Lincoln, Somerset,
-- Washington, York) that don't yet exist in the operators table.
--
-- Expected resolver impact:
--   HI: 349 orphan beaches → 0 (Hawaii County, Honolulu, Maui, Kauai
--     each pick up their county's beaches via via_county tier)
--   ME inland: 53 orphan beaches → 0
--   Combined: 402 of 754 orphan beaches resolve to a canonical operator
--
-- This migration does NOT:
--   - Materialize canonical_operator_id on beaches_gold (separate step)
--   - Curate operator_dogs_policy for these counties (separate work)
--   - Cause any populator to retroactively re-emit BEP rows; new rows
--     get the resolved operator on next run

BEGIN;

UPDATE public.operators
   SET is_canonical = true,
       updated_at   = now()
 WHERE state_code IN ('HI','ME')
   AND level = 'county'
   AND origin_source = 'tiger_counties'
   AND is_canonical = false;

-- Audit: this should report 13 if the upstream pool is unchanged
SELECT count(*) AS promoted_rows
  FROM public.operators
 WHERE state_code IN ('HI','ME')
   AND level = 'county'
   AND is_canonical = true
   AND origin_source = 'tiger_counties';

COMMIT;

NOTIFY pgrst, 'reload schema';
