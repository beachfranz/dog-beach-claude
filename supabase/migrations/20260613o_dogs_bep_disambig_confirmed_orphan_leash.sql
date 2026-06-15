-- 20260613o_dogs_bep_disambig_confirmed_orphan_leash.sql
--
-- Surgical orphan-leash cleanup for contradiction rows where the
-- per_beach_disambiguate_v1 extractor has confirmed scenario A
-- (prohibition correct, leash field is orphan city-wide context that
-- doesn't apply at the beach).
--
-- Context. Migration 20260613n cleaned ~583 contradiction rows where the
-- row had per-section evidence (area_*, sections, rules). That left 526
-- bare-contradiction rows from programmatic populators (operator_county,
-- operator_pad_us, operator_city, city_policy, county_policy, etc.) that
-- lacked per-section evidence and were excluded from the normalizer.
--
-- The extended disambiguator (extract_dogs_disambiguate_v1.py with
-- --include-no-notes) ran 2026-06-13 LATE and confirmed scenario A for
-- 145 fids — wrote `per_beach_disambiguate_v1` rows with allowed=no and
-- leash_required=null at confidence 0.92/0.80.
--
-- But the cascade resolver tiers per_beach_disambiguate_v1 BELOW
-- city_policy (04) and county_policy (02), so for ~51 fids the
-- disambig confirmation is OUTRANKED and the city/county_policy row
-- with the orphan leash field is still winning canonical.
--
-- This migration: for any contradiction row (allowed=no AND
-- leash_required IS NOT NULL) on a fid that has a scenario A disambig
-- confirmation, drop leash_required from claimed_values and stamp
-- `_leash_required_orphan_cleared: true` + `_orphan_cleared_via:
-- disambig_v1`. Preserves source-priority hierarchy; surgically removes
-- the orphan field on the higher-tier row.
--
-- Per Franz 2026-06-13 LATE: keep cascade priority order untouched;
-- clean orphans via disambig-confirmed evidence instead.

BEGIN;

-- Identify the (gold_fid, original_source) pairs to clean. A scenario A
-- confirmation = per_beach_disambiguate_v1 row with allowed=no AND
-- leash_required IS NULL. We don't require is_canonical on the disambig
-- row (it may itself be outranked but the LLM verdict still applies).
UPDATE public.beach_enrichment_provenance bep
   SET claimed_values =
         (bep.claimed_values - 'leash_required')
         || jsonb_build_object(
              '_leash_required_orphan_cleared', true,
              '_orphan_cleared_via', 'disambig_v1'
            )
 WHERE bep.field_group = 'dogs'
   AND bep.claimed_values->>'allowed' = 'no'
   AND bep.claimed_values->>'leash_required' IS NOT NULL
   AND bep.source <> 'per_beach_disambiguate_v1'
   AND EXISTS (
     SELECT 1
       FROM public.beach_enrichment_provenance d
      WHERE d.gold_fid = bep.gold_fid
        AND d.field_group = 'dogs'
        AND d.source = 'per_beach_disambiguate_v1'
        AND d.claimed_values->>'allowed' = 'no'
        AND d.claimed_values->>'leash_required' IS NULL
   );

COMMIT;

NOTIFY pgrst, 'reload schema';
