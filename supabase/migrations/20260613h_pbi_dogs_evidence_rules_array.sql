-- 20260613h_pbi_dogs_evidence_rules_array.sql
--
-- Extend the section-rule columns on vw_pbi_dogs_evidence to also
-- merge the codified-pipeline `rules[]` array path. The codify
-- pipeline (codified_v1:*) emits section evidence in this shape:
--
--   { "rules": [
--       { "region_name": null, "section": "sand",  "rule": "off_leash" },
--       { "region_name": null, "section": "water", "rule": "on_leash"  }
--     ],
--     "subtype": "mou",
--     "citation": "..."
--   }
--
-- The previous view (20260613g) merged flat (area_sand, area_water)
-- + nested (sections.{sand,water,trails,picnic_area}.rule) shapes
-- but missed the rules[] array — leaving ~1,140 codified BEP rows
-- of section evidence invisible to PBI.
--
-- Net effect: claim_sand_rule coverage jumps from ~919 to ~1,800+
-- distinct beaches (still sparse, but ~95% increase). Same for
-- water, trails, picnic.
--
-- When rules[] has multiple entries for the same section (rare;
-- happens when a beach has region_name="north section" vs
-- "south section" entries), we pick the FIRST match. The full
-- array is still available via claim_rules for users who need
-- region-level detail.

BEGIN;

DROP VIEW IF EXISTS public.vw_pbi_dogs_evidence;
CREATE VIEW public.vw_pbi_dogs_evidence AS
SELECT
  bep.gold_fid                                AS fid,
  bep.source,
  bep.source_url,
  bep.confidence,
  bep.is_canonical,
  bep.extraction_type,
  bep.cpad_role,
  bep.cpad_unit_name,
  bep.claimed_values->>'allowed'              AS claim_allowed,
  coalesce((bep.claimed_values->>'_allowed_derived')::boolean, false)
                                              AS claim_allowed_derived,
  bep.claimed_values->>'leash_required'       AS claim_leash_required,
  bep.claimed_values->>'default_rule'         AS claim_default_rule,
  (bep.claimed_values->>'off_leash_exists')::boolean AS claim_off_leash_exists,
  -- Per-section rules — merged across THREE shapes:
  --   1. flat top-level area_* (city_policy, county_policy)
  --   2. nested sections.<key>.rule (section_research_v1)
  --   3. rules[] array entries (codified_v1) — NEW in 20260613h
  -- First non-null wins. Full rules[] array still available as claim_rules.
  coalesce(
    bep.claimed_values->>'area_sand',
    bep.claimed_values->'sections'->'sand'->>'rule',
    (SELECT r->>'rule'
       FROM jsonb_array_elements(coalesce(bep.claimed_values->'rules','[]'::jsonb)) r
      WHERE r->>'section' = 'sand' LIMIT 1)
  )                                           AS claim_sand_rule,
  coalesce(
    bep.claimed_values->>'area_water',
    bep.claimed_values->'sections'->'water'->>'rule',
    (SELECT r->>'rule'
       FROM jsonb_array_elements(coalesce(bep.claimed_values->'rules','[]'::jsonb)) r
      WHERE r->>'section' = 'water' LIMIT 1)
  )                                           AS claim_water_rule,
  coalesce(
    bep.claimed_values->'sections'->'trails'->>'rule',
    (SELECT r->>'rule'
       FROM jsonb_array_elements(coalesce(bep.claimed_values->'rules','[]'::jsonb)) r
      WHERE r->>'section' = 'trails' LIMIT 1)
  )                                           AS claim_trails_rule,
  coalesce(
    bep.claimed_values->'sections'->'picnic_area'->>'rule',
    (SELECT r->>'rule'
       FROM jsonb_array_elements(coalesce(bep.claimed_values->'rules','[]'::jsonb)) r
      WHERE r->>'section' = 'picnic_area' LIMIT 1)
  )                                           AS claim_picnic_rule,
  bep.claimed_values->>'source_quote'         AS claim_source_quote,
  bep.claimed_values->>'ordinance_ref'        AS claim_ordinance_ref,
  bep.claimed_values->>'scope_notes'          AS claim_scope_notes,
  bep.claimed_values->>'subtype'              AS claim_subtype,
  bep.claimed_values->>'citation'             AS claim_citation,
  bep.claimed_values->>'notes'                AS claim_notes,
  bep.claimed_values->>'operator_name'        AS claim_operator_name,
  bep.claimed_values->>'mng_name'             AS claim_mng_name,
  bep.claimed_values->'time_windows'          AS claim_time_windows,
  bep.claimed_values->'rules'                 AS claim_rules,
  bep.claimed_values->'sections'              AS claim_sections,
  bep.claimed_values                          AS claim_raw_jsonb,
  (bep.source ~* '_no_result$')               AS is_sentinel
FROM public.beach_enrichment_provenance bep
JOIN public.beaches_gold b ON b.fid = bep.gold_fid
WHERE bep.field_group = 'dogs'
  AND b.is_active AND b.scoring_tier IN ('daily','hourly');

COMMENT ON VIEW public.vw_pbi_dogs_evidence IS
  'Long-form evidence list for dogs field_group. claim_{sand,water,trails,picnic}_rule unify three extractor shapes: flat (area_*), nested (sections.*), and codified rules[] array. claim_allowed post-normalization (restricted/seasonal→mixed); claim_allowed_derived flags rows where allowed was inferred (not directly extracted).';

COMMIT;

NOTIFY pgrst, 'reload schema';
