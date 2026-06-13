-- 20260613g_pbi_dogs_evidence_section_rules.sql
--
-- Consolidate per-section rule extraction on vw_pbi_dogs_evidence.
-- Different extractors emit section rules in different JSONB shapes:
--
--   Flat top-level (city_policy, e.g. HB park rules):
--     { area_sand: "forbidden", area_water: "on_leash", ... }
--
--   Nested sections (section_research_v1, codified_v1, etc.):
--     { sections: { sand: {rule: "not_allowed"}, water: {rule: "on_leash"}, ... } }
--
-- 20260613e's view only surfaced the flat keys, leaving the more
-- common nested pattern invisible. claim_area_sand was 211 rows;
-- claim_sections->'sand'->>'rule' is 712 rows. Combined coverage
-- (sand) is ~900+ rows.
--
-- Replace claim_area_sand / claim_area_water with unified columns:
--   claim_sand_rule       prefers area_sand, falls back to sections.sand.rule
--   claim_water_rule      same for water
--   claim_trails_rule     sections.trails.rule (no flat equivalent)
--   claim_picnic_rule     sections.picnic_area.rule (no flat equivalent)
--
-- claim_sections kept for users who want the raw nested JSONB.

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
  -- Allowed (post-normalization from 20260613f) + audit flag
  bep.claimed_values->>'allowed'              AS claim_allowed,
  coalesce((bep.claimed_values->>'_allowed_derived')::boolean, false)
                                              AS claim_allowed_derived,
  -- Leash policy (post-normalization)
  bep.claimed_values->>'leash_required'       AS claim_leash_required,
  bep.claimed_values->>'default_rule'         AS claim_default_rule,
  (bep.claimed_values->>'off_leash_exists')::boolean AS claim_off_leash_exists,
  -- Per-section rules — merged across flat (area_*) + nested (sections.*) patterns
  coalesce(
    bep.claimed_values->>'area_sand',
    bep.claimed_values->'sections'->'sand'->>'rule'
  )                                           AS claim_sand_rule,
  coalesce(
    bep.claimed_values->>'area_water',
    bep.claimed_values->'sections'->'water'->>'rule'
  )                                           AS claim_water_rule,
  bep.claimed_values->'sections'->'trails'->>'rule'         AS claim_trails_rule,
  bep.claimed_values->'sections'->'picnic_area'->>'rule'    AS claim_picnic_rule,
  -- Audit / detail
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
  'Long-form evidence list for dogs field_group. claim_{sand,water,trails,picnic}_rule unify the flat (area_*) + nested (sections.*) patterns extractors emit. claim_allowed is post-normalization (restricted/seasonal→mixed); claim_allowed_derived flags rows where the value was inferred (not directly extracted).';

COMMIT;

NOTIFY pgrst, 'reload schema';
