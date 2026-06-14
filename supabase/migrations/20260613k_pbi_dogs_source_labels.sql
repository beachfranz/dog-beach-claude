-- 20260613k_pbi_dogs_source_labels.sql
--
-- Add human-readable source labels to the PBI dogs metadata model.
-- Today PBI shows source strings like 'codified_v1:ps_173' which are
-- inscrutable — the ps_N suffix is a foreign key into policy_source
-- but PBI users can't see what document each ID references.
--
-- This migration adds a `source_label` column to:
--   1. vw_pbi_dogs_evidence — for inline display alongside each BEP row
--   2. vw_pbi_dogs_source_tier — so the dim can be used as a slicer
--      with friendly names
--
-- The label resolves codified_v1:ps_N → policy_source.citation
-- (e.g. 'Long Beach Municipal Code §6.16.310'). Non-codified sources
-- pass through as the source string itself.

BEGIN;

-- ─── 1. vw_pbi_dogs_evidence + source_label ──────────────────────────
DROP VIEW IF EXISTS public.vw_pbi_dogs_evidence;
CREATE VIEW public.vw_pbi_dogs_evidence AS
SELECT
  bep.gold_fid                                AS fid,
  bep.source,
  -- Human-readable label: codified_v1:ps_N → policy_source.citation;
  -- other sources pass through.
  CASE
    WHEN bep.source ~ '^codified_v1:ps_\d+$' THEN
      coalesce(
        (SELECT 'codified: ' || citation FROM public.policy_source
          WHERE id = substring(bep.source FROM 'codified_v1:ps_(\d+)$')::int),
        bep.source
      )
    ELSE bep.source
  END                                         AS source_label,
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
  coalesce(
    bep.claimed_values->>'area_sand',
    bep.claimed_values->'sections'->'sand'->>'rule',
    (SELECT r->>'rule' FROM jsonb_array_elements(coalesce(bep.claimed_values->'rules','[]'::jsonb)) r
      WHERE r->>'section' = 'sand' LIMIT 1)
  )                                           AS claim_sand_rule,
  coalesce(
    bep.claimed_values->>'area_water',
    bep.claimed_values->'sections'->'water'->>'rule',
    (SELECT r->>'rule' FROM jsonb_array_elements(coalesce(bep.claimed_values->'rules','[]'::jsonb)) r
      WHERE r->>'section' = 'water' LIMIT 1)
  )                                           AS claim_water_rule,
  coalesce(
    bep.claimed_values->'sections'->'trails'->>'rule',
    (SELECT r->>'rule' FROM jsonb_array_elements(coalesce(bep.claimed_values->'rules','[]'::jsonb)) r
      WHERE r->>'section' = 'trails' LIMIT 1)
  )                                           AS claim_trails_rule,
  coalesce(
    bep.claimed_values->'sections'->'picnic_area'->>'rule',
    (SELECT r->>'rule' FROM jsonb_array_elements(coalesce(bep.claimed_values->'rules','[]'::jsonb)) r
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
  'Long-form evidence list for dogs field_group. source_label resolves codified_v1:ps_N → policy_source.citation; other sources pass through. Join to vw_pbi_dogs_source_tier on source for tier dim.';

-- ─── 2. vw_pbi_dogs_source_tier + source_label ───────────────────────
DROP VIEW IF EXISTS public.vw_pbi_dogs_source_tier;
CREATE VIEW public.vw_pbi_dogs_source_tier AS
WITH dist AS (
  SELECT DISTINCT source FROM public.beach_enrichment_provenance
   WHERE field_group = 'dogs'
)
SELECT
  source,
  CASE
    WHEN source ~ '^codified_v1:ps_\d+$' THEN
      coalesce(
        (SELECT 'codified: ' || citation FROM public.policy_source
          WHERE id = substring(source FROM 'codified_v1:ps_(\d+)$')::int),
        source
      )
    ELSE source
  END                                         AS source_label,
  CASE
    WHEN source ~* '^state_'                                THEN '01 State'
    WHEN source ~* '^county_'                               THEN '02 County'
    WHEN source ~* '^operator_county'                       THEN '03 Operator (county)'
    WHEN source ~* '^operator_city'                         THEN '03 Operator (city)'
    WHEN source ~* '^operator_pad_us'                       THEN '03 Operator (pad_us)'
    WHEN source ~* '^operator_'                             THEN '03 Operator (other)'
    WHEN source ~* '^city_'                                 THEN '04 City'
    WHEN source ~* '^pad_us_'                               THEN '05 PAD-US unit'
    WHEN source ~* '^cpad'                                  THEN '05 CPAD unit'
    WHEN source ~* '^codified_'                             THEN '06 Codified statute'
    WHEN source ~* '^park_url'                              THEN '07 Park URL extraction'
    WHEN source ~* '^per_beach.*offleash_v2'                THEN '08 Per-beach off-leash extractor'
    WHEN source ~* '^per_beach'                             THEN '08 Per-beach extractor'
    WHEN source ~* '^section_research|^text_repass|^research' THEN '09 LLM research'
    WHEN source = 'manual' OR source ~* '_manual$'          THEN '10 Manual curator'
    WHEN source = 'manual_curator'                          THEN '10 Manual curator'
    WHEN source ~* '_no_result$'                            THEN '99 Sentinel (no result)'
    ELSE '99 Other'
  END AS source_tier,
  CASE
    WHEN source ~* '_no_result$' THEN
      'Sentinel — codifies "no answer found" so future passes know it was attempted. Never canonical.'
    WHEN source ~* '^codified_' THEN
      'Statutory text extracted from Municode / CodePublishing / AmLegal codify pipeline.'
    WHEN source ~* '^per_beach' THEN
      'Per-beach LLM extractor (Tavily web_search + Sonnet/Haiku) with cite-required output.'
    ELSE NULL
  END AS source_description
FROM dist;

COMMENT ON VIEW public.vw_pbi_dogs_source_tier IS
  'Small dim joining to vw_pbi_dogs_evidence on source. source_label resolves codified_v1:ps_N to policy_source.citation; use it instead of source in PBI visuals for human-readable display.';

COMMIT;

NOTIFY pgrst, 'reload schema';
