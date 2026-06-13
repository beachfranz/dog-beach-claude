-- 20260613d_pbi_dogs_metadata_model.sql
--
-- Power BI metadata model for the dog access + leash policy process.
-- Three views forming a small star schema so PBI can visualize:
--   1. beach (one row per scored beach) — consumer surface + evidence
--      counts. Star center.
--   2. evidence (one row per BEP row for field_group='dogs') — the long
--      list of every claim every source ever made about a beach's
--      dog policy. Joins to (1) on fid, to (3) on source.
--   3. source tier (one row per distinct evidence source) — classifies
--      each source into pipeline tier (state-level, county-level,
--      operator, park-specific, LLM-extracted, etc.) so PBI bar/sankey
--      visuals can group naturally.
--
-- Read-only views. RLS not opened (anon can't reach beach_enrichment_
-- provenance per existing policy). Power BI connects with the
-- service-role-grade DB user via the pooler, so RLS doesn't apply.

BEGIN;

-- ─── 1. Per-beach summary (star center) ──────────────────────────────
CREATE OR REPLACE VIEW public.vw_pbi_dogs_beach AS
WITH ev_counts AS (
  SELECT bep.gold_fid AS fid,
         count(*) AS total_evidence_rows,
         count(*) FILTER (WHERE bep.is_canonical) AS canonical_evidence_rows,
         count(DISTINCT bep.source) AS distinct_evidence_sources,
         count(*) FILTER (WHERE bep.source ~* '_no_result$|^per_beach.*no_result') AS sentinel_rows
    FROM public.beach_enrichment_provenance bep
   WHERE bep.field_group = 'dogs'
   GROUP BY bep.gold_fid
)
SELECT
  b.fid,
  b.location_id,
  coalesce(b.display_name_override, b.name) AS beach_name,
  b.state,
  b.county_name,
  b.scoring_tier,
  -- Consumer surface (what beach.html / find.html read)
  bdp.dogs_allowed,
  bdp.leash_policy,
  bdp.has_off_leash,
  bdp.has_on_leash,
  bdp.off_leash_flag,
  bdp.access_rule,
  bdp.dogs_prohibited_start,
  bdp.dogs_prohibited_end,
  bdp.source                                  AS policy_source,
  bdp.curated_at                              AS policy_curated_at,
  bdp.zone_rules_updated_at,
  bdp.zone_rules IS NOT NULL                  AS has_zone_rules,
  -- Evidence layer (BEP) counts
  coalesce(ec.total_evidence_rows, 0)         AS total_evidence_rows,
  coalesce(ec.canonical_evidence_rows, 0)     AS canonical_evidence_rows,
  coalesce(ec.distinct_evidence_sources, 0)   AS distinct_evidence_sources,
  coalesce(ec.sentinel_rows, 0)               AS sentinel_evidence_rows
FROM public.beaches_gold b
LEFT JOIN public.beach_dog_policy bdp ON bdp.arena_group_id = b.fid
LEFT JOIN ev_counts ec ON ec.fid = b.fid
WHERE b.is_active AND b.scoring_tier IN ('daily','hourly');

COMMENT ON VIEW public.vw_pbi_dogs_beach IS
  'Star-schema center for PBI dog-policy model. One row per active scored beach with consumer surface + evidence layer counts. Join to vw_pbi_dogs_evidence on fid.';

-- ─── 2. Per-BEP-row evidence detail ──────────────────────────────────
CREATE OR REPLACE VIEW public.vw_pbi_dogs_evidence AS
SELECT
  bep.gold_fid                                AS fid,
  bep.source,
  bep.source_url,
  bep.confidence,
  bep.is_canonical,
  bep.extraction_type,
  bep.cpad_role,
  bep.cpad_unit_name,
  -- Surface the most-asked claim fields out of claimed_values JSONB
  -- so PBI users don't have to expand JSON.
  bep.claimed_values->>'dogs_allowed'          AS claim_dogs_allowed,
  bep.claimed_values->>'leash_policy'          AS claim_leash_policy,
  (bep.claimed_values->>'has_off_leash')::boolean AS claim_has_off_leash,
  (bep.claimed_values->>'has_on_leash')::boolean  AS claim_has_on_leash,
  bep.claimed_values->>'dogs_prohibited_start' AS claim_prohibited_start,
  bep.claimed_values->>'dogs_prohibited_end'   AS claim_prohibited_end,
  bep.claimed_values->>'access_rule'           AS claim_access_rule,
  bep.claimed_values                           AS claim_raw_jsonb,
  -- Sentinel detection — sources that codify "no answer found"
  (bep.source ~* '_no_result$') AS is_sentinel
FROM public.beach_enrichment_provenance bep
JOIN public.beaches_gold b ON b.fid = bep.gold_fid
WHERE bep.field_group = 'dogs'
  AND b.is_active AND b.scoring_tier IN ('daily','hourly');

COMMENT ON VIEW public.vw_pbi_dogs_evidence IS
  'Long-form evidence list for dogs field_group. One row per BEP claim. Join to vw_pbi_dogs_beach on fid; join to vw_pbi_dogs_source_tier on source.';

-- ─── 3. Source tier classifier ───────────────────────────────────────
CREATE OR REPLACE VIEW public.vw_pbi_dogs_source_tier AS
WITH dist AS (
  SELECT DISTINCT source FROM public.beach_enrichment_provenance
   WHERE field_group = 'dogs'
)
SELECT
  source,
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
      'Per-beach LLM extractor (Tavily web_search + Sonnet) with cite-required output.'
    ELSE NULL
  END AS source_description
FROM dist;

COMMENT ON VIEW public.vw_pbi_dogs_source_tier IS
  'Small dim joining to vw_pbi_dogs_evidence on source. Classifies every BEP source into 10 pipeline tiers so PBI bar/decomposition visuals group naturally.';

COMMIT;

NOTIFY pgrst, 'reload schema';
