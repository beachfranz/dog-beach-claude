-- 20260613e_pbi_dogs_evidence_keys_fix.sql
--
-- Fix the convenience columns on vw_pbi_dogs_evidence. Initial version
-- (20260613d) extracted JSONB keys like 'dogs_allowed' / 'leash_policy'
-- that don't actually exist in the BEP claimed_values payload. The
-- actual frequencies from a survey of all 13,781 dogs BEP rows:
--
--   leash_required     13,574  →  claim_leash_required
--   allowed            13,385  →  claim_allowed
--   source_quote        6,309  →  claim_source_quote (verbatim, useful for audit)
--   default_rule        6,181  →  claim_default_rule
--   off_leash_exists    6,095  →  claim_off_leash_exists (boolean)
--   ordinance_ref       4,747  →  claim_ordinance_ref
--   scope_notes         4,743  →  claim_scope_notes
--   time_windows        3,073  →  claim_time_windows (kept as JSONB)
--   subtype             2,789  →  claim_subtype
--   rules               2,789  →  claim_rules (kept as JSONB)
--   citation            2,789  →  claim_citation
--   notes               2,365  →  claim_notes
--   operator_name       2,016  →  claim_operator_name
--   mng_name            1,532  →  claim_mng_name
--   area_sand             300  →  claim_area_sand (low coverage; useful for HB-style city
--                                                   policies that break out by section)
--   area_water            ~    →  claim_area_water
--
-- Old wishful columns (claim_dogs_allowed, claim_leash_policy, claim_has_off_leash,
-- claim_has_on_leash, claim_prohibited_start, claim_prohibited_end, claim_access_rule)
-- are dropped.

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
  -- Convenience columns extracted from claimed_values JSONB.
  -- Top-frequency keys surfaced as their own columns so PBI users
  -- don't have to expand JSON. Full payload retained as
  -- claim_raw_jsonb for cases the surface columns don't cover.
  bep.claimed_values->>'allowed'              AS claim_allowed,
  bep.claimed_values->>'leash_required'       AS claim_leash_required,
  bep.claimed_values->>'default_rule'         AS claim_default_rule,
  (bep.claimed_values->>'off_leash_exists')::boolean AS claim_off_leash_exists,
  bep.claimed_values->>'source_quote'         AS claim_source_quote,
  bep.claimed_values->>'ordinance_ref'        AS claim_ordinance_ref,
  bep.claimed_values->>'scope_notes'          AS claim_scope_notes,
  bep.claimed_values->>'subtype'              AS claim_subtype,
  bep.claimed_values->>'citation'             AS claim_citation,
  bep.claimed_values->>'notes'                AS claim_notes,
  bep.claimed_values->>'operator_name'        AS claim_operator_name,
  bep.claimed_values->>'mng_name'             AS claim_mng_name,
  bep.claimed_values->>'area_sand'            AS claim_area_sand,
  bep.claimed_values->>'area_water'           AS claim_area_water,
  bep.claimed_values->'time_windows'          AS claim_time_windows,
  bep.claimed_values->'rules'                 AS claim_rules,
  bep.claimed_values                          AS claim_raw_jsonb,
  -- Sentinel detection — sources that codify "no answer found"
  (bep.source ~* '_no_result$') AS is_sentinel
FROM public.beach_enrichment_provenance bep
JOIN public.beaches_gold b ON b.fid = bep.gold_fid
WHERE bep.field_group = 'dogs'
  AND b.is_active AND b.scoring_tier IN ('daily','hourly');

COMMENT ON VIEW public.vw_pbi_dogs_evidence IS
  'Long-form evidence list for dogs field_group. One row per BEP claim. Join to vw_pbi_dogs_beach on fid; join to vw_pbi_dogs_source_tier on source. Convenience columns extracted from top-frequency claimed_values JSONB keys (Jun 2026 survey).';

COMMIT;

NOTIFY pgrst, 'reload schema';
