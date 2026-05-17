-- TRANSIENT BACKFILL: populate missing precedence-1 beach_agency rows for
-- CA scoreable beaches whose authority is unambiguously identifiable from
-- their existing beach_policy_source rows.
--
-- Context: today's policy_source work brought CA tier-1+2 link coverage to
-- 99% and added 1+2+3 coverage broadly. But the spatial governance resolver
-- (`_resolve_governance_gold`) hasn't run since the new rows were added, so
-- `beach_agency` is stale — only 238 of 427 CA scoreable beaches have a
-- precedence-1 row, despite 417 having policy_source links.
--
-- This migration is a TRANSIENT FIX. The next run of run_state_pipeline.py
-- will overwrite these rows. The structural fix (resolver consults
-- beach_policy_source as fallback) is part of the [[consensus-source-authority]]
-- rewrite design.
--
-- The fix unlocks MVP+ "PASS ALL 5" from ~39% → ~60%+ until the resolver runs.
--
-- Per-beach attribution rule:
--   1. Look at all beach_policy_source rows for the beach.
--   2. Pick the issuing_agency with the HIGHEST-AUTHORITY policy_source row.
--      Authority tier (per [[handoff-consensus-source-authority]]):
--        state_statute > federal_regulation > state_regulation
--        > tribal_code > mou > lease_agreement > operating_agreement
--        > superintendents_compendium > municipal_code
--        > special_district_ordinance > agency_administrative_policy
--        > operator_posted_policy > community_attestation
--        > withdrawn_rulemaking > inferred
--   3. Insert beach_agency p=1 with authority_domain='dog_policy'.
--   4. notes column documents this is a transient ps-fallback attribution.

WITH ranked AS (
  SELECT
    bps.beach_fid,
    ps.issuing_agency_id,
    CASE ps.subtype
      WHEN 'state_statute'                THEN 100
      WHEN 'federal_regulation'           THEN 95
      WHEN 'state_regulation'             THEN 90
      WHEN 'tribal_code'                  THEN 85
      WHEN 'mou'                          THEN 80
      WHEN 'lease_agreement'              THEN 75
      WHEN 'operating_agreement'          THEN 70
      WHEN 'superintendents_compendium'   THEN 65
      WHEN 'municipal_code'               THEN 60
      WHEN 'special_district_ordinance'   THEN 55
      WHEN 'agency_administrative_policy' THEN 40
      WHEN 'operator_posted_policy'       THEN 30
      WHEN 'community_attestation'        THEN 20
      WHEN 'withdrawn_rulemaking'         THEN 10
      WHEN 'inferred'                     THEN  5
      ELSE 0
    END AS authority_score,
    ROW_NUMBER() OVER (
      PARTITION BY bps.beach_fid
      ORDER BY
        CASE ps.subtype
          WHEN 'state_statute'                THEN 100
          WHEN 'federal_regulation'           THEN 95
          WHEN 'state_regulation'             THEN 90
          WHEN 'tribal_code'                  THEN 85
          WHEN 'mou'                          THEN 80
          WHEN 'lease_agreement'              THEN 75
          WHEN 'operating_agreement'          THEN 70
          WHEN 'superintendents_compendium'   THEN 65
          WHEN 'municipal_code'               THEN 60
          WHEN 'special_district_ordinance'   THEN 55
          WHEN 'agency_administrative_policy' THEN 40
          WHEN 'operator_posted_policy'       THEN 30
          WHEN 'community_attestation'        THEN 20
          WHEN 'withdrawn_rulemaking'         THEN 10
          WHEN 'inferred'                     THEN  5
          ELSE 0
        END DESC,
        ps.id ASC  -- deterministic tiebreak
    ) AS rn
  FROM public.beach_policy_source bps
  JOIN public.policy_source ps ON ps.id = bps.policy_source_id
  JOIN public.beaches_gold g ON g.fid = bps.beach_fid
  WHERE g.state = 'CA'
    AND g.is_active = TRUE
    AND g.scoring_tier IN ('daily','hourly')
    AND ps.issuing_agency_id IS NOT NULL
    -- only beaches that currently LACK a precedence-1 beach_agency row
    AND NOT EXISTS (
      SELECT 1 FROM public.beach_agency ba
       WHERE ba.beach_fid = bps.beach_fid AND ba.precedence_rank = 1
    )
)
INSERT INTO public.beach_agency
  (beach_fid, agency_id, authority_domain, precedence_rank, notes)
SELECT
  r.beach_fid,
  r.issuing_agency_id,
  'dog_policy',
  1,
  'TRANSIENT: attributed from policy_source on 2026-05-17 because the spatial governance resolver has not yet been re-run with the new ps data. Next run_state_pipeline.py pass may overwrite. See [[governance-resolver-followups-2026-05-17]] + [[handoff-consensus-source-authority]].'
FROM ranked r
WHERE r.rn = 1
ON CONFLICT (beach_fid, agency_id, authority_domain)
DO NOTHING;
