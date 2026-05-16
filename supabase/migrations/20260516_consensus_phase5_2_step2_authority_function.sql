-- Consensus engine rewrite — Phase 5.2 step 2: update
-- policy_source_authority() to assign tribal_code → tier 1.
--
-- Step 1 (in the parent _schema_additions file) added the
-- tribal_code value to the policy_source_subtype enum. Postgres
-- requires the enum ADD VALUE to be committed before the new
-- value can be referenced in function bodies, so this update
-- ships as a separate file.

CREATE OR REPLACE FUNCTION public.policy_source_authority(s policy_source_subtype)
RETURNS smallint
LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE s
    -- Tier 1: codified law
    WHEN 'statute'                   THEN 1
    WHEN 'federal_regulation'        THEN 1
    WHEN 'state_regulation'          THEN 1
    WHEN 'state_statute'             THEN 1
    WHEN 'municipal_code'            THEN 1
    WHEN 'special_district_ordinance' THEN 1
    WHEN 'tribal_code'               THEN 1  -- new in Phase 5.2
    -- Tier 2: operating agreements between government entities
    WHEN 'mou'                       THEN 2
    WHEN 'lease_agreement'           THEN 2
    WHEN 'operating_agreement'       THEN 2
    WHEN 'concession_lease'          THEN 2
    -- Tier 3: agency-administrative + court + tribal resolution
    WHEN 'agency_administrative_policy' THEN 3
    WHEN 'superintendents_compendium' THEN 3
    WHEN 'court_ruling'              THEN 3
    WHEN 'tribal_resolution'         THEN 3
    -- Tier 4: operator-published policy
    WHEN 'operator_posted_policy'    THEN 4
    -- Tier 5: low-authority / non-operative
    WHEN 'withdrawn_rulemaking'      THEN 5
    WHEN 'promotional_listing'       THEN 5
    WHEN 'community_attestation'     THEN 5
    WHEN 'news_article'              THEN 5
    -- Tier 6: fallback
    WHEN 'inferred'                  THEN 6
    ELSE 7
  END::smallint
$$;
