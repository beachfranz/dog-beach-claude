-- Operator unification — Phase 13: encode "specific > general" rule in
-- policy_source_effective_tier_for_beach.
--
-- Today's tier function ranks by subtype authority only:
--   Tier 0  operator_posted_policy with entity_operator match
--   Tier 1  codified law (statute/federal_regulation/municipal_code/etc.)
--   Tier 3  agency_administrative_policy, superintendents_compendium
--   Tier 4  operator_posted_policy (when entity_operator doesn't match)
--   Tier 5+ low-authority
--   Tier 99 city-preemption sentinel
--
-- Bug Franz called out: a Point Reyes beach gets BOTH the Point Reyes-specific
-- agency_admin policy (Tier 3) AND the NPS-wide operator_posted policy (Tier 0)
-- as evidence. Tier 0 < Tier 3, so the broad NPS-wide rule wins — even though
-- the park-specific rule is the more correct policy for THIS beach.
--
-- Franz's rule: park-specific > system-wide. Specific scope beats broad
-- scope, even when the broad scope has nominally higher authority. The
-- cascade should prefer the policy_source linked to the FEWEST beaches.
--
-- Implementation: add a specificity multiplier to the returned tier.
--   bucket 0: 1 link        → specificity offset = 0
--   bucket 1: 2-10 links    → 10
--   bucket 2: 11-30 links   → 20
--   bucket 3: 31-100 links  → 30
--   bucket 4: 101+ links    → 40
--
-- Effective tier = bucket*10 + base_subtype_tier. The city-preemption
-- sentinel (99) and operator-match (now becomes 0 + bucket) still work.
--
-- For Point Reyes vs NPS-wide:
--   PS 218 (Point Reyes, 53 links → bucket 3): tier 3 base + 30 = 33
--   PS 383 (NPS-wide, 130 links → bucket 4): operator-match tier 0 + 40 = 40
--   → PS 218 (33) beats PS 383 (40). ✓

BEGIN;

CREATE OR REPLACE FUNCTION public.policy_source_effective_tier_for_beach(p_ps_id bigint, p_beach_fid bigint)
RETURNS smallint
LANGUAGE sql
STABLE PARALLEL SAFE
AS $function$
  WITH ps_row AS (
    SELECT id, subtype, issuing_operator_id, citation
      FROM public.policy_source
     WHERE id = p_ps_id
  ),
  beach_row AS (
    SELECT fid, geom, state
      FROM public.beaches_gold
     WHERE fid = p_beach_fid
  ),
  -- Specificity bucket: how many other beaches this policy_source covers.
  -- Bounded subquery (single SELECT against a small table) — fast.
  spec AS (
    SELECT
      CASE
        WHEN n <=   1 THEN 0
        WHEN n <=  10 THEN 1
        WHEN n <=  30 THEN 2
        WHEN n <= 100 THEN 3
        ELSE 4
      END AS bucket
    FROM (
      SELECT count(*) AS n
        FROM public.beach_policy_source bps
       WHERE bps.policy_source_id = p_ps_id
    ) c
  ),
  operator_match AS (
    SELECT 1 AS hit
      FROM ps_row p
      JOIN public.entity_operator eo
        ON eo.entity_type = 'beach'
       AND eo.entity_id   = p_beach_fid
       AND eo.operator_id = p.issuing_operator_id
      JOIN public.operators op
        ON op.id = eo.operator_id
       AND op.is_canonical = true
     WHERE p.subtype = 'operator_posted_policy'
       AND p.issuing_operator_id IS NOT NULL
  ),
  city_preemption AS (
    SELECT 1 AS hit
      FROM ps_row p, beach_row b
     WHERE p.subtype IN ('municipal_code', 'agency_administrative_policy')
       AND p.citation ~  '^[^,(]*\mCounty\M'
       AND p.citation !~ '^(City|Town|Village|Borough)\M'
       AND b.fid <> 8534
       AND EXISTS (
         SELECT 1
           FROM public.jurisdictions_buf200m jb_city
           JOIN public.jurisdictions j_city ON j_city.id = jb_city.id
          WHERE j_city.state = b.state
            AND j_city.funcstat = 'A'
            AND j_city.place_type LIKE 'C%'
            AND ST_Contains(jb_city.geom, b.geom)
       )
  ),
  base_tier AS (
    SELECT CASE
      WHEN EXISTS (SELECT 1 FROM city_preemption) THEN 99::smallint
      WHEN EXISTS (SELECT 1 FROM operator_match)  THEN 0::smallint
      ELSE public.policy_source_authority((SELECT subtype FROM ps_row))
    END AS t
  )
  -- City-preemption keeps its sentinel value (99) — the offset is moot
  -- because anything ranked 99 already loses to anything < 99.
  -- For other tiers: effective = bucket*10 + base.
  SELECT CASE
    WHEN (SELECT t FROM base_tier) = 99 THEN 99::smallint
    ELSE ((SELECT bucket FROM spec) * 10 + (SELECT t FROM base_tier))::smallint
  END;
$function$;

COMMIT;
