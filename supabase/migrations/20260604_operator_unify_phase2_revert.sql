-- Roll back Phase 2 — the defensive is_canonical filter was wrong.
--
-- The Phase 2 rewrite joined `public.operators` (plural) on
-- `op.id = entity_operator.operator_id` to check is_canonical. But
-- entity_operator.operator_id today points at SINGULAR operator IDs
-- (FK target is the singular `operator` table; we haven't done the
-- Phase 3 remap yet). Plural and singular happen to overlap on
-- small IDs (both have id=7, id=8, etc.) but the entities aren't
-- the same — the join was finding unrelated plural rows.
--
-- Symptom: Tier 0 silently regressed to Tier 4 for some beaches where
-- the operator_id's value happened to be a non-canonical plural row's id.
--
-- Fix: revert to the original function. The defensive filter is the
-- right idea, but it has to go in AFTER Phase 3 remaps the FK column
-- to plural IDs. Moving it to Phase 4.

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
  operator_match AS (
    SELECT 1 AS hit
      FROM ps_row p, public.entity_operator eo
     WHERE p.subtype = 'operator_posted_policy'
       AND p.issuing_operator_id IS NOT NULL
       AND eo.entity_type = 'beach'
       AND eo.entity_id   = p_beach_fid
       AND eo.operator_id = p.issuing_operator_id
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
  )
  SELECT CASE
    WHEN EXISTS (SELECT 1 FROM operator_match)  THEN 0::smallint
    WHEN EXISTS (SELECT 1 FROM city_preemption) THEN 99::smallint
    ELSE public.policy_source_authority((SELECT subtype FROM ps_row))
  END;
$function$;

COMMIT;
