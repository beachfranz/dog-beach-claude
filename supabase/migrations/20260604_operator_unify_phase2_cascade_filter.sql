-- Operator unification — Phase 2: defensive is_canonical filter on Tier 0.
--
-- Phase 1 added the is_canonical column and tagged 210 plural rows. Phase 2
-- (this file) makes the cascade-critical function explicitly check it.
--
-- Today's `policy_source_effective_tier_for_beach` joins entity_operator and
-- policy_source.issuing_operator_id without referencing operator/operators
-- directly — Tier 0 fires whenever those FK columns happen to match. The FK
-- columns currently point at singular operator IDs, which were canonical by
-- construction. After Phase 3 remaps those FKs to plural IDs, the columns
-- could in principle hold any operator's ID — so adding an explicit
-- is_canonical=true check now is defensive against future writes (e.g.,
-- PIP-based attribution that lands non-canonical rows in entity_operator).
--
-- Surface-level change: a defensive join. Behavioral change: none today
-- (every entity_operator.operator_id today is canonical anyway). Behavioral
-- guarantee tomorrow: Tier 0 only fires on canonical operators.

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

  -- Operator-posted policy for the beach's ACTUAL operator wins tier 0,
  -- BUT only when the operator is is_canonical. Defensive: while
  -- entity_operator's contents today are all canonical (its FK target was
  -- singular `operator`), the unification phases ahead will repoint FKs
  -- to the unified table where non-canonical rows exist. This filter
  -- preserves the Tier-0-is-canonical-only invariant.
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
  )

  SELECT CASE
    WHEN EXISTS (SELECT 1 FROM operator_match)  THEN 0::smallint
    WHEN EXISTS (SELECT 1 FROM city_preemption) THEN 99::smallint
    ELSE public.policy_source_authority((SELECT subtype FROM ps_row))
  END;
$function$;

COMMIT;
