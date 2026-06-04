-- Operator unification — Phase 4: defensive is_canonical filter on Tier 0.
--
-- Reinstates the rewrite that Phase 2 attempted and rolled back. Now valid
-- because Phase 3 remapped entity_operator.operator_id (and the other FK
-- columns) onto canonical plural IDs. Joining `operators` on those IDs
-- now lands on the intended canonical row.
--
-- Today's behavior: Tier 0 fires on entity_operator linkage alone. After
-- this rewrite: Tier 0 also requires the linked operator to be is_canonical.
-- All existing entity_operator rows currently point at canonical operators
-- (post-Phase 3), so no behavior change today. Guarantees the invariant
-- against future writes: PIP-based attribution that lands non-canonical
-- rows in entity_operator can't silently fire Tier 0.

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
