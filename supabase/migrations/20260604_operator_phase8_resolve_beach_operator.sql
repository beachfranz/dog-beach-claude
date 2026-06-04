-- Operator unification — Phase 8: _resolve_beach_operator(p_fid) function.
--
-- For a given beach, walks the PIP-priority hierarchy and returns the
-- canonical operator_id of the highest-priority match (or NULL).
--
-- Priority order — strongest jurisdiction first:
--   1. CPAD (smallest containing polygon's mng_agncy) — CA only realistically
--   2. City (TIGER place + 200m buffer per [[pip-for-places-uses-200m]])
--   3. County fallback
--
-- Tier 0 invariant: only is_canonical=true operators are returned.
-- A beach with no canonical match returns NULL.
--
-- NPS / CSP / tribal / military layers deferred. NPS lacks geom (only
-- lat/lng). CSP units are operated by CDPR (the unit isn't the operator),
-- so a unit→operator mapping is needed first. Tribal/military are rare
-- on the coastal scoring set.
--
-- This function is read-only and STABLE — safe to call repeatedly.

BEGIN;

CREATE OR REPLACE FUNCTION public._resolve_beach_operator(p_fid bigint)
RETURNS bigint
LANGUAGE sql
STABLE PARALLEL SAFE
AS $function$
  WITH beach AS (
    SELECT fid, state, geom FROM public.beaches_gold WHERE fid = p_fid
  ),
  -- Priority 1: CPAD smallest containing polygon's manager
  via_cpad AS (
    SELECT op.id
    FROM beach b
    JOIN public.cpad_units cu
      ON ST_Contains(cu.geom, b.geom)
     AND cu.mng_agncy IS NOT NULL
    JOIN public.operators op
      ON lower(op.canonical_name) = lower(cu.mng_agncy)
     AND op.is_canonical = true
    ORDER BY ST_Area(cu.geom) ASC
    LIMIT 1
  ),
  -- Priority 2: City via 200m buffer
  via_city AS (
    SELECT op.id
    FROM beach b
    JOIN public.jurisdictions_buf200m jb ON ST_Contains(jb.geom, b.geom)
    JOIN public.jurisdictions j
      ON j.id = jb.id
     AND j.funcstat = 'A'
     AND j.place_type LIKE 'C%'
    JOIN public.operators op
      ON op.state_code = b.state
     AND (lower(op.canonical_name) = 'city of ' || lower(j.name)
          OR lower(op.canonical_name) = lower(j.name))
     AND op.is_canonical = true
    LIMIT 1
  ),
  -- Priority 3: County fallback
  via_county AS (
    SELECT op.id
    FROM beach b
    JOIN public.counties co ON ST_Contains(co.geom, b.geom)
    JOIN public.operators op
      ON op.state_code = b.state
     AND lower(op.canonical_name) = lower(co.name || ' County')
     AND op.is_canonical = true
    LIMIT 1
  )
  SELECT COALESCE(
    (SELECT id FROM via_cpad),
    (SELECT id FROM via_city),
    (SELECT id FROM via_county)
  );
$function$;

COMMENT ON FUNCTION public._resolve_beach_operator(bigint) IS
  'Returns the canonical operator_id for a beach via PIP hierarchy (CPAD → City + 200m buffer → County). NULL if no canonical match. Built in operator-unification Phase 8 (2026-06-04). Used by beach_operator backfill in Phase 9.';

COMMIT;
