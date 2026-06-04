-- Operator unification — Phases 6b + 8b + 9b: New England town correction.
--
-- Original Phases 6/8/9 used a city filter `place_type LIKE 'C%' AND funcstat='A'`.
-- This missed all New England towns, which TIGER classifies as `place_type='U1'`
-- with `funcstat='S'` (Statistical). MA has 176 U1 towns + 0 C1 cities — every
-- famous beach town (Brewster, Falmouth, Provincetown, etc.) ended up in
-- the county fallback.
--
-- This combined migration:
--   * 6b — promotes U1/U2 town candidates to is_canonical
--   * 8b — widens _resolve_beach_operator to consider U1/U2 with funcstat IN ('A','S')
--   * 9b — re-resolves the 615 county-attributed beaches that now sit in a town
--
-- NPS / state-parks / military / tribal layers still deferred.

BEGIN;

-- =============================================================================
-- 6b: Promote U1/U2 town candidates to is_canonical
-- =============================================================================
-- Match plural rows by (state, name) with several common canonical-name patterns:
--   "Town of X" — TIGER name X, canonical "Town of X"
--   "City of X" — TIGER name X, canonical "City of X" (used in non-NE)
--   bare "X"     — TIGER name X, canonical just "X"

WITH active AS (
  SELECT fid, state, geom FROM public.beaches_gold
   WHERE is_active AND scoring_tier IN ('hourly','daily')
),
town_cands AS (
  SELECT DISTINCT j.name AS town_name, b.state
    FROM active b
    JOIN public.jurisdictions_buf200m jb ON ST_Contains(jb.geom, b.geom)
    JOIN public.jurisdictions j ON j.id=jb.id
   WHERE j.place_type IN ('U1','U2')
     AND j.funcstat IN ('A','S')
)
UPDATE public.operators p
   SET is_canonical = true
  FROM town_cands tc
 WHERE p.state_code = tc.state
   AND NOT p.is_canonical
   AND (lower(p.canonical_name) = 'town of ' || lower(tc.town_name)
        OR lower(p.canonical_name) = 'city of ' || lower(tc.town_name)
        OR lower(p.canonical_name) = lower(tc.town_name));

-- =============================================================================
-- 8b: Update _resolve_beach_operator to consider U1/U2 with funcstat 'A' or 'S'
-- =============================================================================
-- Priority adjustments:
--   * City/Town tier widened to include U1/U2 (NE towns) + funcstat='S' (which
--     is how TIGER classifies functioning NE towns)
--   * Within the city/town tier, prefer governmental (C1/C2/U1/U2) over CDP (C5)
--     when a beach sits in multiple containing polygons (Cape Cod scenario:
--     beach in both Brewster U1 and Brewster Center C5)

CREATE OR REPLACE FUNCTION public._resolve_beach_operator(p_fid bigint)
RETURNS bigint
LANGUAGE sql
STABLE PARALLEL SAFE
AS $function$
  WITH beach AS (
    SELECT fid, state, geom FROM public.beaches_gold WHERE fid = p_fid
  ),
  via_cpad AS (
    SELECT op.id
    FROM beach b
    JOIN public.cpad_units cu
      ON ST_Contains(cu.geom, b.geom) AND cu.mng_agncy IS NOT NULL
    JOIN public.operators op
      ON lower(op.canonical_name) = lower(cu.mng_agncy)
     AND op.is_canonical = true
    ORDER BY ST_Area(cu.geom) ASC
    LIMIT 1
  ),
  via_city_or_town AS (
    SELECT op.id
    FROM beach b
    JOIN public.jurisdictions_buf200m jb ON ST_Contains(jb.geom, b.geom)
    JOIN public.jurisdictions j
      ON j.id = jb.id
     AND j.place_type IN ('C1','C2','C5','C8','C9','U1','U2')
     AND j.funcstat IN ('A','S')
    JOIN public.operators op
      ON op.state_code = b.state
     AND (lower(op.canonical_name) = 'city of ' || lower(j.name)
          OR lower(op.canonical_name) = 'town of ' || lower(j.name)
          OR lower(op.canonical_name) = lower(j.name))
     AND op.is_canonical = true
    -- Prefer governmental over CDP, then smallest polygon (most specific)
    ORDER BY (CASE WHEN j.place_type IN ('C1','C2','U1','U2') THEN 0 ELSE 1 END),
             ST_Area(j.geom) ASC
    LIMIT 1
  ),
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
    (SELECT id FROM via_city_or_town),
    (SELECT id FROM via_county)
  );
$function$;

-- =============================================================================
-- 9b: Re-resolve Phase-9 backfilled rows where the new resolver returns
--     a different (more specific) operator
-- =============================================================================
-- Preserves the 19 hand-curated rows (they don't match the Phase 9 notes string).

UPDATE public.entity_operator eo
   SET operator_id = public._resolve_beach_operator(eo.entity_id),
       notes = 'Re-resolved by Phase 9b (NE town correction, 2026-06-04)'
 WHERE eo.entity_type = 'beach'
   AND eo.notes LIKE 'PIP-attribution backfill (operator unification phase 9%'
   AND public._resolve_beach_operator(eo.entity_id) IS NOT NULL
   AND public._resolve_beach_operator(eo.entity_id) <> eo.operator_id;

-- Optionally backfill beaches that had NO resolver hit before but might now
INSERT INTO public.entity_operator
  (entity_type, entity_id, operator_id, scope_section, precedence_rank, notes)
SELECT
  'beach'::entity_type, b.fid, public._resolve_beach_operator(b.fid),
  'sand', 1, 'PIP-attribution backfill (NE town fix, Phase 9b, 2026-06-04)'
FROM public.beaches_gold b
WHERE b.is_active
  AND b.scoring_tier IN ('hourly','daily')
  AND public._resolve_beach_operator(b.fid) IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM public.entity_operator eo
     WHERE eo.entity_type='beach' AND eo.entity_id = b.fid
  );

COMMIT;
