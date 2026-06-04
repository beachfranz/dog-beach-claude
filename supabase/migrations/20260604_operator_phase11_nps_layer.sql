-- Operator unification — Phase 11: add NPS layer to _resolve_beach_operator.
--
-- pad_us_units has all federal/state/local protected-area polygons (2.8GB).
-- Filtered by mng_name='NPS' gives ~131 beaches covered by NPS units:
--   Cape Cod National Seashore (30) — the unincorporated Barnstable beaches
--   Point Reyes National Seashore (27)
--   Golden Gate NRA (24)
--   Phillip Burton Wilderness (16)
--   Sleeping Bear Dunes (10), Olympic (3), Channel Islands (3), etc.
--
-- New resolver priority:
--   1. NPS (federal protected) — pad_us_units WHERE mng_name='NPS', smallest containing polygon
--   2. CPAD (CA only)
--   3. City/town (with 200m buffer)
--   4. County fallback
--
-- NPS canonical operator (id 1683 'United States National Park Service')
-- already exists with is_canonical=true. Phase 10 already bridged its
-- operator-posted policy (nps.gov/pore/planyourvisit/pets.htm) into
-- policy_source. So these 131 beaches gain Tier 0 immediately on re-resolve.
--
-- Other federal layers (FWS, USFS, BLM, USACE, etc.) deferred — same pattern
-- applies but each needs verifying its canonical operator exists.

BEGIN;

CREATE OR REPLACE FUNCTION public._resolve_beach_operator(p_fid bigint)
RETURNS bigint
LANGUAGE sql
STABLE PARALLEL SAFE
AS $function$
  WITH beach AS (
    SELECT fid, state, geom FROM public.beaches_gold WHERE fid = p_fid
  ),
  -- Priority 1: NPS-managed pad_us_units (smallest containing polygon)
  via_nps AS (
    SELECT op.id
    FROM beach b
    JOIN public.pad_us_units p
      ON ST_Contains(p.geom, b.geom) AND p.mng_name = 'NPS'
    JOIN public.operators op
      ON op.id = 1683  -- United States National Park Service canonical
     AND op.is_canonical = true
    ORDER BY ST_Area(p.geom) ASC
    LIMIT 1
  ),
  -- Priority 2: CPAD (CA protected areas; manager-name match)
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
  -- Priority 3: City/town (200m buffer; governmental > CDP; smallest)
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
    ORDER BY (CASE WHEN j.place_type IN ('C1','C2','U1','U2') THEN 0 ELSE 1 END),
             ST_Area(j.geom) ASC
    LIMIT 1
  ),
  -- Priority 4: County fallback
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
    (SELECT id FROM via_nps),
    (SELECT id FROM via_cpad),
    (SELECT id FROM via_city_or_town),
    (SELECT id FROM via_county)
  );
$function$;

-- Re-resolve all backfilled rows — anything that should now hit NPS
UPDATE public.entity_operator eo
   SET operator_id = public._resolve_beach_operator(eo.entity_id),
       notes = 'Re-resolved by Phase 11 (NPS layer added, 2026-06-04)'
 WHERE eo.entity_type = 'beach'
   AND (eo.notes LIKE 'PIP-attribution backfill%'
        OR eo.notes LIKE 'Re-resolved by Phase%')
   AND public._resolve_beach_operator(eo.entity_id) IS NOT NULL
   AND public._resolve_beach_operator(eo.entity_id) <> eo.operator_id;

COMMIT;
