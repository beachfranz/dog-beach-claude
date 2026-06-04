-- Phase 14d.1 — exclude NOAA + BOEM from federal layer in _resolve_beach_operator.
--
-- 14e.1's proposal audit revealed 22 wrong beach attributions where coastal
-- beaches fell inside PAD-US polygons tagged NOAA (National Marine Sanctuaries)
-- or BOEM (Outer Continental Shelf parcels). These agencies regulate marine
-- activities (fishing, vessel speed, offshore energy) — NOT land management.
--
-- Per [[mpas-are-footnotes]] convention: marine designations are not
-- consumer-facing beach operators.
--
-- Schema check:
--   NOAA: 19 MPA + 4 UNKE — all marine. Exclude wholesale.
--   BOEM: 101 OCS         — all offshore. Exclude wholesale.
--   USBR: Reclamation Management Areas etc. — land-based. KEEP.
--   BLM:  1 MPA + 1500+ land rows — KEEP at agency level; BLM-MPA is an
--         edge case (2 Sea Ranch beaches at CCNM offshore rocks), not
--         systemically wrong enough to warrant a per-designation filter.
--
-- Patch is surgical: one extra predicate on the federal CTE.

BEGIN;

CREATE OR REPLACE FUNCTION public._resolve_beach_operator(p_fid bigint)
RETURNS bigint
LANGUAGE sql
STABLE PARALLEL SAFE
AS $function$
  WITH beach AS (
    SELECT fid, state, geom FROM public.beaches_gold WHERE fid = p_fid
  ),
  via_pad_us_federal AS (
    SELECT op.id
    FROM beach b
    JOIN public.pad_us_units p
      ON p.state = b.state
     AND p.mng_type = 'FED'
     AND p.mng_name NOT IN ('NOAA', 'BOEM')   -- ★ Phase 14d.1: marine regulators excluded
     AND ST_Contains(p.geom, b.geom)
    JOIN public.operators op
      ON p.mng_name = ANY(op.pad_us_mng_name)
     AND op.is_canonical = true
     AND op.is_active
    ORDER BY ST_Area(p.geom) ASC
    LIMIT 1
  ),
  via_cpad_ca AS (
    SELECT op.id
    FROM beach b
    JOIN public.cpad_units cu
      ON ST_Contains(cu.geom, b.geom)
     AND cu.mng_agncy IS NOT NULL
    JOIN public.operators op
      ON lower(op.canonical_name) = lower(cu.mng_agncy)
     AND op.is_canonical = true
     AND op.is_active
    WHERE b.state = 'CA'
    ORDER BY ST_Area(cu.geom) ASC
    LIMIT 1
  ),
  via_pad_us_state AS (
    SELECT op.id
    FROM beach b
    JOIN public.pad_us_units p
      ON p.state = b.state
     AND p.mng_type = 'STAT'
     AND ST_Contains(p.geom, b.geom)
    JOIN public.operators op
      ON p.mng_name = ANY(op.pad_us_mng_name)
     AND op.state_code = b.state
     AND op.is_canonical = true
     AND op.is_active
    WHERE b.state <> 'CA'
    ORDER BY ST_Area(p.geom) ASC
    LIMIT 1
  ),
  via_pad_us_district AS (
    SELECT op.id
    FROM beach b
    JOIN public.pad_us_units p
      ON p.state = b.state
     AND p.mng_type = 'DIST'
     AND ST_Contains(p.geom, b.geom)
    JOIN public.operators op
      ON p.mng_name = ANY(op.pad_us_mng_name)
     AND op.state_code = b.state
     AND op.is_canonical = true
     AND op.is_active
    WHERE b.state <> 'CA'
    ORDER BY ST_Area(p.geom) ASC
    LIMIT 1
  ),
  via_pad_us_local AS (
    SELECT op.id
    FROM beach b
    JOIN public.pad_us_units p
      ON p.state = b.state
     AND p.mng_type = 'LOC'
     AND ST_Contains(p.geom, b.geom)
    JOIN public.operators op
      ON p.mng_name = ANY(op.pad_us_mng_name)
     AND op.state_code = b.state
     AND op.is_canonical = true
     AND op.is_active
    WHERE b.state <> 'CA'
    ORDER BY ST_Area(p.geom) ASC
    LIMIT 1
  ),
  via_tiger_city AS (
    SELECT op.id
    FROM beach b
    JOIN public.jurisdictions_buf200m jb ON ST_Contains(jb.geom, b.geom)
    JOIN public.jurisdictions j
      ON j.id = jb.id
     AND j.state = b.state
     AND j.place_type IN ('C1','C2','C5','C8','C9','U1','U2')
     AND j.funcstat IN ('A','S')
    JOIN public.operators op
      ON op.state_code = b.state
     AND (lower(op.canonical_name) = 'city of ' || lower(j.name)
          OR lower(op.canonical_name) = 'town of ' || lower(j.name)
          OR lower(op.canonical_name) = lower(j.name))
     AND op.is_canonical = true
     AND op.is_active
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
     AND op.is_active
    LIMIT 1
  )
  SELECT COALESCE(
    (SELECT id FROM via_pad_us_federal),
    (SELECT id FROM via_cpad_ca),
    (SELECT id FROM via_pad_us_state),
    (SELECT id FROM via_pad_us_district),
    (SELECT id FROM via_pad_us_local),
    (SELECT id FROM via_tiger_city),
    (SELECT id FROM via_county)
  );
$function$;

COMMIT;
