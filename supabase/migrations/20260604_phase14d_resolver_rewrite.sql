-- Phase 14d — rewrite _resolve_beach_operator around PAD-US.
--
-- Architecture (decided 2026-06-04):
--
--   1. PAD-US Federal (mng_type='FED', state-scoped, smallest containing)
--   2. CPAD CA enrichment (CA beaches only; richer mng_agncy strings)
--   3. PAD-US State (mng_type='STAT', state-scoped, smallest)
--   4. PAD-US District (mng_type='DIST', state-scoped, smallest)
--   5. PAD-US Local (mng_type='LOC', state-scoped, smallest)
--   6. TIGER City 200m buffer (coastal-edge fallback)
--   7. TIGER County (final fallback)
--
-- Key design choices:
--   - Every PAD-US JOIN carries `p.state = b.state` (Franz: national-scope joins fail).
--   - Match by `mng_name` short code via `pad_us_units.mng_name = ANY(operators.pad_us_mng_name)`.
--   - Phase 14a seeded all federal agency-wide codes (FWS/USACE/DOD/BIA/NRCS/USBR/BOEM/NOAA/TVA)
--     and state-park canonicals (OPRD/WSPRC/DCR/MDDNR) so this resolver can land hits.
--   - State agency match also constrains `op.state_code = b.state` to disambiguate
--     (e.g., MA's DCR vs MD's MDDNR both carry mng_name='SDNR' in their array).
--   - Federal agency match does NOT constrain op.state_code — federal canonicals are
--     vestigially tagged state_code='CA' but apply nationally.
--   - Axis 1 (level hierarchy) is load-bearing. Federal wins over CPAD even on CA.
--   - CPAD enrichment is below federal because CPAD's federal mng_agncy strings
--     are sometimes ambiguous and we trust PAD-US's mng_name codes more for federal.
--
-- Idempotent (CREATE OR REPLACE).

BEGIN;

CREATE OR REPLACE FUNCTION public._resolve_beach_operator(p_fid bigint)
RETURNS bigint
LANGUAGE sql
STABLE PARALLEL SAFE
AS $function$
  WITH beach AS (
    SELECT fid, state, geom FROM public.beaches_gold WHERE fid = p_fid
  ),
  -- ── Layer 1: PAD-US Federal ──────────────────────────────────────────
  -- Match smallest containing federal polygon to canonical agency operator
  -- via mng_name short code (NPS/FWS/USFS/BLM/USACE/DOD/BIA/NRCS/USBR/BOEM/NOAA/TVA).
  via_pad_us_federal AS (
    SELECT op.id
    FROM beach b
    JOIN public.pad_us_units p
      ON p.state = b.state
     AND p.mng_type = 'FED'
     AND ST_Contains(p.geom, b.geom)
    JOIN public.operators op
      ON p.mng_name = ANY(op.pad_us_mng_name)
     AND op.is_canonical = true
     AND op.is_active
    ORDER BY ST_Area(p.geom) ASC
    LIMIT 1
  ),
  -- ── Layer 2: CPAD CA enrichment ──────────────────────────────────────
  -- For CA beaches, CPAD's mng_agncy strings are richer than PAD-US codes.
  -- Smallest containing CPAD polygon with a matching canonical operator.
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
  -- ── Layer 3: PAD-US State (non-CA paths; CA handled by CPAD above) ──
  -- State agencies are state-specific: constrain op.state_code = b.state.
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
    WHERE b.state <> 'CA'  -- CA handled by CPAD layer above
    ORDER BY ST_Area(p.geom) ASC
    LIMIT 1
  ),
  -- ── Layer 4: PAD-US District (regional, special districts) ──────────
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
  -- ── Layer 5: PAD-US Local (city/county/CDP) ─────────────────────────
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
  -- ── Layer 6: TIGER City buffered 200m (coastal-edge fallback) ───────
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
  -- ── Layer 7: TIGER County (final fallback) ──────────────────────────
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
