-- 20261013_dog_park_operator_point_on_surface.sql
--
-- _resolve_dog_park_operator tested ST_Contains(parent_polygon, park.geom)
-- with the dog park's full polygon. A dog area whose outline crosses the
-- edge of its PAD-US / CPAD unit or TIGER city polygon fails every tier
-- that should match and falls through to a coarser one (usually county).
-- Test ST_PointOnSurface(geom) instead — a point guaranteed to lie inside
-- the dog area. For point geoms it is the point itself, so nothing else
-- changes.
--
-- Function body is otherwise identical to
-- 20260614f_materialize_dog_park_canonical_operator_id.sql. The trigger
-- (tg_compute_dp_canonical_operator) calls the function by name, so it
-- picks this up without re-wiring. The backfill re-resolves active parks
-- and only writes rows whose answer changed.
--
-- Beach side (_resolve_beach_operator) has the same ST_Contains shape but
-- is deliberately NOT changed here: long beach polygons cross boundaries
-- far more often, so that change needs its own before/after review.
--
-- Verify after apply:
--   SELECT state, count(*) FILTER (WHERE canonical_operator_id IS NOT NULL) AS with_op,
--          count(*) AS total
--     FROM public.dog_parks_gold WHERE is_active GROUP BY state ORDER BY state;
-- (run once before applying too, to compare)

BEGIN;

CREATE OR REPLACE FUNCTION public._resolve_dog_park_operator(p_fid bigint)
RETURNS bigint
LANGUAGE sql STABLE PARALLEL SAFE
AS $function$
  WITH park AS (
    -- Point inside the dog area, not the whole shape: ST_Contains(parent,
    -- polygon) fails when a dog area pokes over a park or city edge and the
    -- park then falls through to a coarser tier (often county).
    SELECT fid, state, ST_PointOnSurface(geom) AS geom
      FROM public.dog_parks_gold WHERE fid = p_fid
  ),
  via_pad_us_federal AS (
    SELECT op.id
    FROM park b
    JOIN public.pad_us_units p
      ON p.state = b.state
     AND p.mng_type = 'FED'
     AND p.mng_name NOT IN ('NOAA', 'BOEM')
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
    FROM park b
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
    FROM park b
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
    FROM park b
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
    FROM park b
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
    FROM park b
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
    FROM park b
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

COMMENT ON FUNCTION public._resolve_dog_park_operator(bigint) IS
  'Mirror of _resolve_beach_operator for dog_parks_gold. 7-tier spatial '
  'cascade: federal → CPAD (CA only) → state → district → local → '
  'TIGER city → county. Filters on is_canonical=true at every tier. '
  'Tests ST_PointOnSurface(park.geom) since 20261013.';

UPDATE public.dog_parks_gold AS dp
   SET canonical_operator_id = public._resolve_dog_park_operator(dp.fid)
 WHERE dp.is_active
   AND dp.canonical_operator_id IS DISTINCT FROM public._resolve_dog_park_operator(dp.fid);

COMMIT;
