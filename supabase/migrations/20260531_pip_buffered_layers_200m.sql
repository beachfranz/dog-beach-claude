-- 20260531_pip_buffered_layers_200m.sql
--
-- Three lean buffered polygon layers (200m) for point-in-polygon use
-- against beach lat/lng. Per [[pip-for-places-uses-200m]] HARD pin —
-- TIGER coastal-city polygons stop at the high-water line, beach pins
-- sit 10-200m on the water side, so a 200m buffer on the source polygon
-- captures coastal-edge beaches that strict ST_Contains would miss.
--
-- Layers:
--   jurisdictions_buf200m   -- TIGER places (C1 layer per Franz vocab)
--   cpad_units_buf200m      -- California Protected Areas Database (CA only)
--   pad_us_units_buf200m    -- USGS PAD-US (all non-CA states)
--
-- Each is a lean two-column table: source PK + buffered geom. Callers
-- JOIN back to the source table by PK for non-geom columns. This avoids
-- column drift; if jurisdictions.name changes, no second source of truth
-- to update.
--
-- ON DELETE CASCADE on the FK keeps the buffered table in sync when source
-- rows are deleted. Inserts and geom UPDATEs to source must be propagated
-- via the refresh function (TIGER/CPAD/PAD-US update infrequently — annual
-- at most — so refresh-on-bulk-load is the natural pattern).
--
-- Buffer applied via geography: ST_Buffer(geom::geography, 200)::geometry
-- (pad_us_units has cached geom_geog and uses that directly to skip the cast).
-- Result is in SRID 4326, geometry type may be MultiPolygon (or Polygon for
-- the simple cases). GIST index added AFTER populate for faster build.
--
-- Substitution pattern (for callers, NOT done in this migration):
--   -- before
--   JOIN jurisdictions j ON ST_DWithin(j.geom, b.geom, 0.0018)  -- ~200m
--   WHERE j.name = 'X' AND j.funcstat = 'A'
--   -- after
--   JOIN jurisdictions_buf200m jb ON ST_Contains(jb.geom, b.geom)
--   JOIN jurisdictions j ON j.id = jb.id
--   WHERE j.name = 'X' AND j.funcstat = 'A'

BEGIN;

-- ─── Layer 1: jurisdictions_buf200m (TIGER places) ────────────────────

CREATE TABLE IF NOT EXISTS public.jurisdictions_buf200m (
  id   integer PRIMARY KEY REFERENCES public.jurisdictions(id) ON DELETE CASCADE,
  geom geometry(MultiPolygon, 4326) NOT NULL
);

COMMENT ON TABLE public.jurisdictions_buf200m IS
  'TIGER places (jurisdictions) with 200m geographic buffer pre-applied. '
  'Use for PIP against beach lat/lng to catch coastal-edge beaches that '
  'sit 10-200m on the water side of the source high-water polygon. '
  'See [[pip-for-places-uses-200m]] pin. Refreshed via refresh_jurisdictions_buf200m().';

CREATE OR REPLACE FUNCTION public.refresh_jurisdictions_buf200m()
 RETURNS bigint
 LANGUAGE plpgsql
AS $$
DECLARE v_n bigint;
BEGIN
  TRUNCATE public.jurisdictions_buf200m;
  INSERT INTO public.jurisdictions_buf200m (id, geom)
  SELECT j.id,
         ST_Multi(ST_Buffer(j.geom::geography, 200)::geometry)::geometry(MultiPolygon, 4326)
    FROM public.jurisdictions j
   WHERE j.geom IS NOT NULL;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  RETURN v_n;
END;
$$;

COMMENT ON FUNCTION public.refresh_jurisdictions_buf200m IS
  'Idempotent rebuild of jurisdictions_buf200m from public.jurisdictions. '
  'Call after bulk TIGER reloads.';

-- ─── Layer 2: cpad_units_buf200m (CA only) ────────────────────────────

CREATE TABLE IF NOT EXISTS public.cpad_units_buf200m (
  unit_id integer PRIMARY KEY REFERENCES public.cpad_units(unit_id) ON DELETE CASCADE,
  geom    geometry(MultiPolygon, 4326) NOT NULL
);

COMMENT ON TABLE public.cpad_units_buf200m IS
  'California Protected Areas Database units with 200m geographic buffer '
  'pre-applied. Use for PIP against beach lat/lng for CA-only park-unit '
  'overlays. Pairs with pad_us_units_buf200m for non-CA states. '
  'Refreshed via refresh_cpad_units_buf200m().';

CREATE OR REPLACE FUNCTION public.refresh_cpad_units_buf200m()
 RETURNS bigint
 LANGUAGE plpgsql
AS $$
DECLARE v_n bigint;
BEGIN
  TRUNCATE public.cpad_units_buf200m;
  INSERT INTO public.cpad_units_buf200m (unit_id, geom)
  SELECT c.unit_id,
         ST_Multi(ST_Buffer(c.geom::geography, 200)::geometry)::geometry(MultiPolygon, 4326)
    FROM public.cpad_units c
   WHERE c.geom IS NOT NULL;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  RETURN v_n;
END;
$$;

COMMENT ON FUNCTION public.refresh_cpad_units_buf200m IS
  'Idempotent rebuild of cpad_units_buf200m from public.cpad_units. '
  'Call after bulk CPAD reloads.';

-- ─── Layer 3: pad_us_units_buf200m (non-CA states) ────────────────────

CREATE TABLE IF NOT EXISTS public.pad_us_units_buf200m (
  unit_id bigint PRIMARY KEY REFERENCES public.pad_us_units(unit_id) ON DELETE CASCADE,
  geom    geometry(MultiPolygon, 4326) NOT NULL
);

COMMENT ON TABLE public.pad_us_units_buf200m IS
  'USGS PAD-US units with 200m geographic buffer pre-applied. Use for PIP '
  'against beach lat/lng for non-CA states (CA uses cpad_units_buf200m). '
  'Refreshed via refresh_pad_us_units_buf200m(). Source ~315k rows; '
  'expect ~minutes for full refresh.';

CREATE OR REPLACE FUNCTION public.refresh_pad_us_units_buf200m()
 RETURNS bigint
 LANGUAGE plpgsql
AS $$
DECLARE v_n bigint;
BEGIN
  TRUNCATE public.pad_us_units_buf200m;
  INSERT INTO public.pad_us_units_buf200m (unit_id, geom)
  SELECT p.unit_id,
         ST_Multi(ST_Buffer(
           ST_Simplify(p.geom, 0.0001)::geography,
           200,
           'quad_segs=4'
         )::geometry)::geometry(MultiPolygon, 4326)
    FROM public.pad_us_units p
   WHERE p.geom IS NOT NULL;
  GET DIAGNOSTICS v_n = ROW_COUNT;
  RETURN v_n;
END;
$$;

COMMENT ON FUNCTION public.refresh_pad_us_units_buf200m IS
  'Idempotent rebuild of pad_us_units_buf200m from public.pad_us_units. '
  'Applies ST_Simplify(0.0001°, ~11m) before the buffer to drop vertex '
  'counts on complex polygons (Yosemite, national forests etc.); uses '
  'quad_segs=4 (vs default 8) on ST_Buffer to halve segment count. '
  '~10-50× faster than precise spheroidal buffer with negligible loss '
  'at the 200m PIP scale. Call after bulk PAD-US reloads.';

COMMIT;

-- ─── Initial populate (outside the structural txn so it does not block
--     other migrations if the populate is long) ────────────────────────

BEGIN;
SELECT public.refresh_jurisdictions_buf200m() AS jurisdictions_n;
SELECT public.refresh_cpad_units_buf200m()    AS cpad_n;
SELECT public.refresh_pad_us_units_buf200m()  AS pad_us_n;
COMMIT;

-- ─── Build GIST indexes after populate (faster than building during INSERT) ─

BEGIN;
CREATE INDEX IF NOT EXISTS jurisdictions_buf200m_geom_gix
  ON public.jurisdictions_buf200m USING GIST (geom);
CREATE INDEX IF NOT EXISTS cpad_units_buf200m_geom_gix
  ON public.cpad_units_buf200m   USING GIST (geom);
CREATE INDEX IF NOT EXISTS pad_us_units_buf200m_geom_gix
  ON public.pad_us_units_buf200m USING GIST (geom);

ANALYZE public.jurisdictions_buf200m;
ANALYZE public.cpad_units_buf200m;
ANALYZE public.pad_us_units_buf200m;
COMMIT;
