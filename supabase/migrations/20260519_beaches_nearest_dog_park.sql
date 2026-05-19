-- Phase (d) Part 1 — materialize nearest_dog_park_* on beaches_gold.
--
-- Per Franz 2026-05-19 reframing: every beach detail page surfaces its
-- nearest dog park as an "extend the day" additive option (always shown),
-- with a reframed "better fit today" treatment when tier 1-3 triggers
-- fire (dogs not allowed / closed / dogs_prohibited window active now).
--
-- Materialized columns let the consumer surface render the suggestion
-- with one row lookup instead of a per-request spatial query. Refresh
-- function exists for when dog_parks_gold changes (call it manually for
-- v1; trigger-based refresh deferred until DP catalog grows).
--
-- Same-entity filter: OSM tags Huntington Dog Beach + Rosie's Dog Beach
-- as BOTH leisure=beach (in beaches_gold) AND leisure=dog_park (in
-- dog_parks_gold). Their "nearest DP" would otherwise be themselves at
-- 0m. Filter excludes DPs within 50m of the beach centroid.
--
-- Distance cap: 25 mi (40 km). Beyond that, the DP suggestion isn't
-- actionable — show nothing instead. Beaches with no DP within 25mi
-- get NULL nearest_dog_park_fid (skip DP component on their detail page).

BEGIN;

-- ─── 1. Columns ──────────────────────────────────────────────────────

ALTER TABLE public.beaches_gold
  ADD COLUMN IF NOT EXISTS nearest_dog_park_fid bigint
    REFERENCES public.dog_parks_gold(fid) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS nearest_dog_park_distance_m double precision,
  ADD COLUMN IF NOT EXISTS nearest_dog_park_updated_at timestamptz;

CREATE INDEX IF NOT EXISTS beaches_gold_nearest_dp_idx
  ON public.beaches_gold (nearest_dog_park_fid) WHERE nearest_dog_park_fid IS NOT NULL;

COMMENT ON COLUMN public.beaches_gold.nearest_dog_park_fid IS
  'Materialized nearest dog_parks_gold.fid within 25mi, excluding '
  'same-entity OSM overlaps (DPs within 50m of beach centroid). NULL if '
  'no DP within range. Refresh via refresh_nearest_dog_park_for_beach(fid).';

-- ─── 2. Refresh function (per-beach) ─────────────────────────────────
-- Geometry-KNN prefilter + geography for accurate distance on top 5
-- candidates only. Avoids the geography-index slowness that hit the
-- statement timeout on the first attempt.

CREATE OR REPLACE FUNCTION public.refresh_nearest_dog_park_for_beach(p_fid bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_dp_fid    bigint;
  v_dist_m    double precision;
BEGIN
  WITH b AS (SELECT geom FROM public.beaches_gold WHERE fid = p_fid),
  -- Geometry KNN: fast, uses dog_parks_gold_geom_gist_idx.
  -- Pull 5 candidates so we can skip same-entity OSM overlaps + still
  -- have alternates within the 25mi cap.
  candidates AS (
    SELECT g.fid, g.geom
    FROM public.dog_parks_gold g, b
    WHERE g.is_active = true
      AND g.geom && ST_Expand(b.geom, 0.5)   -- ~30mi bbox prefilter
    ORDER BY g.geom <-> b.geom
    LIMIT 5
  ),
  -- Accurate geography distance + same-entity (>50m) + cap (40000m = 25mi)
  filtered AS (
    SELECT
      c.fid,
      ST_Distance(c.geom::geography, b.geom::geography) AS dist_m
    FROM candidates c, b
    WHERE ST_Distance(c.geom::geography, b.geom::geography) BETWEEN 50 AND 40000
    ORDER BY dist_m
    LIMIT 1
  )
  SELECT fid, dist_m INTO v_dp_fid, v_dist_m FROM filtered;

  UPDATE public.beaches_gold
     SET nearest_dog_park_fid        = v_dp_fid,
         nearest_dog_park_distance_m = v_dist_m,
         nearest_dog_park_updated_at = now()
   WHERE fid = p_fid;
END $$;

COMMENT ON FUNCTION public.refresh_nearest_dog_park_for_beach(bigint) IS
  'Recompute nearest_dog_park_fid + distance for one beach. Excludes '
  'same-entity OSM overlaps (>50m only) + caps at 25mi. Geometry-KNN '
  'prefilter + geography distance on top 5 candidates. Call manually '
  'after dog_parks_gold changes; no automatic trigger yet. Bulk init '
  'via scripts/refresh_nearest_dog_park.py per [[chunked-subprocess]].';

COMMIT;

-- ─── NOTE ────────────────────────────────────────────────────────────
-- Initial population is INTENTIONALLY NOT done in this migration.
-- Per [[chunked-subprocess]] HARD rule, the ~3,156-beach loop runs via
-- scripts/refresh_nearest_dog_park.py which commits per 250 beaches so
-- no single statement can hit the pooler timeout. Run that script after
-- applying this migration.
