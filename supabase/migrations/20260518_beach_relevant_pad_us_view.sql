-- Filter PAD-US to polygons within 500m of a beach or dog park.
--
-- Per docs/codify_pip_resolver_architecture.md and the
-- pin discussion 2026-05-17: reduces PIP cost by ~99%
-- (90,500 → ~900 polygons across 5 priority states).
--
-- The 500m buffer absorbs beach coord-correction; new beaches added to
-- beaches_gold (or new dog parks to osm_dog_parks) are picked up on
-- REFRESH MATERIALIZED VIEW CONCURRENTLY public.beach_relevant_pad_us.

BEGIN;

-- Bump for this transaction — the spatial cross-join across 178k pad_us
-- × 10k beaches × 6k dog parks takes longer than the default 2-min cap.
SET LOCAL statement_timeout = '30min';

DROP MATERIALIZED VIEW IF EXISTS public.beach_relevant_pad_us;

-- Restructured: do the spatial INNER JOIN once per source (beach + dog_park),
-- collect the unit_ids that match, then build the final view by joining
-- those unit_ids back to pad_us_units. Avoids the expensive WHERE-EXISTS
-- pattern that times out.
CREATE MATERIALIZED VIEW public.beach_relevant_pad_us AS
WITH beach_hits AS (
  SELECT DISTINCT pu.unit_id
  FROM public.pad_us_units pu
  JOIN public.beaches_gold g
    ON g.is_active
   AND ST_DWithin(pu.geom::geography, g.geom::geography, 500)
),
dog_park_hits AS (
  SELECT DISTINCT pu.unit_id
  FROM public.pad_us_units pu
  JOIN public.osm_dog_parks odp
    ON ST_DWithin(pu.geom::geography, odp.geom::geography, 500)
)
SELECT
  pu.*,
  (pu.unit_id IN (SELECT unit_id FROM beach_hits))    AS near_beach,
  (pu.unit_id IN (SELECT unit_id FROM dog_park_hits)) AS near_dog_park
FROM public.pad_us_units pu
WHERE pu.unit_id IN (SELECT unit_id FROM beach_hits)
   OR pu.unit_id IN (SELECT unit_id FROM dog_park_hits);

-- Required for CONCURRENTLY refresh
CREATE UNIQUE INDEX brpu_pk ON public.beach_relevant_pad_us(unit_id);

-- Spatial + filter indexes
CREATE INDEX brpu_geom_gix         ON public.beach_relevant_pad_us USING gist (geom);
CREATE INDEX brpu_state_idx        ON public.beach_relevant_pad_us(state);
CREATE INDEX brpu_mng_type_idx     ON public.beach_relevant_pad_us(mng_type);
CREATE INDEX brpu_own_type_idx     ON public.beach_relevant_pad_us(own_type);
CREATE INDEX brpu_mng_name_idx     ON public.beach_relevant_pad_us(mng_name);
CREATE INDEX brpu_near_beach_idx   ON public.beach_relevant_pad_us(near_beach)    WHERE near_beach;
CREATE INDEX brpu_near_dog_park_idx ON public.beach_relevant_pad_us(near_dog_park) WHERE near_dog_park;

COMMENT ON MATERIALIZED VIEW public.beach_relevant_pad_us IS
  'PAD-US polygons within 500m of a beach (beaches_gold) or dog park (osm_dog_parks). '
  'Reduces PIP cost by ~99% vs full pad_us_units. '
  'Refresh: REFRESH MATERIALIZED VIEW CONCURRENTLY public.beach_relevant_pad_us; '
  'See docs/codify_pip_resolver_architecture.md.';

-- Audit per-state coverage (5 priority states)
SELECT state,
       count(*) AS kept,
       count(*) FILTER (WHERE near_beach AND near_dog_park) AS both,
       count(*) FILTER (WHERE near_beach AND NOT near_dog_park) AS beach_only,
       count(*) FILTER (WHERE NOT near_beach AND near_dog_park) AS dog_park_only
FROM public.beach_relevant_pad_us
WHERE state IN ('CA','WA','OR','MI','MA')
GROUP BY state ORDER BY 1;

COMMIT;
