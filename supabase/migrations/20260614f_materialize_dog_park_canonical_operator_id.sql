-- 20260614f_materialize_dog_park_canonical_operator_id.sql
--
-- Port the 2026-06-14 beaches_gold canonical_operator_id materialization
-- (20260614e) to dog_parks_gold per [[paired-functions-port-fixes-both-sides]].
--
-- Adds:
--   1. _resolve_dog_park_operator(p_fid) — mirrors _resolve_beach_operator
--      exactly, but reads from dog_parks_gold instead of beaches_gold.
--      Same 7-tier cascade: federal → CPAD → state → district → local →
--      city → county. Same is_canonical filter.
--   2. dog_parks_gold.canonical_operator_id — materialized result, indexed.
--      Sits ALONGSIDE the existing inferred_operator_id column (which is
--      populated by the OSM-tag-driven Python attribution script,
--      scripts/dog_park_operator_attribution.py, CA/OR/WA only, 17%
--      coverage). The two columns are semantically distinct:
--        - inferred_operator_id   — OSM operator tag + decision tree
--        - canonical_operator_id  — 7-tier spatial cascade
--      Where they agree → confidence. Where they disagree → audit signal.
--   3. tg_compute_dp_canonical_operator AFTER trigger to keep the column
--      in sync on geom INSERT/UPDATE.
--
-- The beach-side migration jumped coverage from 38.8% → 95.7%. The
-- dog-park side starts at 17% via inferred_operator_id; the spatial
-- cascade should hit ~95% for the same reasons (CPAD/PAD-US/TIGER
-- containment is near-universal).

BEGIN;

-- 1. Resolver function -----------------------------------------------------
CREATE OR REPLACE FUNCTION public._resolve_dog_park_operator(p_fid bigint)
RETURNS bigint
LANGUAGE sql STABLE PARALLEL SAFE
AS $function$
  WITH park AS (
    SELECT fid, state, geom FROM public.dog_parks_gold WHERE fid = p_fid
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
  'TIGER city → county. Filters on is_canonical=true at every tier.';

-- 2. Column ----------------------------------------------------------------
ALTER TABLE public.dog_parks_gold
  ADD COLUMN IF NOT EXISTS canonical_operator_id bigint
    REFERENCES public.operators(id) ON DELETE SET NULL;

COMMENT ON COLUMN public.dog_parks_gold.canonical_operator_id IS
  'Materialized result of _resolve_dog_park_operator(fid). 7-tier spatial '
  'cascade answer. Sibling to dog_parks_gold.inferred_operator_id, which '
  'comes from the Python OSM-tag attribution script and uses a different '
  'decision tree. Both columns can be populated; disagreement is an audit '
  'signal.';

CREATE INDEX IF NOT EXISTS dog_parks_gold_canonical_operator_id_idx
  ON public.dog_parks_gold (canonical_operator_id);

-- 3. Trigger function ------------------------------------------------------
CREATE OR REPLACE FUNCTION public._set_canonical_operator_for_dog_park()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE public.dog_parks_gold
     SET canonical_operator_id = public._resolve_dog_park_operator(NEW.fid)
   WHERE fid = NEW.fid
     AND canonical_operator_id IS DISTINCT FROM
         public._resolve_dog_park_operator(NEW.fid);
  RETURN NULL;
END
$$;

-- 4. Trigger wiring --------------------------------------------------------
DROP TRIGGER IF EXISTS tg_compute_dp_canonical_operator ON public.dog_parks_gold;
CREATE TRIGGER tg_compute_dp_canonical_operator
  AFTER INSERT OR UPDATE OF geom ON public.dog_parks_gold
  FOR EACH ROW EXECUTE FUNCTION public._set_canonical_operator_for_dog_park();

-- 5. One-shot backfill -----------------------------------------------------
UPDATE public.dog_parks_gold AS dp
   SET canonical_operator_id = public._resolve_dog_park_operator(dp.fid)
 WHERE dp.is_active;

-- 6. Audit -----------------------------------------------------------------
SELECT
  count(*) AS total_active,
  count(*) FILTER (WHERE canonical_operator_id IS NOT NULL) AS with_canonical,
  count(*) FILTER (WHERE inferred_operator_id IS NOT NULL)  AS with_inferred,
  count(*) FILTER (WHERE canonical_operator_id IS NOT NULL
                     AND inferred_operator_id IS NOT NULL
                     AND canonical_operator_id <> inferred_operator_id) AS disagree,
  round(100.0 * count(*) FILTER (WHERE canonical_operator_id IS NOT NULL)
              / nullif(count(*),0), 1) AS pct_canonical
  FROM public.dog_parks_gold
 WHERE is_active;

COMMIT;

NOTIFY pgrst, 'reload schema';
