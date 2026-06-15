-- 20260614h_bep_operator_id_backfill.sql
--
-- Backfill bep.operator_id on existing dogs BEP rows. Replays each source
-- family's natural operator linkage against current data. Sibling of
-- 20260614g which wires going-forward writes; this fills the historical
-- gap so the 20260614i tiebreak has signal to work with from day one.
--
-- Reversibility: UPDATE bep SET operator_id = NULL
--                 WHERE field_group='dogs'
--                   AND source IN (the seven sources below).

BEGIN;

-- 1. city_policy: re-JOIN via c1_jurisdiction_id
WITH updated AS (
UPDATE public.beach_enrichment_provenance bep
   SET operator_id = op.id
  FROM public.beaches_gold g
  JOIN public.operators op
    ON op.jurisdiction_id = g.c1_jurisdiction_id
   AND op.level = 'city' AND op.is_canonical AND op.is_active
 WHERE bep.gold_fid = g.fid AND bep.field_group = 'dogs'
   AND bep.source = 'city_policy' AND bep.operator_id IS NULL
 RETURNING 1
)
SELECT count(*) AS city_policy_updated FROM updated;

-- 2. county_policy: re-JOIN via county_geoid
WITH updated AS (
UPDATE public.beach_enrichment_provenance bep
   SET operator_id = op.id
  FROM public.beaches_gold g
  JOIN public.operators op
    ON op.county_geoid = g.county_geoid
   AND op.level = 'county' AND op.is_canonical AND op.is_active
 WHERE bep.gold_fid = g.fid AND bep.field_group = 'dogs'
   AND bep.source = 'county_policy' AND bep.operator_id IS NULL
 RETURNING 1
)
SELECT count(*) AS county_policy_updated FROM updated;

-- 3. operator_dogs_policy_v1: claimed_values carries operator_id from the populator
WITH updated AS (
UPDATE public.beach_enrichment_provenance bep
   SET operator_id = (bep.claimed_values->>'operator_id')::bigint
 WHERE bep.field_group = 'dogs'
   AND bep.source = 'operator_dogs_policy_v1'
   AND bep.operator_id IS NULL
   AND bep.claimed_values ? 'operator_id'
 RETURNING 1
)
SELECT count(*) AS operator_dogs_policy_v1_updated FROM updated;

-- 4. operator_policy_exceptions_v1: same shape
WITH updated AS (
UPDATE public.beach_enrichment_provenance bep
   SET operator_id = (bep.claimed_values->>'operator_id')::bigint
 WHERE bep.field_group = 'dogs'
   AND bep.source = 'operator_policy_exceptions_v1'
   AND bep.operator_id IS NULL
   AND bep.claimed_values ? 'operator_id'
 RETURNING 1
)
SELECT count(*) AS operator_policy_exceptions_v1_updated FROM updated;

-- 5a. codified_v1:ps_N — via policy_source.issuing_operator_id (direct)
WITH updated AS (
UPDATE public.beach_enrichment_provenance bep
   SET operator_id = ps.issuing_operator_id
  FROM public.policy_source ps
 WHERE bep.policy_source_id = ps.id
   AND bep.field_group = 'dogs'
   AND bep.source LIKE 'codified_v1:%'
   AND bep.operator_id IS NULL
   AND ps.issuing_operator_id IS NOT NULL
 RETURNING 1
)
SELECT count(*) AS codified_v1_direct_updated FROM updated;

-- 5b. codified_v1:ps_N — via policy_source.issuing_agency_id → agency.name → operators.canonical_name.
--     The schema separates agency_id from operator_id by mutex constraint
--     chk_policy_source_issuer_mutex, but the names refer to the same
--     real-world entity (e.g. agency #422 "Washington State Parks and
--     Recreation Commission" is operator #84045 with the same canonical
--     name). Walk the name to recover the operator_id linkage. Lifts
--     codified coverage from ~16% to ~79%.
WITH updated AS (
UPDATE public.beach_enrichment_provenance bep
   SET operator_id = op.id
  FROM public.policy_source ps
  JOIN public.agency a ON a.id = ps.issuing_agency_id
  JOIN public.operators op
    ON lower(op.canonical_name) = lower(a.name)
   AND op.is_canonical AND op.is_active
 WHERE bep.policy_source_id = ps.id
   AND bep.field_group = 'dogs'
   AND bep.source LIKE 'codified_v1:%'
   AND bep.operator_id IS NULL
   AND ps.issuing_agency_id IS NOT NULL
 RETURNING 1
)
SELECT count(*) AS codified_v1_via_agency_updated FROM updated;

-- 6. cpad_unit_dogs_policy_v1: cpad_units.mng_agncy → operators.canonical_name.
--    BEP stored the unit_name in cpad_unit_name; join back to find the cpad_unit.
WITH updated AS (
UPDATE public.beach_enrichment_provenance bep
   SET operator_id = op.id
  FROM public.cpad_units cu
  JOIN public.operators op
    ON lower(op.canonical_name) = lower(cu.mng_agncy)
   AND op.is_canonical AND op.is_active
 WHERE bep.field_group = 'dogs'
   AND bep.source = 'cpad_unit_dogs_policy_v1'
   AND bep.operator_id IS NULL
   AND cu.unit_name = bep.cpad_unit_name
 RETURNING 1
)
SELECT count(*) AS cpad_unit_dogs_policy_v1_updated FROM updated;

-- 7. pad_us_dogs_policy_v1: via beach_polygon_membership (preserves loader semantics)
WITH updated AS (
UPDATE public.beach_enrichment_provenance bep
   SET operator_id = op.id
  FROM public.beach_polygon_membership m
  JOIN public.beaches_gold g ON g.fid = m.gold_fid
  JOIN public.operators op
    ON m.mng_name = ANY(op.pad_us_mng_name)
   AND op.state_code = g.state
   AND op.is_canonical AND op.is_active
 WHERE bep.gold_fid = m.gold_fid
   AND m.polygon_kind = 'pad_us_unit'
   AND m.match_strength = 4
   AND bep.field_group = 'dogs'
   AND bep.source = 'pad_us_dogs_policy_v1'
   AND bep.operator_id IS NULL
 RETURNING 1
)
SELECT count(*) AS pad_us_dogs_policy_v1_updated FROM updated;

COMMIT;

-- Final summary
SELECT source, count(*) FILTER (WHERE operator_id IS NOT NULL) AS with_operator,
       count(*) AS total,
       round(100.0 * count(*) FILTER (WHERE operator_id IS NOT NULL)
                   / nullif(count(*),0), 1) AS pct
  FROM public.beach_enrichment_provenance
 WHERE field_group='dogs'
   AND source IN ('city_policy','county_policy','operator_dogs_policy_v1',
                  'operator_policy_exceptions_v1','cpad_unit_dogs_policy_v1',
                  'pad_us_dogs_policy_v1')
    OR source LIKE 'codified_v1:%'
 GROUP BY source ORDER BY total DESC LIMIT 20;

NOTIFY pgrst, 'reload schema';
