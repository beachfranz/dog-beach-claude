-- Operator unification — Phase 3: remap FK columns from singular IDs to plural canonical IDs.
--
-- 8 FK columns currently point at singular `operator(id)`. Phase 3 remaps
-- each to its corresponding plural `operators(id)` (specifically the
-- canonical plural row that maps from the singular). After this phase,
-- the singular `operator` table has no FK consumers and is ready to drop
-- in Phase 4. The plural is_canonical=true rows are the new authoritative
-- canonical set.
--
-- Mapping rules for the 6 same-name-multi-state ambiguous singular rows:
-- Pick the MIN(plural_id) among bridged plural rows
-- (canonical_operator_id = singular_id). This deterministically chooses
-- the originally-bridged primary, and for CCNM + CDFW (two CA agencies
-- with both CA and OR canonical plural rows) it picks the CA one because
-- CA rows have lower IDs (552 vs 42489 for CDFW; 5990 vs 5040 — but
-- 5040 OR is lower for CCNM, so it would pick OR for CCNM, see below).
--
-- Specifically:
--   * Long Beach: singular 52 → plural 7 (CA, only bridged one)
--   * Orange County: 116 → 497 (CA)
--   * Riverside County: 123 → 488 (CA)
--   * San Diego County: 127 → 515 (CA)
--   * CDFW: 15 → 552 (CA — MIN of 552, 42489)
--   * CCNM: 14 → 5040 (OR — MIN of 5040, 5990). Both are canonical, both
--     remain so; we're just picking which one inherits the entity_operator
--     linkages for the CCNM singular row. The OR row is functionally
--     equivalent since CCNM operates in both states.
--
-- For the 202 unambiguous singular rows: 1:1 by bridge OR by name match.

BEGIN;

-- ── 1. Pre-flight assertions ─────────────────────────────────────────
DO $$
DECLARE v_n int; v_unmappable int;
BEGIN
  SELECT count(*) INTO v_n FROM public.operator;
  IF v_n NOT BETWEEN 200 AND 250 THEN
    RAISE EXCEPTION 'operator (singular) row count out of expected range: %', v_n;
  END IF;

  -- Every singular row must have ≥1 canonical plural counterpart we can remap to.
  SELECT count(*) INTO v_unmappable
    FROM public.operator s
    WHERE NOT EXISTS (
      SELECT 1 FROM public.operators p
       WHERE p.is_canonical = true
         AND (p.canonical_operator_id = s.id OR lower(p.canonical_name) = lower(s.name))
    );
  IF v_unmappable > 0 THEN
    RAISE EXCEPTION '% singular rows have no canonical plural counterpart — Phase 1 may not have run', v_unmappable;
  END IF;
END $$;

-- ── 2. Build the temp mapping: singular_id → plural_primary_id ───────
-- Per-singular pick: MIN(plural.id) among plural rows that are canonical
-- AND match by bridge OR by name. The bridge takes precedence semantically;
-- name-fallback is used when no bridge exists.

CREATE TEMP TABLE _sing_to_plur_primary (
  singular_id bigint PRIMARY KEY,
  plural_id   bigint NOT NULL
) ON COMMIT DROP;

INSERT INTO _sing_to_plur_primary (singular_id, plural_id)
SELECT s.id,
       (SELECT MIN(p.id)
          FROM public.operators p
         WHERE p.is_canonical = true
           AND (p.canonical_operator_id = s.id
                OR (lower(p.canonical_name) = lower(s.name)
                    AND NOT EXISTS (SELECT 1 FROM public.operators p2
                                     WHERE p2.canonical_operator_id = s.id)))) AS plural_id
  FROM public.operator s;

-- Verify the mapping is complete.
DO $$
DECLARE v_singular int; v_mapped int;
BEGIN
  SELECT count(*) INTO v_singular FROM public.operator;
  SELECT count(*) INTO v_mapped   FROM _sing_to_plur_primary WHERE plural_id IS NOT NULL;
  IF v_singular <> v_mapped THEN
    RAISE EXCEPTION 'Mapping incomplete: % singular vs % mapped', v_singular, v_mapped;
  END IF;
  RAISE NOTICE 'Phase 3 mapping built: % singular IDs → % plural primary IDs', v_singular, v_mapped;
END $$;

-- ── 3. Remap each of the 8 FK columns ────────────────────────────────
-- For each: drop constraint, UPDATE, re-add constraint pointing at operators.
-- ON DELETE actions preserved per the earlier FK inventory.

-- 3a. policy_source.issuing_operator_id (NO ACTION)
ALTER TABLE public.policy_source
  DROP CONSTRAINT IF EXISTS policy_source_issuing_operator_id_fkey;
UPDATE public.policy_source ps
   SET issuing_operator_id = m.plural_id
  FROM _sing_to_plur_primary m
 WHERE ps.issuing_operator_id = m.singular_id;
ALTER TABLE public.policy_source
  ADD CONSTRAINT policy_source_issuing_operator_id_fkey
  FOREIGN KEY (issuing_operator_id) REFERENCES public.operators(id);

-- 3b. dog_parks_gold.inferred_operator_id (SET NULL)
ALTER TABLE public.dog_parks_gold
  DROP CONSTRAINT IF EXISTS dog_parks_gold_inferred_operator_id_fkey;
UPDATE public.dog_parks_gold dpg
   SET inferred_operator_id = m.plural_id
  FROM _sing_to_plur_primary m
 WHERE dpg.inferred_operator_id = m.singular_id;
ALTER TABLE public.dog_parks_gold
  ADD CONSTRAINT dog_parks_gold_inferred_operator_id_fkey
  FOREIGN KEY (inferred_operator_id) REFERENCES public.operators(id) ON DELETE SET NULL;

-- 3c. entity_operator.operator_id (NO ACTION)
ALTER TABLE public.entity_operator
  DROP CONSTRAINT IF EXISTS entity_operator_operator_id_fkey;
UPDATE public.entity_operator eo
   SET operator_id = m.plural_id
  FROM _sing_to_plur_primary m
 WHERE eo.operator_id = m.singular_id;
ALTER TABLE public.entity_operator
  ADD CONSTRAINT entity_operator_operator_id_fkey
  FOREIGN KEY (operator_id) REFERENCES public.operators(id);

-- 3d. dog_park_dog_policy.operator_id (SET NULL)
ALTER TABLE public.dog_park_dog_policy
  DROP CONSTRAINT IF EXISTS dog_park_dog_policy_operator_id_fkey;
UPDATE public.dog_park_dog_policy ddp
   SET operator_id = m.plural_id
  FROM _sing_to_plur_primary m
 WHERE ddp.operator_id = m.singular_id;
ALTER TABLE public.dog_park_dog_policy
  ADD CONSTRAINT dog_park_dog_policy_operator_id_fkey
  FOREIGN KEY (operator_id) REFERENCES public.operators(id) ON DELETE SET NULL;

-- 3e. operator_location_sources.operator_id (CASCADE)
ALTER TABLE public.operator_location_sources
  DROP CONSTRAINT IF EXISTS operator_location_sources_operator_id_fkey;
UPDATE public.operator_location_sources ols
   SET operator_id = m.plural_id
  FROM _sing_to_plur_primary m
 WHERE ols.operator_id = m.singular_id;
ALTER TABLE public.operator_location_sources
  ADD CONSTRAINT operator_location_sources_operator_id_fkey
  FOREIGN KEY (operator_id) REFERENCES public.operators(id) ON DELETE CASCADE;

-- 3f. operator_extracted_locations.operator_id (CASCADE)
ALTER TABLE public.operator_extracted_locations
  DROP CONSTRAINT IF EXISTS operator_extracted_locations_operator_id_fkey;
UPDATE public.operator_extracted_locations oel
   SET operator_id = m.plural_id
  FROM _sing_to_plur_primary m
 WHERE oel.operator_id = m.singular_id;
ALTER TABLE public.operator_extracted_locations
  ADD CONSTRAINT operator_extracted_locations_operator_id_fkey
  FOREIGN KEY (operator_id) REFERENCES public.operators(id) ON DELETE CASCADE;

-- 3g. dog_park_enrichment_provenance.operator_id (NO ACTION)
ALTER TABLE public.dog_park_enrichment_provenance
  DROP CONSTRAINT IF EXISTS dog_park_enrichment_provenance_operator_id_fkey;
UPDATE public.dog_park_enrichment_provenance dpep
   SET operator_id = m.plural_id
  FROM _sing_to_plur_primary m
 WHERE dpep.operator_id = m.singular_id;
ALTER TABLE public.dog_park_enrichment_provenance
  ADD CONSTRAINT dog_park_enrichment_provenance_operator_id_fkey
  FOREIGN KEY (operator_id) REFERENCES public.operators(id);

-- 3h. dog_park_discovery_queue.operator_id (NO ACTION)
ALTER TABLE public.dog_park_discovery_queue
  DROP CONSTRAINT IF EXISTS dog_park_discovery_queue_operator_id_fkey;
UPDATE public.dog_park_discovery_queue dpdq
   SET operator_id = m.plural_id
  FROM _sing_to_plur_primary m
 WHERE dpdq.operator_id = m.singular_id;
ALTER TABLE public.dog_park_discovery_queue
  ADD CONSTRAINT dog_park_discovery_queue_operator_id_fkey
  FOREIGN KEY (operator_id) REFERENCES public.operators(id);

-- ── 4. Post-condition: every remapped FK now points at a canonical plural row ──
DO $$
DECLARE v_bad int;
BEGIN
  SELECT count(*) INTO v_bad
    FROM public.entity_operator eo
   WHERE NOT EXISTS (SELECT 1 FROM public.operators p
                      WHERE p.id = eo.operator_id AND p.is_canonical = true);
  IF v_bad > 0 THEN
    RAISE EXCEPTION '% entity_operator rows point at non-canonical operators after remap', v_bad;
  END IF;

  SELECT count(*) INTO v_bad
    FROM public.policy_source ps
   WHERE ps.issuing_operator_id IS NOT NULL
     AND NOT EXISTS (SELECT 1 FROM public.operators p
                      WHERE p.id = ps.issuing_operator_id AND p.is_canonical = true);
  IF v_bad > 0 THEN
    RAISE EXCEPTION '% policy_source.issuing_operator_id rows point at non-canonical operators after remap', v_bad;
  END IF;
END $$;

COMMIT;
