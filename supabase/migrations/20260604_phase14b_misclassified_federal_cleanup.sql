-- Phase 14b — clean up misclassified `level='federal'` rows.
--
-- 11 rows were tagged level='federal' (origin_source='seed_federal_widened')
-- when they're actually cities/counties or random place names. Survey:
--
--   82481 Crescent City     →  369 City of Crescent City
--   82950 Long Beach        →    7 City of Long Beach
--   82958 Los Angeles       →   80 City of Los Angeles
--   83071 Monterey          →  223 City of Monterey
--   83309 Riverside County  →  488 Riverside County
--   83364 San Diego         →  455 City of San Diego
--   83365 San Diego County  →  515 San Diego County
--   83371 San Francisco     →  335 City of San Francisco
--   83412 Santa Cruz        →   50 City of Santa Cruz
--    8494 Seattle           → 2114 City of Seattle
--   83343 Salmon Creek      → (no canonical counterpart — junk row, deactivate)
--
-- Same dedup pattern as Step 3's LA County fix: parent_operator_id +
-- is_canonical=false. COALESCE(parent_operator_id, id) in downstream
-- callers transparently resolves to the canonical parent.
--
-- 36 entity_operator rows currently point at these IDs (beach attributions
-- written when these were considered canonical "federal" ops). Repoint
-- them to the canonical parents now — Phase 14e's resolver re-run will
-- likely reshape them again, but interim queries should land on canonical IDs.
--
-- Salmon Creek (83343) is a junk row — a place/stream name that shouldn't
-- be an operator at all. Set is_canonical=false + is_active=false. Phase 14e
-- will re-resolve any entity_operator rows pointing at it.
--
-- Idempotent via NULL-aware predicates.

BEGIN;

-- ─────────────────────────────────────────────────────────────────────
-- 1. Mark 10 city/county dupes with parent_operator_id + correct level
-- ─────────────────────────────────────────────────────────────────────
UPDATE public.operators SET
  parent_operator_id = 369, level = 'city', is_canonical = false, updated_at = now()
 WHERE id = 82481 AND (parent_operator_id IS DISTINCT FROM 369 OR is_canonical IS NOT FALSE);

UPDATE public.operators SET
  parent_operator_id = 7, level = 'city', is_canonical = false, updated_at = now()
 WHERE id = 82950 AND (parent_operator_id IS DISTINCT FROM 7 OR is_canonical IS NOT FALSE);

UPDATE public.operators SET
  parent_operator_id = 80, level = 'city', is_canonical = false, updated_at = now()
 WHERE id = 82958 AND (parent_operator_id IS DISTINCT FROM 80 OR is_canonical IS NOT FALSE);

UPDATE public.operators SET
  parent_operator_id = 223, level = 'city', is_canonical = false, updated_at = now()
 WHERE id = 83071 AND (parent_operator_id IS DISTINCT FROM 223 OR is_canonical IS NOT FALSE);

UPDATE public.operators SET
  parent_operator_id = 488, level = 'county', is_canonical = false, updated_at = now()
 WHERE id = 83309 AND (parent_operator_id IS DISTINCT FROM 488 OR is_canonical IS NOT FALSE);

UPDATE public.operators SET
  parent_operator_id = 455, level = 'city', is_canonical = false, updated_at = now()
 WHERE id = 83364 AND (parent_operator_id IS DISTINCT FROM 455 OR is_canonical IS NOT FALSE);

UPDATE public.operators SET
  parent_operator_id = 515, level = 'county', is_canonical = false, updated_at = now()
 WHERE id = 83365 AND (parent_operator_id IS DISTINCT FROM 515 OR is_canonical IS NOT FALSE);

UPDATE public.operators SET
  parent_operator_id = 335, level = 'city', is_canonical = false, updated_at = now()
 WHERE id = 83371 AND (parent_operator_id IS DISTINCT FROM 335 OR is_canonical IS NOT FALSE);

UPDATE public.operators SET
  parent_operator_id = 50, level = 'city', is_canonical = false, updated_at = now()
 WHERE id = 83412 AND (parent_operator_id IS DISTINCT FROM 50 OR is_canonical IS NOT FALSE);

UPDATE public.operators SET
  parent_operator_id = 2114, level = 'city', is_canonical = false, updated_at = now()
 WHERE id = 8494 AND (parent_operator_id IS DISTINCT FROM 2114 OR is_canonical IS NOT FALSE);

-- ─────────────────────────────────────────────────────────────────────
-- 2. Deactivate Salmon Creek (junk row, no canonical counterpart)
-- ─────────────────────────────────────────────────────────────────────
UPDATE public.operators SET
  is_canonical = false, is_active = false, level = 'unknown', updated_at = now()
 WHERE id = 83343 AND (is_canonical IS NOT FALSE OR is_active IS NOT FALSE);

-- ─────────────────────────────────────────────────────────────────────
-- 3. Repoint 36 entity_operator rows pointing at these IDs to canonical parents
-- ─────────────────────────────────────────────────────────────────────
UPDATE public.entity_operator eo
   SET operator_id = COALESCE(op.parent_operator_id, op.id),
       notes = 'Repointed by Phase 14b (misclassified federal cleanup, 2026-06-04)'
  FROM public.operators op
 WHERE op.id = eo.operator_id
   AND eo.entity_type = 'beach'
   AND op.id IN (82481, 82950, 82958, 83071, 83309, 83343, 83364, 83365, 83371, 83412, 8494)
   AND op.parent_operator_id IS NOT NULL;
-- Note: Salmon Creek (83343) has no parent_operator_id, so entity_operator rows
-- pointing at it are skipped here. Phase 14e re-resolves them.

-- ─────────────────────────────────────────────────────────────────────
-- Audit
-- ─────────────────────────────────────────────────────────────────────
DO $$
DECLARE v_still_misclass int; v_eo_orphans int;
BEGIN
  SELECT count(*) INTO v_still_misclass
  FROM public.operators
  WHERE id IN (82481, 82950, 82958, 83071, 83309, 83364, 83365, 83371, 83412, 8494)
    AND (is_canonical IS NOT FALSE OR parent_operator_id IS NULL);

  SELECT count(*) INTO v_eo_orphans
  FROM public.entity_operator eo
  JOIN public.operators op ON op.id = eo.operator_id
  WHERE eo.entity_type = 'beach' AND op.is_canonical IS NOT TRUE
    AND op.parent_operator_id IS NULL;

  RAISE NOTICE '10 city/county dupes still misclassified: %', v_still_misclass;
  RAISE NOTICE 'entity_operator rows pointing at non-canonical no-parent ops: %', v_eo_orphans;

  IF v_still_misclass > 0 THEN
    RAISE EXCEPTION 'Cleanup incomplete: % rows still misclassified', v_still_misclass;
  END IF;
END $$;

COMMIT;
