-- Phase 14e.2 — apply proposed re-resolutions from _eo_14e_proposed.
--
-- Reads from the table populated by 14e.1. UPDATE-only, NULL-guarded.
-- After this commits, ~198 entity_operator beach rows are updated.
--
-- Rollback path:
--   UPDATE public.entity_operator eo
--      SET operator_id = s.old_op, notes = s.old_notes
--     FROM public._eo_14e_snapshot s
--    WHERE eo.entity_type = 'beach' AND eo.entity_id = s.entity_id;

BEGIN;
SET LOCAL statement_timeout = '5min';

-- ─────────────────────────────────────────────────────────────────────
-- 1. Apply the UPDATE
-- ─────────────────────────────────────────────────────────────────────
UPDATE public.entity_operator eo
   SET operator_id = p.new_op,
       notes = 'Re-resolved by Phase 14e (PAD-US-primary resolver, 2026-06-04)'
  FROM public._eo_14e_proposed p
 WHERE eo.entity_type = 'beach'
   AND eo.entity_id = p.entity_id
   AND p.new_op IS NOT NULL
   AND p.new_op IS DISTINCT FROM eo.operator_id;

-- ─────────────────────────────────────────────────────────────────────
-- 2. Mark proposals as applied
-- ─────────────────────────────────────────────────────────────────────
UPDATE public._eo_14e_proposed p
   SET applied = true
 WHERE p.new_op IS NOT NULL
   AND p.new_op IS DISTINCT FROM p.old_op;

-- ─────────────────────────────────────────────────────────────────────
-- 3. Post-state audit
-- ─────────────────────────────────────────────────────────────────────
DO $$
DECLARE
  v_applied int;
  v_manual_untouched int;
  v_pip_at_new int;
  v_at_noncanonical int;
BEGIN
  SELECT count(*) INTO v_applied FROM public._eo_14e_proposed WHERE applied = true;

  SELECT count(*) INTO v_manual_untouched
    FROM public.entity_operator eo
   WHERE eo.entity_type='beach'
     AND eo.notes NOT LIKE 'PIP-attribution backfill%'
     AND eo.notes NOT LIKE 'Re-resolved by Phase%'
     AND eo.notes NOT LIKE 'Repointed by Phase 14b%';

  SELECT count(*) INTO v_pip_at_new
    FROM public.entity_operator eo
   WHERE eo.entity_type='beach'
     AND eo.notes LIKE 'Re-resolved by Phase 14e%';

  SELECT count(*) INTO v_at_noncanonical
    FROM public.entity_operator eo
    JOIN public.operators op ON op.id = eo.operator_id
   WHERE eo.entity_type='beach' AND op.is_canonical IS NOT TRUE;

  RAISE NOTICE '──────── Phase 14e.2 apply audit ────────';
  RAISE NOTICE 'Rows updated:                              %', v_applied;
  RAISE NOTICE 'Rows now marked Re-resolved by Phase 14e:  %', v_pip_at_new;
  RAISE NOTICE 'Manual-curation rows untouched:            %', v_manual_untouched;
  RAISE NOTICE 'Beach rows still at non-canonical operator: %', v_at_noncanonical;

  -- Sanity bounds
  IF v_applied = 0 THEN
    RAISE NOTICE 'No-op: migration already applied or proposed table empty.';
  ELSIF v_applied > 300 THEN
    RAISE EXCEPTION 'Applied % rows — exceeded expected ~198. Inspect _eo_14e_snapshot to rollback.', v_applied;
  END IF;

  IF v_at_noncanonical > 0 THEN
    RAISE EXCEPTION 'Found % beach attributions at non-canonical operators after apply. Inspect.', v_at_noncanonical;
  END IF;
END $$;

COMMIT;
