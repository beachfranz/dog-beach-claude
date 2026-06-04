-- Operator unification — Option B Step 3: promote 28 ops + mark 2 duplicates.
--
-- Criterion (chosen 2026-06-04): operator has ≥1 row in operator_dogs_policy
-- with policy_found=true AND applies_to_all=true. This matches Phase 10's
-- bridge safety boundary — universal-scope policies that correctly apply to
-- every beach the operator is attributed to.
--
-- Survey returned 30 ops matching the criterion. Two are duplicates that
-- carry the same LA County beach-rule URL (beaches.lacounty.gov) as
-- already-canonical op 1322 (Los Angeles County Department of Beaches and
-- Harbors). Those get parent_operator_id=1322 instead of promotion — the
-- cascade resolver and polygon_to_operator both use COALESCE(parent_operator_id, id)
-- so beaches routed to 1236/1324 still resolve to 1322's content.
--
--   1236 (Home Owners Association - Los Angeles County)       → parent=1322
--   1324 (Los Angeles - Department of Parks and Recreation)   → parent=1322
--
-- Net: 28 ops promoted, 2 ops sidelined to their canonical parent.
--
-- Distribution of promotions: OR(9), MA(8), WA(8), CA(2), RI(1). Mix of
-- coastal cities (Cape Meares, Boston Harbor Islands), inland cities
-- (Cambridge, Newton — harmless to promote, no beach attributions follow),
-- and Catalina/Del Norte.
--
-- Downstream chain triggered by this promotion:
--   Step 4: re-resolve entity_operator → coastal beaches attach to newly-canonical ops
--   Step 5: re-run Phase 10 bridge → operator_dogs_policy rows promote to policy_source
--   Step 6: Phase 13a additive linkage for NPS unit-specific policy_sources
--
-- Idempotent: filters skip already-applied state on re-run.

BEGIN;

-- ─────────────────────────────────────────────────────────────────────
-- 1. Mark 2 LA County duplicates with parent_operator_id = 1322
-- ─────────────────────────────────────────────────────────────────────
UPDATE public.operators
   SET parent_operator_id = 1322,
       updated_at = now()
 WHERE id IN (1236, 1324)
   AND parent_operator_id IS DISTINCT FROM 1322;

-- ─────────────────────────────────────────────────────────────────────
-- 2. Promote the other 28 eligible non-canonical operators
-- ─────────────────────────────────────────────────────────────────────
CREATE TEMP TABLE _pre_state AS
SELECT
  (SELECT count(*) FROM public.operators WHERE is_canonical = true) AS canonical_before;

UPDATE public.operators op
   SET is_canonical = true,
       updated_at = now()
 WHERE op.is_canonical IS NOT TRUE
   AND op.id NOT IN (1236, 1324)
   AND op.id IN (
     SELECT DISTINCT odp.operator_id
     FROM public.operator_dogs_policy odp
     WHERE odp.policy_found = true
       AND odp.applies_to_all = true
   );

-- Audit
DO $$
DECLARE
  v_before int;
  v_after int;
  v_promoted int;
BEGIN
  SELECT canonical_before INTO v_before FROM _pre_state;
  SELECT count(*) INTO v_after FROM public.operators WHERE is_canonical = true;
  v_promoted := v_after - v_before;

  RAISE NOTICE 'Canonical operators: % → % (% promoted, expected 28)', v_before, v_after, v_promoted;

  IF v_promoted = 0 THEN
    RAISE NOTICE 'No-op: migration already applied or no eligible rows.';
  ELSIF v_promoted > 50 THEN
    RAISE EXCEPTION 'Promoted % rows — that''s way more than expected (~28). Aborting.', v_promoted;
  END IF;
END $$;

COMMIT;
