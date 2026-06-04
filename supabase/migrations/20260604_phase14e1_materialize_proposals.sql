-- Phase 14e.1 — snapshot pre-state + materialize proposed re-resolutions.
--
-- NO entity_operator changes in this migration. Only:
--   1. Permanent snapshot of pre-state (for post-hoc rollback)
--   2. Permanent table of proposed (old_op, new_op) per PIP-system row
--   3. Audit notices showing the shape of changes
--
-- 14e.2 (separate migration) applies the actual UPDATE from the proposed
-- table. The split exists so we can inspect proposed counts BEFORE any
-- entity_operator row is touched.
--
-- Why permanent tables (not temp):
--   - _eo_14e_snapshot is the rollback path if 14e.2 produces surprising
--     consumer-surface effects. Keep around for ~1 month, then drop.
--   - _eo_14e_proposed is read by 14e.2's UPDATE. Temp would die at COMMIT.
--
-- Both tables include `_apply_run` flag so 14e.2 can mark which proposals
-- were actually applied (in case future runs need to re-materialize).

BEGIN;
SET LOCAL statement_timeout = '20min';

-- ─────────────────────────────────────────────────────────────────────
-- 1. Permanent snapshot — pre-state for every entity_operator beach row
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS public._eo_14e_snapshot;
CREATE TABLE public._eo_14e_snapshot AS
SELECT
  entity_id,
  operator_id AS old_op,
  notes AS old_notes,
  now()::timestamptz AS snapshotted_at
FROM public.entity_operator
WHERE entity_type = 'beach';

CREATE INDEX ON public._eo_14e_snapshot (entity_id);

COMMENT ON TABLE public._eo_14e_snapshot IS
  'Phase 14e.1 snapshot of entity_operator beach rows before re-resolution. '
  'Drop after verification (~1 month after Phase 14e.2 apply).';

-- ─────────────────────────────────────────────────────────────────────
-- 2. Permanent proposed table — (entity_id, old_op, new_op) for PIP-system rows
-- ─────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS public._eo_14e_proposed;
CREATE TABLE public._eo_14e_proposed AS
SELECT
  eo.entity_id,
  eo.operator_id AS old_op,
  public._resolve_beach_operator(eo.entity_id) AS new_op,
  now()::timestamptz AS computed_at,
  false AS applied
FROM public.entity_operator eo
WHERE eo.entity_type = 'beach'
  AND (eo.notes LIKE 'PIP-attribution backfill%'
       OR eo.notes LIKE 'Re-resolved by Phase%'
       OR eo.notes LIKE 'Repointed by Phase 14b%');

CREATE INDEX ON public._eo_14e_proposed (entity_id);

COMMENT ON TABLE public._eo_14e_proposed IS
  'Phase 14e.1 proposed re-resolutions. 14e.2 applies the UPDATE from this table. '
  'applied=true after 14e.2 runs. Drop after verification.';

-- ─────────────────────────────────────────────────────────────────────
-- 3. Audit notices
-- ─────────────────────────────────────────────────────────────────────
DO $$
DECLARE
  v_total int; v_will_change int; v_new_null int;
  v_promote int; v_at_noncanon int;
  v_unchanged int;
BEGIN
  SELECT count(*) INTO v_total FROM public._eo_14e_proposed;
  SELECT count(*) INTO v_will_change FROM public._eo_14e_proposed
   WHERE new_op IS NOT NULL AND new_op IS DISTINCT FROM old_op;
  SELECT count(*) INTO v_new_null FROM public._eo_14e_proposed WHERE new_op IS NULL;
  SELECT count(*) INTO v_unchanged FROM public._eo_14e_proposed
   WHERE new_op IS NOT DISTINCT FROM old_op;

  SELECT count(*) INTO v_at_noncanon
    FROM public._eo_14e_proposed p
    JOIN public.operators op ON op.id = p.old_op
   WHERE op.is_canonical IS NOT TRUE;

  SELECT count(*) INTO v_promote
    FROM public._eo_14e_proposed p
    JOIN public.operators op ON op.id = p.old_op
   WHERE p.new_op IS NOT NULL
     AND p.new_op IS DISTINCT FROM p.old_op
     AND op.is_canonical IS NOT TRUE;

  RAISE NOTICE '──────── Phase 14e.1 proposal audit ────────';
  RAISE NOTICE 'PIP-system rows in scope:                  %', v_total;
  RAISE NOTICE 'Will change (new ≠ old, not NULL):         %', v_will_change;
  RAISE NOTICE 'No change (new = old):                     %', v_unchanged;
  RAISE NOTICE 'new_op resolved NULL (will keep old):      %', v_new_null;
  RAISE NOTICE 'Currently at non-canonical operator:       %', v_at_noncanon;
  RAISE NOTICE 'Of changes: promotion to canonical:        %', v_promote;
  RAISE NOTICE '';
  RAISE NOTICE 'No entity_operator rows were modified.';
  RAISE NOTICE 'Run Phase 14e.2 to apply, or DROP _eo_14e_proposed to abort.';
END $$;

COMMIT;
