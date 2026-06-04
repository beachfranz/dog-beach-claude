-- Operator unification — Phase 5a: drop singular operator table.
--
-- After Phases 1-4 the singular `operator` table has no live consumers:
--   * All 8 FK columns that pointed at it were remapped to plural canonical
--     IDs (Phase 3).
--   * The Tier 0 function joins plural with is_canonical (Phase 4).
--   * No reads from singular `operator` remain in the cascade-critical path.
--
-- One residual FK survives: operators.canonical_operator_id (the bridge
-- column) references singular operator(id). Drop the column — it's been
-- dead infrastructure since Phase 3 (the cascade no longer uses it; the
-- is_canonical flag is the source of truth for canonical status).
--
-- Phase 5b (rename `operators` → `operator` + sweep ~50 SQL function
-- bodies + 17 Python/dbt files) is parked. The end state after Phase 5a:
-- one operator table named `operators` (plural). Confusion is reduced
-- (the singular table is gone) but the plural name persists until 5b ships.

BEGIN;

-- ── 1. Drop the bridge column (its FK target is about to disappear) ──
ALTER TABLE public.operators DROP COLUMN canonical_operator_id;

-- ── 2. Drop the singular operator table ──────────────────────────────
DROP TABLE public.operator;

-- ── 3. Post-condition: only `operators` remains ──────────────────────
DO $$
DECLARE v_n int;
BEGIN
  SELECT count(*) INTO v_n FROM pg_class c
   JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname='public' AND c.relname IN ('operator','operators') AND c.relkind='r';
  IF v_n <> 1 THEN
    RAISE EXCEPTION 'Expected exactly 1 table (operators) after Phase 5a; found %', v_n;
  END IF;
END $$;

COMMIT;
