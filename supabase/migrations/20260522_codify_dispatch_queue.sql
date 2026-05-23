-- Phase A of cascade-correctness (task #154 / #181): persistent queue for
-- codify dispatch.
--
-- Before: codify_cascade_phase.py writes an ephemeral JSON manifest under
-- tmp/ enumerating which jurisdictions need agent dispatch. Operator reads
-- it manually, dispatches by hand, runs the pipeline again next day, the
-- same queue gets re-derived from scratch — no record of what's in-flight,
-- what's been approved, what failed, or what's been done already.
--
-- After (this migration): cascade_phase UPSERTs into this table. Re-runs
-- are idempotent (UNIQUE constraint on state+kind+target_name). Future
-- phases (B + C, pinned to memory) layer approval workflow + auto-dispatch
-- on top of this same table without schema changes.
--
-- Status flow (see memory pin cascade-correctness-phase-bc-design):
--   pending     ← cascade_phase finds a unit needing dispatch
--   approved    ← operator reviews + approves (Phase B; manual SET for now)
--   dispatched  ← auto-dispatch worker fires the agent (Phase C)
--   completed   ← agent finished + wrote policy_source rows
--   failed      ← agent errored / timed out / refused
--   skipped     ← operator explicitly rejected (with failure_reason)
--
-- Reversibility: DROP TABLE public.codify_dispatch_queue;

BEGIN;

CREATE TABLE public.codify_dispatch_queue (
  id              BIGSERIAL PRIMARY KEY,
  state           TEXT NOT NULL,                                                          -- 2-letter state code
  kind            TEXT NOT NULL                                                           -- entity classifier
                  CHECK (kind IN ('federal_unit', 'jurisdiction_county', 'jurisdiction_city')),
  target_name     TEXT NOT NULL,                                                          -- the unit/jurisdiction name
  target_meta     JSONB NOT NULL DEFAULT '{}'::jsonb,                                     -- mng_name, fips, beaches_in_unit, suggested_cmd, …
  status          TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'approved', 'dispatched',
                                    'completed', 'failed', 'skipped')),
  dispatch_command TEXT,                                                                  -- captured at dispatch time; auditable
  result_meta     JSONB,                                                                  -- agent output summary / ps_id refs / etc.
  failure_reason  TEXT,                                                                   -- populated on failed/skipped
  approved_at     TIMESTAMPTZ,
  approved_by     TEXT,                                                                   -- operator handle / 'cli' / etc.
  dispatched_at   TIMESTAMPTZ,
  completed_at    TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- Idempotency: same unit in same state should produce ONE row, not many.
  -- UPSERT key.
  CONSTRAINT codify_dispatch_queue_target_unique
    UNIQUE (state, kind, target_name)
);

COMMENT ON TABLE public.codify_dispatch_queue IS
  'Persistent queue for codify-cascade dispatch (task #154). cascade_phase '
  'UPSERTs pending rows; operator approves; auto-dispatch worker fires '
  'agents (Phases B + C pinned to memory). Replaces the ephemeral JSON '
  'manifest pattern.';

COMMENT ON COLUMN public.codify_dispatch_queue.status IS
  'Flow: pending → approved → dispatched → completed | failed | skipped. '
  'See memory pin cascade-correctness-phase-bc-design for the full workflow.';

-- Auto-update updated_at on any mutation
CREATE OR REPLACE FUNCTION public.tg_codify_dispatch_queue_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END $$;

CREATE TRIGGER codify_dispatch_queue_updated_at
  BEFORE UPDATE ON public.codify_dispatch_queue
  FOR EACH ROW EXECUTE FUNCTION public.tg_codify_dispatch_queue_updated_at();

-- Index for the common queries
CREATE INDEX codify_dispatch_queue_state_status_idx
  ON public.codify_dispatch_queue (state, status);
CREATE INDEX codify_dispatch_queue_status_idx
  ON public.codify_dispatch_queue (status)
  WHERE status IN ('pending', 'approved');

DO $$
BEGIN
  RAISE NOTICE 'codify_dispatch_queue created';
END $$;

COMMIT;
