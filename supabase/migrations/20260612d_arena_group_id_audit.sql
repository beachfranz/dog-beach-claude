-- 20260612d_arena_group_id_audit.sql
--
-- Pure observability: log every change to arena.group_id so we can
-- chase the upstream mystery surfaced by the Carmel forensic — fids
-- 8286 (downtown Carmel Beach) and 8287 (river beach, "Carmel River
-- State Beach") ended up sharing group_id=8286 even though the four
-- documented steps of populate_arena_group_id should not have done it.
--
-- Next time something glues distant beaches into one cluster, the
-- audit row tells us when it happened, what called it (via the
-- app.arena_clustering_active GUC already set by populate_arena_group_id),
-- and which session/user. Never blocks the change.

BEGIN;

CREATE TABLE IF NOT EXISTS public.arena_group_id_audit (
  id                      bigserial PRIMARY KEY,
  fid                     bigint     NOT NULL,
  before_group_id         bigint,
  after_group_id          bigint,
  changed_at              timestamptz NOT NULL DEFAULT now(),
  -- Set by populate_arena_group_id during its run (Step 1 of the fn).
  -- TRUE → change came from the canonical clustering routine.
  -- FALSE/NULL → some other code path (trigger? one-off migration?
  --              manual UPDATE? tg_arena_auto_promote_to_gold?)
  clustering_active       boolean,
  db_user                 text,
  application_name        text,
  -- Trigger nesting depth at fire time. >1 means we're inside another
  -- trigger's chain (cascade from a different change).
  trigger_depth           integer
);

CREATE INDEX IF NOT EXISTS arena_group_id_audit_changed_at_idx
  ON public.arena_group_id_audit (changed_at DESC);
CREATE INDEX IF NOT EXISTS arena_group_id_audit_fid_idx
  ON public.arena_group_id_audit (fid);

-- Trigger fn
CREATE OR REPLACE FUNCTION public._tg_arena_group_id_audit()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO public.arena_group_id_audit (
    fid, before_group_id, after_group_id,
    clustering_active, db_user, application_name, trigger_depth
  ) VALUES (
    NEW.fid, OLD.group_id, NEW.group_id,
    coalesce(current_setting('app.arena_clustering_active', true), '')::text = 'true',
    session_user,    -- built-in function, not a column reference
    current_setting('application_name', true),
    pg_trigger_depth()
  );
  RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS tg_arena_group_id_audit ON public.arena;
CREATE TRIGGER tg_arena_group_id_audit
AFTER UPDATE OF group_id ON public.arena
FOR EACH ROW
WHEN (OLD.group_id IS DISTINCT FROM NEW.group_id)
EXECUTE FUNCTION public._tg_arena_group_id_audit();

COMMIT;
