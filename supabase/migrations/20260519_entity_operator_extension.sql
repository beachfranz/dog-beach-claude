-- Phase (a) Part 2 — entity_operator polymorphic table (rename + extend beach_operator).
--
-- Per Franz 2026-05-19: "We will definitely be adding more types down the road,
-- going to need a [polymorphic shape]". Lock Option A from session discussion.
--
-- Shape:
--   id              bigserial PRIMARY KEY
--   entity_type     ENUM('beach','dog_park')  -- extensible via ALTER TYPE
--   entity_id       bigint                    -- FID into the table named by entity_type
--   operator_id     bigint REFERENCES operator(id)
--   scope_section   text
--   ...standard precedence/dates/notes
--
-- FK on entity_id is enforced via BEFORE trigger (not declarative) so the
-- column can resolve to multiple tables based on entity_type. Adding a new
-- entity type later = ALTER TYPE entity_type ADD VALUE 'foo' + one CASE
-- arm in the trigger.
--
-- Backwards compatibility:
--   beach_operator (table) is DROPPED and recreated as a read-only VIEW
--   filtering entity_operator WHERE entity_type='beach'. All current callers
--   (walkthrough-editor.html, bridge_operator_to_cascade.py,
--   audit_operator_extractions.py) only SELECT — confirmed by grep — so the
--   view shim is safe. Writes (yesterday's Phase 1/2 migrations) are already
--   applied; new writes go to entity_operator directly.
--
-- Dependencies updated in this migration:
--   - policy_source_effective_tier_for_beach() switches to entity_operator
--     with entity_type='beach' filter. Same behavior, new source.
--
-- After this migration, the consensus chain still works identically; only
-- the table name moves. The cascade functions don't need re-firing because
-- behavior is unchanged.

BEGIN;

-- ─── 0. Pre-flight: confirm row count for the migration ──────────────
DO $$
DECLARE n integer;
BEGIN
  SELECT count(*) INTO n FROM public.beach_operator;
  RAISE NOTICE 'Migrating % rows from beach_operator → entity_operator', n;
END $$;

-- ─── 1. ENUM ─────────────────────────────────────────────────────────

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'entity_type') THEN
    CREATE TYPE public.entity_type AS ENUM ('beach','dog_park');
  END IF;
END $$;

-- ─── 2. New polymorphic table ────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.entity_operator (
  id              bigserial PRIMARY KEY,
  entity_type     public.entity_type NOT NULL,
  entity_id       bigint NOT NULL,
  operator_id     bigint NOT NULL REFERENCES public.operator(id),
  scope_section   text NOT NULL,
  precedence_rank smallint NOT NULL DEFAULT 1,
  start_date      date,
  end_date        date,
  notes           text,
  created_at      timestamptz NOT NULL DEFAULT now(),

  UNIQUE (entity_type, entity_id, operator_id, scope_section)
);

COMMENT ON TABLE public.entity_operator IS
  'Polymorphic operator linkage. Successor to beach_operator (which is now a '
  'read-only VIEW). entity_id resolves to beaches_gold.fid when entity_type=beach, '
  'dog_parks_gold.fid when entity_type=dog_park. Existence enforced via '
  'entity_operator_check_entity_exists trigger (not declarative FK). '
  'Add new entity types via ALTER TYPE entity_type ADD VALUE + trigger CASE arm. '
  'Created 2026-05-19 per Phase 3 dog-park first-class scope.';

CREATE INDEX IF NOT EXISTS ix_entity_operator_entity
  ON public.entity_operator (entity_type, entity_id);
CREATE INDEX IF NOT EXISTS ix_entity_operator_operator
  ON public.entity_operator (operator_id);

-- ─── 3. Existence-check trigger (replaces declarative FK) ────────────

CREATE OR REPLACE FUNCTION public.entity_operator_check_entity_exists()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  CASE NEW.entity_type
    WHEN 'beach' THEN
      IF NOT EXISTS (SELECT 1 FROM public.beaches_gold WHERE fid = NEW.entity_id) THEN
        RAISE EXCEPTION 'entity_operator: beach with fid=% does not exist', NEW.entity_id;
      END IF;
    WHEN 'dog_park' THEN
      IF NOT EXISTS (SELECT 1 FROM public.dog_parks_gold WHERE fid = NEW.entity_id) THEN
        RAISE EXCEPTION 'entity_operator: dog_park with fid=% does not exist', NEW.entity_id;
      END IF;
    ELSE
      RAISE EXCEPTION 'entity_operator: unknown entity_type %', NEW.entity_type;
  END CASE;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tg_entity_operator_check_entity ON public.entity_operator;
CREATE TRIGGER tg_entity_operator_check_entity
  BEFORE INSERT OR UPDATE OF entity_type, entity_id
  ON public.entity_operator
  FOR EACH ROW EXECUTE FUNCTION public.entity_operator_check_entity_exists();

-- ─── 4. Migrate existing 19 rows from beach_operator ─────────────────

INSERT INTO public.entity_operator (
  entity_type, entity_id, operator_id, scope_section,
  precedence_rank, start_date, end_date, notes, created_at
)
SELECT
  'beach'::public.entity_type,
  bo.beach_fid,
  bo.operator_id,
  bo.scope_section,
  bo.precedence_rank,
  bo.start_date,
  bo.end_date,
  bo.notes,
  bo.created_at
FROM public.beach_operator bo
ON CONFLICT (entity_type, entity_id, operator_id, scope_section) DO NOTHING;

-- ─── 5. Drop the table, replace with read-only view ──────────────────

DROP TABLE public.beach_operator;

CREATE VIEW public.beach_operator AS
SELECT
  eo.entity_id     AS beach_fid,
  eo.operator_id,
  eo.scope_section,
  eo.precedence_rank,
  eo.start_date,
  eo.end_date,
  eo.notes,
  eo.created_at
FROM public.entity_operator eo
WHERE eo.entity_type = 'beach';

COMMENT ON VIEW public.beach_operator IS
  'Legacy read-only view over entity_operator filtered to entity_type=beach. '
  'Existed as a table until 2026-05-19; replaced by polymorphic entity_operator '
  'when dog parks became first-class entities. NEW writes should go to '
  'public.entity_operator directly. Confirmed-read-only callers: '
  'admin/walkthrough-editor.html, scripts/bridge_operator_to_cascade.py, '
  'scripts/audit_operator_extractions.py.';

-- ─── 6. Update policy_source_effective_tier_for_beach helper ─────────
-- The function used to query beach_operator; now queries entity_operator
-- with the beach filter. Behavior unchanged.

CREATE OR REPLACE FUNCTION public.policy_source_effective_tier_for_beach(
    p_ps_id bigint, p_beach_fid bigint
) RETURNS smallint
LANGUAGE sql STABLE PARALLEL SAFE AS $$
  SELECT CASE
    WHEN ps.subtype = 'operator_posted_policy'
      AND ps.issuing_operator_id IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM public.entity_operator eo
        WHERE eo.entity_type = 'beach'
          AND eo.entity_id   = p_beach_fid
          AND eo.operator_id = ps.issuing_operator_id
      )
    THEN 0::smallint
    ELSE public.policy_source_authority(ps.subtype)
  END
  FROM public.policy_source ps
  WHERE ps.id = p_ps_id
$$;

COMMENT ON FUNCTION public.policy_source_effective_tier_for_beach(bigint,bigint) IS
  'Returns the effective tier for a (ps, beach) pair. Boosts operator_posted_policy '
  'from tier 4 to tier 0 when an entity_operator link confirms the operator manages '
  'that beach. 2026-05-19: source moved from beach_operator → entity_operator '
  '(entity_type=beach filter); behavior unchanged.';

-- ─── 7. Verification ─────────────────────────────────────────────────

SELECT
  'entity_operator_total'  AS metric, count(*)::text AS val FROM public.entity_operator
UNION ALL SELECT
  'entity_operator_beach', count(*)::text FROM public.entity_operator WHERE entity_type='beach'
UNION ALL SELECT
  'entity_operator_dog_park', count(*)::text FROM public.entity_operator WHERE entity_type='dog_park'
UNION ALL SELECT
  'beach_operator_view_rows', count(*)::text FROM public.beach_operator
UNION ALL SELECT
  'tier_helper_definition_uses_entity_operator',
  CASE WHEN pg_get_functiondef('public.policy_source_effective_tier_for_beach(bigint,bigint)'::regprocedure)
            LIKE '%entity_operator%' THEN 'true' ELSE 'false' END;

COMMIT;
