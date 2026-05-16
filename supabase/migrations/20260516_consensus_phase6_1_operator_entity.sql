-- Consensus engine rewrite — Phase 6.1: build the `operator` entity +
-- `beach_operator` m:m join, and add `issuing_operator_id` to
-- policy_source so operator-issued sources (tier-4 operator_posted_policy,
-- mostly) have a proper issuer attribution.
--
-- DRAFT — review before applying.
--
-- Companion to Phase 6 (agency entity). Operators are the entities that
-- physically run a beach day-to-day; some operators are themselves
-- agencies (City of Long Beach operates Rosie's), some are agency
-- departments (LA County Beaches & Harbors operates Dockweiler), and
-- some are non-governmental (Preservation Society of HDB operates HDB,
-- concessionaires, private clubs).
--
-- Key distinction from agency:
-- - Agencies have STATUTORY authority. They issue laws (tier 1) and
--   admin policy (tier 3).
-- - Operators have OPERATIONAL authority. They run the place. They
--   may issue operator-posted policy (tier 4) but never laws.
-- - The same legal entity can be both (City of LB is agency AND
--   operator at Rosie's). When that's true, operator.agency_id FKs
--   back to the agency row to express the identity.
--
-- For non-gov operators (Preservation Society, conservancies, etc.):
-- - agency_id is NULL.
-- - delegated_authority_via FKs to the tier-2 policy_source row that
--   delegates authority to them (typically a MOU or lease). This
--   captures the legal chain: agency → MOU → operator → posted_policy.
-- - This is the "carve-out" answer from the 2026-05-16 conversation:
--   exceptions live in (a) the agency's law itself (carve-out
--   language in evidence_verbatim), (b) the beach record itself for
--   geographic carve-outs (HDB is its own fid), and (c) the
--   operator's posted_policy with delegated authority chained back
--   through this column.
--
-- Idempotency: all object creation guarded by IF NOT EXISTS / DO blocks.

-- ─── 1. operator_type enum ────────────────────────────────────────

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'operator_type') THEN
    CREATE TYPE operator_type AS ENUM (
      -- Government operators (operator.agency_id should point at the
      -- corresponding agency row when the operator IS the agency or
      -- agency-department itself).
      'city',
      'city_department',
      'county',
      'county_department',
      'state',
      'state_department',
      'federal_department',
      'federal_military',
      'tribal',
      'special_district',
      -- Non-government operators (agency_id is NULL; if they have
      -- operational authority it's delegated via
      -- delegated_authority_via).
      'nonprofit',          -- Preservation Society of HDB,
                            -- Crystal Cove Conservancy, etc.
      'concessionaire',     -- private companies running NPS or DPR
                            -- concession leases
      'private_company',    -- private beach club, resort beach
      'cooperative'         -- joint-management coalitions
    );
  END IF;
END$$;

-- ─── 2. operator table ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.operator (
  id                BIGSERIAL PRIMARY KEY,
  name              TEXT NOT NULL,
  short_name        TEXT,
  type              operator_type NOT NULL,
  -- If the operator IS an agency (or agency department), FK back
  -- to the agency row so we preserve identity. NULL for non-gov
  -- operators.
  agency_id         BIGINT REFERENCES public.agency(id),
  -- For non-gov operators with delegated authority, FK to the
  -- tier-2 policy_source (MOU / lease / operating_agreement /
  -- concession_lease) that delegates authority to them. NULL
  -- when authority is direct (gov operators) or non-existent.
  delegated_authority_via BIGINT REFERENCES public.policy_source(id),
  web_url           TEXT,
  -- Where the operator's posted rules live. The operator's
  -- tier-4 policy_source rows typically have source_url within
  -- this domain.
  rules_url         TEXT,
  metadata          JSONB DEFAULT '{}'::jsonb,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- A non-gov operator without delegated authority is a red flag:
  -- they're running a place without legal grounding. We allow it
  -- (private beach with no lease, etc.) but flag the row shape so
  -- it doesn't slip through silently.
  CONSTRAINT chk_operator_gov_or_delegated CHECK (
    agency_id IS NOT NULL
    OR delegated_authority_via IS NOT NULL
    OR type IN ('private_company', 'cooperative')
  )
);

CREATE INDEX IF NOT EXISTS ix_operator_type ON public.operator (type);
CREATE INDEX IF NOT EXISTS ix_operator_agency
  ON public.operator (agency_id) WHERE agency_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_operator_delegated_via
  ON public.operator (delegated_authority_via)
  WHERE delegated_authority_via IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_operator_name ON public.operator (name);
CREATE UNIQUE INDEX IF NOT EXISTS ux_operator_name_type
  ON public.operator (name, type);

COMMENT ON TABLE public.operator IS
  'First-class entity for beach day-to-day operators. Gov operators '
  'set agency_id back to their agency row. Non-gov operators '
  '(nonprofits, concessionaires) set delegated_authority_via to the '
  'tier-2 MOU/lease policy_source row that grants them authority. '
  'Operators never issue laws — only operator_posted_policy (tier 4). '
  'Phase 6.1 of consensus engine rewrite, 2026-05-16.';

COMMENT ON COLUMN public.operator.agency_id IS
  'For government operators: FK to the agency row (City of LB '
  'operates Rosie''s — agency_id = City of LB''s agency row). '
  'For non-gov operators: NULL.';

COMMENT ON COLUMN public.operator.delegated_authority_via IS
  'For non-gov operators (Preservation Society of HDB, Crystal Cove '
  'Conservancy, concessionaires): FK to the tier-2 policy_source '
  'that delegates operational authority to this operator. Typically '
  'an MOU, lease, operating_agreement, or concession_lease. NULL '
  'for gov operators (authority is direct, not delegated).';

-- ─── 3. beach_operator m:m join ───────────────────────────────────

CREATE TABLE IF NOT EXISTS public.beach_operator (
  beach_fid         BIGINT NOT NULL REFERENCES public.beaches_gold(fid),
  operator_id       BIGINT NOT NULL REFERENCES public.operator(id),
  -- Section scope: which part of the beach this operator runs.
  -- NULL means the whole beach. Section-scoped (Manhattan Beach
  -- pattern: LA County operates sand, City of MB operates Strand
  -- bike path). Values mirror beach_policy_source.section
  -- vocabulary: 'sand', 'walkway', 'parking', 'water', 'pier',
  -- 'grass_lawn', etc.
  scope_section     TEXT,
  -- When multiple operators run the same section (rare),
  -- precedence determines canonical. Default 1.
  precedence_rank   SMALLINT NOT NULL DEFAULT 1,
  start_date        DATE,
  end_date          DATE,
  notes             TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  -- (beach_fid, operator_id, section) composite key. NULL section
  -- treated as a single value via COALESCE; we don't use NULL
  -- twice for the same (beach, operator), so unique-with-coalesce
  -- index serves that.
  PRIMARY KEY (beach_fid, operator_id, scope_section)
);

CREATE INDEX IF NOT EXISTS ix_beach_operator_beach
  ON public.beach_operator (beach_fid);
CREATE INDEX IF NOT EXISTS ix_beach_operator_operator
  ON public.beach_operator (operator_id);

COMMENT ON TABLE public.beach_operator IS
  'Beach-to-operator many-to-many. Each row: operator X runs '
  'section Y of beach Z. scope_section NULL means whole beach. '
  'Phase 6.1 of consensus engine rewrite, 2026-05-16.';

-- ─── 4. Add issuing_operator_id to policy_source ──────────────────
-- Tier-4 operator_posted_policy rows need an issuer attribution
-- that points at the operator (not at an agency, since operators
-- aren't agencies). For consistency: at most one of
-- (issuing_agency_id, issuing_operator_id) is set; in rare cases
-- both could be NULL (legacy / inferred / fallback).

ALTER TABLE public.policy_source
  ADD COLUMN IF NOT EXISTS issuing_operator_id BIGINT
  REFERENCES public.operator(id);

CREATE INDEX IF NOT EXISTS ix_policy_source_issuing_operator
  ON public.policy_source (issuing_operator_id)
  WHERE issuing_operator_id IS NOT NULL;

-- Mutex: at most one of agency/operator can be the issuer for a
-- given row. Both NULL is allowed (legacy / unknown).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conname = 'chk_policy_source_issuer_mutex'
  ) THEN
    ALTER TABLE public.policy_source
      ADD CONSTRAINT chk_policy_source_issuer_mutex CHECK (
        issuing_agency_id IS NULL OR issuing_operator_id IS NULL
      );
  END IF;
END$$;

COMMENT ON COLUMN public.policy_source.issuing_operator_id IS
  'For operator-issued policy_source rows (typically tier-4 '
  'operator_posted_policy): FK to the operator that published the '
  'rules. Mutex with issuing_agency_id (at most one set). Phase '
  '6.1 of consensus engine rewrite, 2026-05-16.';
