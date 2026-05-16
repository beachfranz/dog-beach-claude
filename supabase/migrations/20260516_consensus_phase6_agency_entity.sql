-- Consensus engine rewrite — Phase 6: build the `agency` entity +
-- `beach_agency` m:m join.
--
-- DRAFT — review before applying.
--
-- Per the 2026-05-16 conversation: laws live with agencies; beaches
-- inherit laws from agencies via beach_agency. The agency table has
-- been a placeholder bigint on policy_source.issuing_agency_id
-- since Phase 1; this migration builds the table for real.
--
-- This is the law-side of the entity layer. The operator-side
-- (operator + beach_operator) lands in Phase 6.1 — split because
-- operators have a different shape (no laws; may FK back to an
-- agency when the operator IS the same legal entity).
--
-- After Phase 6:
-- - Agencies exist as first-class entities (cities, counties, states,
--   federal, tribal, military, special districts, departments)
-- - Beaches link to agencies via beach_agency with authority_domain
-- - policy_source.issuing_agency_id gets a real FK (all existing
--   rows are NULL, so the constraint adds cleanly)
-- - Phase 7 (next) will backfill beach_agency from existing
--   c1_jurisdiction_id / county_name / cpad_unit_id / military_lands
--   / tribal_lands signals on beaches_gold
--
-- Idempotency: all object creation guarded by IF NOT EXISTS.

-- ─── 1. agency_type enum ──────────────────────────────────────────

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agency_type') THEN
    CREATE TYPE agency_type AS ENUM (
      -- City-level
      'city',              -- charter or general-law city
      'city_department',   -- e.g., City of LA Public Works
      -- County-level
      'county',            -- a CA county as a whole
      'county_department', -- e.g., LA County Beaches & Harbors, OC Parks,
                           -- OC Health, LA County Fire / Lifeguards
      -- State-level
      'state',             -- "State of California"
      'state_department',  -- e.g., CA DPR, CDFW, State Lands Commission,
                           -- Coastal Commission
      -- Federal civilian
      'federal',           -- federal government (umbrella)
      'federal_department',-- e.g., NPS, USFWS, BLM, BIA, NOAA
      -- Federal military (distinct: 18 USC §1382 + adjacency-only)
      'federal_military',  -- DoD, Navy, Army, Coast Guard
      -- Sovereign nations
      'tribal',            -- sovereign tribal nations under federal Indian law
      -- Regional special districts
      'special_district'   -- EBRPD, regional park districts, etc.
    );
  END IF;
END$$;

-- ─── 2. agency table ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.agency (
  id                BIGSERIAL PRIMARY KEY,
  name              TEXT NOT NULL,
  short_name        TEXT,
  type              agency_type NOT NULL,
  -- For hierarchical agencies: LA County Beaches & Harbors has
  -- parent_agency = LA County. parks.ca.gov (DPR) has parent =
  -- State of California. NPS has parent = United States.
  parent_agency_id  BIGINT REFERENCES public.agency(id),
  web_url           TEXT,
  -- Where the agency's codified law / administrative policy lives.
  -- Examples: 'https://library.municode.com/ca/los_angeles_county',
  -- 'https://www.ecfr.gov/current/title-36',
  -- 'https://www.parks.ca.gov/Dogs'.
  code_archive_url  TEXT,
  -- PIP cross-references: which polygon layer covers this agency's
  -- jurisdiction, plus an optional filter to identify the specific
  -- polygon. Used by Phase 7 backfill to auto-link beaches to
  -- agencies via spatial containment.
  pip_layer         TEXT,
  pip_filter        TEXT,
  -- Default authority scope. Cities typically have dog_policy +
  -- operations + parking + fire. County health depts have
  -- water_quality. State Lands has tidelands. The actual
  -- beach-level relationship is captured per-row in beach_agency
  -- which can narrow or extend this default.
  default_authority_domains TEXT[] DEFAULT '{}',
  metadata          JSONB DEFAULT '{}'::jsonb,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_agency_type ON public.agency (type);
CREATE INDEX IF NOT EXISTS ix_agency_parent ON public.agency (parent_agency_id)
  WHERE parent_agency_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_agency_name ON public.agency (name);
-- Prevent duplicate agencies; (name, type) is sufficient distinction
-- since "City of LA" (city) and "Los Angeles County" (county) are
-- different names.
CREATE UNIQUE INDEX IF NOT EXISTS ux_agency_name_type
  ON public.agency (name, type);

COMMENT ON TABLE public.agency IS
  'First-class entity for government bodies with statutory authority. '
  'Cities, counties, states, federal departments, tribal nations, '
  'special districts, military commands. Laws live on policy_source '
  'rows where issuing_agency_id points at the agency. Beaches '
  'inherit laws from agencies via the beach_agency join. '
  'Phase 6 of consensus engine rewrite, 2026-05-16.';

COMMENT ON COLUMN public.agency.parent_agency_id IS
  'For hierarchical relationships: LA County Beaches & Harbors '
  '(county_department) parent = LA County (county). California '
  'State Parks (state_department) parent = State of California (state).';

COMMENT ON COLUMN public.agency.pip_layer IS
  'Which polygon layer covers this agency''s spatial jurisdiction. '
  'Used by Phase 7 backfill to auto-link beaches via spatial '
  'containment. Examples: jurisdictions (for cities), counties '
  '(for counties), pad_us (for state/federal land), tribal_lands, '
  'military_lands, special_districts.';

-- ─── 3. beach_agency m:m join ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.beach_agency (
  beach_fid         BIGINT NOT NULL REFERENCES public.beaches_gold(fid),
  agency_id         BIGINT NOT NULL REFERENCES public.agency(id),
  -- What domain this agency has authority over AT THIS BEACH.
  -- Examples: 'dog_policy', 'water_quality', 'operations',
  -- 'parking', 'fire', 'water_safety', 'wildlife_protection',
  -- 'tidelands', 'coastal_access', 'enforcement',
  -- 'cultural_protection', 'access_restriction' (military).
  -- One agency can have multiple domains at one beach (separate
  -- rows). Multiple agencies can have the same domain at one
  -- beach — that's where precedence_rank kicks in.
  authority_domain  TEXT NOT NULL,
  -- When multiple agencies claim the same domain (e.g., city +
  -- county on dog_policy at section-split beaches), lower rank
  -- wins canonical resolution. Default 1.
  precedence_rank   SMALLINT NOT NULL DEFAULT 1,
  notes             TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (beach_fid, agency_id, authority_domain)
);

CREATE INDEX IF NOT EXISTS ix_beach_agency_beach
  ON public.beach_agency (beach_fid);
CREATE INDEX IF NOT EXISTS ix_beach_agency_agency
  ON public.beach_agency (agency_id);
CREATE INDEX IF NOT EXISTS ix_beach_agency_domain
  ON public.beach_agency (authority_domain);

COMMENT ON TABLE public.beach_agency IS
  'Beach-to-agency many-to-many. Each row: agency X has '
  'authority_domain Y at beach Z with precedence_rank R. '
  'Phase 6 of consensus engine rewrite, 2026-05-16.';

-- ─── 4. Add FK from policy_source to agency ───────────────────────
-- All existing policy_source rows have issuing_agency_id = NULL
-- (Phase 1/2 left it as a placeholder bigint until the agency
-- table existed). Adding the FK constraint now is safe; future
-- rows can either be NULL or point at a real agency.

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conname = 'fk_policy_source_issuing_agency'
  ) THEN
    ALTER TABLE public.policy_source
      ADD CONSTRAINT fk_policy_source_issuing_agency
      FOREIGN KEY (issuing_agency_id) REFERENCES public.agency(id);
  END IF;
END$$;
