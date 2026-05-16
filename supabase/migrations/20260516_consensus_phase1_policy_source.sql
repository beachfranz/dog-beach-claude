-- Consensus engine rewrite — Phase 1: schema additions, no behavior change.
--
-- Per docs/consensus_engine_rewrite_design.md, this is the first of five
-- staged migrations. Adds the structural foundation for entity-aware
-- consensus (policy_source as a first-class entity, evidence-for links
-- on BEP, beach_policy_source as the m:m rule layer). Existing engine
-- continues to operate unchanged — all new tables empty, all new
-- columns nullable, no triggers or functions modified yet.
--
-- Rollback safety: every object created here is idempotently checked
-- (CREATE TYPE IF NOT EXISTS via DO block; CREATE TABLE IF NOT EXISTS;
-- ADD COLUMN IF NOT EXISTS). Re-running the migration is a no-op.
--
-- Forward path: Phase 2 (seed from walkthroughs) inserts data; Phase 3
-- swaps the rebuild trigger to read from these tables.

-- ─── policy_source_subtype enum ────────────────────────────────────
-- Authority tier is intrinsic to subtype; computed via the
-- policy_source_authority() function defined below.
-- Adding subtype values later requires ALTER TYPE; backward-incompatible
-- if values are removed. Order here matches the design doc's tier
-- definitions but the tier is not encoded in enum order — see the
-- function for the canonical mapping.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'policy_source_subtype') THEN
    CREATE TYPE policy_source_subtype AS ENUM (
      -- Tier 1: codified law
      'statute',
      'federal_regulation',
      'state_regulation',
      'state_statute',
      'municipal_code',
      'special_district_ordinance',
      -- Tier 2: operating agreements between government entities
      'mou',
      'lease_agreement',
      'operating_agreement',
      'concession_lease',
      -- Tier 3: agency-administrative + court + tribal
      'agency_administrative_policy',
      'superintendents_compendium',
      'court_ruling',
      'tribal_resolution',
      -- Tier 4: operator-published policy
      'operator_posted_policy',
      -- Tier 5: low-authority / non-operative
      'withdrawn_rulemaking',
      'promotional_listing',
      'community_attestation',
      'news_article',
      -- Tier 6: fallback
      'inferred',
      'unknown'
    );
  END IF;
END$$;

-- ─── policy_source table ───────────────────────────────────────────
-- First-class entity. Each row represents a single legal/policy
-- instrument with intrinsic authority (via subtype). Extractions
-- point at policy_source rows; the same source can have multiple
-- extraction reads (e.g., several LLM passes against the same PDF).

CREATE TABLE IF NOT EXISTS public.policy_source (
  id              BIGSERIAL PRIMARY KEY,
  subtype         policy_source_subtype NOT NULL,
  citation        TEXT NOT NULL,
  -- agency_id will be a FK to the public.agency entity when that's
  -- created (later phase of law-as-primary-source-ca). For now,
  -- a plain bigint placeholder. NULL is valid (sources can exist
  -- before their issuing agency is fully modeled).
  issuing_agency_id BIGINT,
  scope           TEXT[],
  source_url      TEXT,
  full_text       TEXT,
  effective_date  DATE,
  last_verified   TIMESTAMPTZ,
  parent_citation_id BIGINT REFERENCES public.policy_source(id),
  withdrawn_at    TIMESTAMPTZ,
  metadata        JSONB DEFAULT '{}'::jsonb,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_policy_source_subtype
  ON public.policy_source (subtype);

CREATE INDEX IF NOT EXISTS ix_policy_source_issuing_agency
  ON public.policy_source (issuing_agency_id)
  WHERE issuing_agency_id IS NOT NULL;

-- ─── policy_source_authority() function ────────────────────────────
-- Authority tier 1-6 (lower = stronger). Computed from subtype, not
-- stored as a column, so the mapping is unambiguous and changing
-- the function automatically re-tiers all rows. IMMUTABLE so it's
-- usable in indexes and ORDER BY without performance penalty.

CREATE OR REPLACE FUNCTION public.policy_source_authority(s policy_source_subtype)
RETURNS smallint
LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE s
    WHEN 'statute'                   THEN 1
    WHEN 'federal_regulation'        THEN 1
    WHEN 'state_regulation'          THEN 1
    WHEN 'state_statute'             THEN 1
    WHEN 'municipal_code'            THEN 1
    WHEN 'special_district_ordinance' THEN 1
    WHEN 'mou'                       THEN 2
    WHEN 'lease_agreement'           THEN 2
    WHEN 'operating_agreement'       THEN 2
    WHEN 'concession_lease'          THEN 2
    WHEN 'agency_administrative_policy' THEN 3
    WHEN 'superintendents_compendium' THEN 3
    WHEN 'court_ruling'              THEN 3
    WHEN 'tribal_resolution'         THEN 3
    WHEN 'operator_posted_policy'    THEN 4
    WHEN 'withdrawn_rulemaking'      THEN 5
    WHEN 'promotional_listing'       THEN 5
    WHEN 'community_attestation'     THEN 5
    WHEN 'news_article'              THEN 5
    WHEN 'inferred'                  THEN 6
    ELSE 7
  END::smallint
$$;

-- ─── BEP extensions: policy_source_id + relevance_verified ─────────
-- Evidence-for-source linkage. NULL = legacy extraction not yet
-- mapped to a policy_source (the vast majority on Phase 1 land).
-- relevance_verified gates whether the URL was confirmed to
-- substantiate the claim — defaults FALSE on existing rows;
-- Phase 4 backfills based on presence-of-quote + URL-fanout audit.

ALTER TABLE public.beach_enrichment_provenance
  ADD COLUMN IF NOT EXISTS policy_source_id BIGINT
    REFERENCES public.policy_source(id);

ALTER TABLE public.beach_enrichment_provenance
  ADD COLUMN IF NOT EXISTS relevance_verified BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS ix_bep_policy_source
  ON public.beach_enrichment_provenance (policy_source_id)
  WHERE policy_source_id IS NOT NULL;

-- ─── beach_policy_source table ─────────────────────────────────────
-- The m:m rule layer. Each row: this policy_source speaks to this
-- (beach, section) with this rule. Replaces zone_rules.regions[].sections{}
-- as the source-of-truth; zone_rules JSONB becomes a derived view in
-- Phase 3.
--
-- Primary key (beach_fid, policy_source_id, section) means one source
-- can only have one claim per section per beach. Multiple sources can
-- speak to the same (beach, section) — that's the conflict resolution
-- case the new resolver handles by walking authority tier.

CREATE TABLE IF NOT EXISTS public.beach_policy_source (
  beach_fid        BIGINT NOT NULL REFERENCES public.beaches_gold(fid),
  policy_source_id BIGINT NOT NULL REFERENCES public.policy_source(id),
  section          TEXT   NOT NULL,
  rule             TEXT   NOT NULL,
  rule_modifier    JSONB,
  evidence_verbatim TEXT,
  evidence_url     TEXT,
  extracted_at     TIMESTAMPTZ,
  last_verified    TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (beach_fid, policy_source_id, section)
);

CREATE INDEX IF NOT EXISTS ix_beach_policy_source_beach
  ON public.beach_policy_source (beach_fid);

CREATE INDEX IF NOT EXISTS ix_beach_policy_source_source
  ON public.beach_policy_source (policy_source_id);

-- Authority-ordered lookups will use this combined index. The new
-- resolver walks beach × section by tier:
--   SELECT ... FROM beach_policy_source bps
--   JOIN policy_source ps ON ps.id = bps.policy_source_id
--   WHERE bps.beach_fid = ? AND bps.section = ?
--   ORDER BY policy_source_authority(ps.subtype), ps.effective_date DESC NULLS LAST
--   LIMIT 1
CREATE INDEX IF NOT EXISTS ix_beach_policy_source_beach_section
  ON public.beach_policy_source (beach_fid, section);

-- ─── Comments ──────────────────────────────────────────────────────

COMMENT ON TABLE public.policy_source IS
  'First-class entity for legal/policy instruments (statutes, MOUs, '
  'agency policies, operator-published rules, court rulings, etc.). '
  'Authority tier is intrinsic to subtype via policy_source_authority(). '
  'Phase 1 of consensus engine rewrite, 2026-05-16. See '
  'docs/consensus_engine_rewrite_design.md';

COMMENT ON TABLE public.beach_policy_source IS
  'm:m rule layer. Each row: this policy_source speaks to this '
  '(beach_fid, section) with this rule + evidence. Replaces '
  'zone_rules.regions[].sections{} as source-of-truth in Phase 3.';

COMMENT ON COLUMN public.beach_enrichment_provenance.policy_source_id IS
  'Optional FK identifying the policy_source this extraction is '
  'evidence FOR. NULL on legacy extractions; backfilled in Phase 4.';

COMMENT ON COLUMN public.beach_enrichment_provenance.relevance_verified IS
  'TRUE when the source_url has been confirmed to substantiate the '
  'claim. FALSE for legacy rows without verbatim quote or for '
  'URL-fanout-flagged rows. Phase 4 backfill.';
