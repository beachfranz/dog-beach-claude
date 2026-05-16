-- Consensus engine rewrite — Phase 3: entity-aware resolver +
-- operative_status concept + manual_curator zone_rules-preservation.
--
-- DRAFT — NOT YET APPLIED. Review before running.
--
-- Per docs/consensus_engine_rewrite_design.md plus the HBDB
-- non-enforcement discussion 2026-05-16, this phase has FOUR changes
-- combined into one atomic migration:
--
-- 1. NEW operative_status enum on beach_policy_source — captures
--    "on the books but not enforced here" cases. A tier-1 statute
--    that's explicitly non-enforced by a lower-tier MOU is no longer
--    selected as the canonical rule; it's surfaced as supplementary
--    background with its status flagged.
--
-- 2. NEW injector `_zr_inject_from_policy_sources(zr, fid)` walks
--    beach_policy_source for this beach, filters to
--    operative_status = 'operative', picks the highest-authority
--    row per section, writes rule + rich evidence into the section.
--    Includes ALL rows (operative + non-operative) in a
--    supplementary_sources array so the UI can render the full
--    legal picture.
--
-- 3. UPDATED `_promote_zone_rules_for_fid(fid)`:
--    a) Respects beach_dog_policy.source = 'manual_curator' AND
--       non-null zone_rules — returns 'preserved' rather than
--       rebuilding (the Rosie's-style data-destruction fix).
--    b) Adds the new injector LAST in the chain (perimeter →
--       sand_from_policy → sections_from_bep → from_policy_sources).
--       Backward-compatible no-op for beaches without
--       beach_policy_source rows.
--
-- 4. BACKFILL: marks HBDB's HBMC §13.08.070 row as `non_enforced`
--    with status_basis_id pointing at the MOU (the document that
--    records the non-enforcement). After this, HBDB sand resolves
--    to off_leash (tier-2 MOU) instead of on_leash (tier-1 statute).
--    Ground truth, defensible provenance.
--
-- Rollback: function bodies are CREATE OR REPLACE; the prior
-- versions can be restored by re-running the previous migration.
-- The new columns can be DROPPED; the enum DROPPED. No data loss.
-- Idempotent — re-running this file is a no-op.

-- ─── 1. operative_status enum + columns on beach_policy_source ─────

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'operative_status') THEN
    CREATE TYPE operative_status AS ENUM (
      'operative',                  -- this rule applies and is enforced here (default)
      'non_enforced',                -- on the books but explicitly not enforced (per a referenced lower-tier doc)
      'superseded_by_lower_tier',    -- explicitly overridden by a more-specific lower-tier arrangement
      'inactive'                     -- withdrawn / repealed / sunset
    );
  END IF;
END$$;

ALTER TABLE public.beach_policy_source
  ADD COLUMN IF NOT EXISTS operative_status operative_status NOT NULL DEFAULT 'operative';

-- status_basis_id: when status != 'operative', this FK points at the
-- policy_source row that records the status. For HBDB's HBMC row
-- (non_enforced), this points at the MOU. For an inactive statute,
-- this could point at the withdrawal document.
ALTER TABLE public.beach_policy_source
  ADD COLUMN IF NOT EXISTS status_basis_id BIGINT REFERENCES public.policy_source(id);

ALTER TABLE public.beach_policy_source
  ADD COLUMN IF NOT EXISTS status_note TEXT;

COMMENT ON COLUMN public.beach_policy_source.operative_status IS
  'Per-beach-per-section operative state for this policy_source. '
  'Default ''operative''. The resolver picks canonical only from '
  'operative rows; non-operative rows surface as supplementary '
  'background with their status flagged.';

COMMENT ON COLUMN public.beach_policy_source.status_basis_id IS
  'When operative_status != ''operative'', the policy_source that '
  'records the change. E.g., HBDB''s HBMC §13.08.070 row is '
  'non_enforced and status_basis_id points at the HB↔PSHDB MOU '
  'that records the non-enforcement.';

CREATE INDEX IF NOT EXISTS ix_beach_policy_source_operative
  ON public.beach_policy_source (beach_fid, section)
  WHERE operative_status = 'operative';

-- ─── 2. New injector: _zr_inject_from_policy_sources ───────────────

CREATE OR REPLACE FUNCTION public._zr_inject_from_policy_sources(p_zr jsonb, p_fid bigint)
RETURNS jsonb
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  v_zr jsonb := p_zr;
  v_regs jsonb;
  v_section_data jsonb;
  v_rec record;
BEGIN
  IF v_zr IS NULL OR p_fid IS NULL THEN
    RETURN v_zr;
  END IF;

  -- For each section at this beach, find the highest-authority
  -- OPERATIVE row. Non-operative rows are excluded from canonical
  -- selection but included in the supplementary_sources array below.
  FOR v_rec IN
    SELECT DISTINCT ON (bps.section)
      bps.section,
      bps.rule,
      bps.rule_modifier,
      bps.evidence_verbatim,
      bps.evidence_url,
      ps.subtype,
      ps.citation,
      ps.source_url AS source_url_canonical,
      policy_source_authority(ps.subtype) AS tier,
      bps.policy_source_id AS canonical_policy_source_id,
      -- All sources at this section (operative + non-operative,
      -- excluding the canonical), with status flags. Ordered by
      -- tier so the drawer renders highest-authority background
      -- first.
      (
        SELECT jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
          'rule', bps2.rule,
          'tier', policy_source_authority(ps2.subtype),
          'subtype', ps2.subtype::text,
          'citation', ps2.citation,
          'quote', bps2.evidence_verbatim,
          'source_url', bps2.evidence_url,
          'operative_status', bps2.operative_status::text,
          'status_note', bps2.status_note,
          'status_basis_citation', basis.citation
        )) ORDER BY policy_source_authority(ps2.subtype))
        FROM public.beach_policy_source bps2
        JOIN public.policy_source ps2 ON ps2.id = bps2.policy_source_id
        LEFT JOIN public.policy_source basis ON basis.id = bps2.status_basis_id
        WHERE bps2.beach_fid = bps.beach_fid
          AND bps2.section = bps.section
          AND bps2.policy_source_id <> bps.policy_source_id
      ) AS supplementary
    FROM public.beach_policy_source bps
    JOIN public.policy_source ps ON ps.id = bps.policy_source_id
    WHERE bps.beach_fid = p_fid
      AND bps.operative_status = 'operative'
    ORDER BY
      bps.section,
      policy_source_authority(ps.subtype) ASC,
      ps.effective_date DESC NULLS LAST,
      bps.last_verified DESC NULLS LAST
  LOOP
    v_section_data := jsonb_strip_nulls(jsonb_build_object(
      'rule', v_rec.rule,
      'evidence', jsonb_strip_nulls(jsonb_build_object(
        'quote', v_rec.evidence_verbatim,
        'source', v_rec.subtype::text,
        'citation', v_rec.citation,
        'source_url', v_rec.evidence_url,
        'authority_tier', v_rec.tier
      )),
      'modifier', v_rec.rule_modifier,
      'supplementary_sources', v_rec.supplementary
    ));

    IF v_zr ? 'regions' AND jsonb_typeof(v_zr->'regions') = 'array' THEN
      v_regs := public._zr_ensure_unnamed_region(v_zr->'regions');
    ELSE
      v_regs := jsonb_build_array(
        jsonb_build_object('name', NULL, 'sections', '{}'::jsonb)
      );
    END IF;

    v_regs := (
      SELECT jsonb_agg(
        CASE
          WHEN (r->>'name') IS NULL THEN
            jsonb_set(r, ARRAY['sections', v_rec.section], v_section_data, true)
          ELSE r
        END
      )
      FROM jsonb_array_elements(v_regs) r
    );

    v_zr := jsonb_set(v_zr, '{regions}', v_regs);
  END LOOP;

  RETURN v_zr;
END $$;

COMMENT ON FUNCTION public._zr_inject_from_policy_sources(jsonb, bigint) IS
  'Phase 3 (2026-05-16): inject section rules from beach_policy_source '
  'rows where operative_status = ''operative''. Picks highest-authority '
  'tier per section; embeds supplementary sources (operative + non-'
  'operative with status flags) for the rendering drawer. Runs LAST in '
  '_promote_zone_rules_for_fid so it wins over legacy BEP-derived '
  'sections. No-op for beaches without beach_policy_source rows.';

-- ─── 3. Updated _promote_zone_rules_for_fid ────────────────────────

CREATE OR REPLACE FUNCTION public._promote_zone_rules_for_fid(p_fid bigint)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
  v_zr jsonb;
  v_source text;
  v_existing_zr jsonb;
BEGIN
  IF p_fid IS NULL THEN RETURN 'noop (null fid)'; END IF;

  SELECT source, zone_rules INTO v_source, v_existing_zr
    FROM public.beach_dog_policy
   WHERE arena_group_id = p_fid;

  -- Manual curator protection. Fixes the Rosie's-style data-
  -- destruction bug where the trigger blew away rich verbatim
  -- evidence on every rebuild.
  IF v_source = 'manual_curator' AND v_existing_zr IS NOT NULL THEN
    RETURN 'preserved (manual_curator)';
  END IF;

  v_zr := '{"regions":[{"name":null,"sections":{}}],"global_notes":null}'::jsonb;

  -- Legacy injector chain — unchanged.
  v_zr := public._zr_inject_perimeter(v_zr, p_fid);
  v_zr := public._zr_inject_sand_from_policy(v_zr, p_fid);
  v_zr := public._zr_inject_sections_from_bep(v_zr, p_fid);

  -- NEW: entity-aware injector. Runs last so it wins on conflict.
  v_zr := public._zr_inject_from_policy_sources(v_zr, p_fid);

  UPDATE public.beach_dog_policy
     SET zone_rules = v_zr
   WHERE arena_group_id = p_fid;

  RETURN 'updated';
END $$;

COMMENT ON FUNCTION public._promote_zone_rules_for_fid(bigint) IS
  'Phase 3 (2026-05-16): respects beach_dog_policy.source = '
  '''manual_curator'' (preserves zone_rules) and walks the new '
  '_zr_inject_from_policy_sources injector last so entity-based '
  'rules win over legacy BEP-derived rules. Backward-compatible: '
  'beaches without beach_policy_source rows behave as before.';

-- ─── 4. Backfill: mark HBDB's HBMC §13.08.070 row as non_enforced ──

UPDATE public.beach_policy_source bps
   SET operative_status = 'non_enforced',
       status_basis_id  = mou.id,
       status_note      = 'Non-enforced at HBDB per the City of Huntington Beach ↔ Preservation Society of Huntington Dog Beach MOU, which records: "Rather than the state enforcing standard blanket dog prohibitions in this specific area, the city and managing bodies allow dogs to run here."'
  FROM public.policy_source statute,
       public.policy_source mou
 WHERE bps.beach_fid = 6212
   AND bps.section = 'sand'
   AND bps.policy_source_id = statute.id
   AND statute.citation = 'Huntington Beach Municipal Code §13.08.070 (Dogs and Other Animals)'
   AND mou.citation = 'MOU: City of Huntington Beach ↔ Preservation Society of Huntington Dog Beach';

-- ─── 5. Rebuild zone_rules for the six walkthrough beaches ─────────
-- The new resolver runs for these six. Other ~3,950 beaches stay
-- exactly as they are.

SELECT public._promote_zone_rules_for_fid(6097);  -- Fort Funston
SELECT public._promote_zone_rules_for_fid(6212);  -- HBDB
SELECT public._promote_zone_rules_for_fid(6411);  -- Rosie's
SELECT public._promote_zone_rules_for_fid(8330);  -- Crystal Cove
SELECT public._promote_zone_rules_for_fid(8452);  -- Crown Memorial
SELECT public._promote_zone_rules_for_fid(8477);  -- Dockweiler
