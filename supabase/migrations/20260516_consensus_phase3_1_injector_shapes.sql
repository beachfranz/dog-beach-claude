-- Consensus engine rewrite — Phase 3.1: extend the entity-aware
-- injector to handle BOTH zone_rules shapes (top-level regions[]
-- AND seasons[].regions[]).
--
-- Phase 3 shipped an injector that only handled the v1 shape
-- (regions[] at top). HBDB uses the v2 shape (seasons[] with a
-- named region nested inside). The injector ran but silently found
-- no place to write, so HBDB's beach_policy_source rows didn't
-- flow into zone_rules.
--
-- This phase refactors the injector to:
-- 1. Compute all section resolutions into a single per-call JSONB
--    object (cleaner than re-running the loop per shape).
-- 2. Detect the existing zone_rules shape and merge resolutions
--    into the correct nesting.
-- 3. For v2 (seasons[]): merge into the FIRST region of each
--    season. For HBDB (one season, one region) this is exactly
--    right. For beaches with multiple regions per season, the
--    same resolutions apply to the first region — refinement
--    will require adding region_name to beach_policy_source,
--    deferred until a beach surfaces the need.
-- 4. For v1 (regions[] at top): merge into the unnamed region as
--    before.
--
-- Also re-fires the rebuild for HBDB (fid 6212) so the new
-- behavior takes effect immediately.

-- ─── Updated injector ──────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public._zr_inject_from_policy_sources(p_zr jsonb, p_fid bigint)
RETURNS jsonb
LANGUAGE plpgsql STABLE
AS $$
DECLARE
  v_zr jsonb := p_zr;
  v_section_resolutions jsonb := '{}'::jsonb;
  v_rec record;
  v_section_data jsonb;
  v_new_seasons jsonb;
  v_season jsonb;
  v_regs jsonb;
BEGIN
  IF v_zr IS NULL OR p_fid IS NULL THEN
    RETURN v_zr;
  END IF;

  -- ── Pass 1: compute the resolved section data per section ──
  -- DISTINCT ON picks the canonical operative row per section,
  -- ordered by authority tier asc (lower = higher authority),
  -- then effective_date desc, then last_verified desc. The
  -- supplementary array includes ALL rows at this section
  -- (operative + non-operative with status flags).
  FOR v_rec IN
    SELECT DISTINCT ON (bps.section)
      bps.section,
      bps.rule,
      bps.rule_modifier,
      bps.evidence_verbatim,
      bps.evidence_url,
      ps.subtype,
      ps.citation,
      policy_source_authority(ps.subtype) AS tier,
      bps.policy_source_id AS canonical_policy_source_id,
      (
        SELECT jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
          'rule',                  bps2.rule,
          'tier',                  policy_source_authority(ps2.subtype),
          'subtype',               ps2.subtype::text,
          'citation',              ps2.citation,
          'quote',                 bps2.evidence_verbatim,
          'source_url',            bps2.evidence_url,
          'operative_status',      bps2.operative_status::text,
          'status_note',           bps2.status_note,
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
        'quote',          v_rec.evidence_verbatim,
        'source',         v_rec.subtype::text,
        'citation',       v_rec.citation,
        'source_url',     v_rec.evidence_url,
        'authority_tier', v_rec.tier
      )),
      'modifier', v_rec.rule_modifier,
      'supplementary_sources', v_rec.supplementary
    ));
    v_section_resolutions := jsonb_set(
      v_section_resolutions,
      ARRAY[v_rec.section],
      v_section_data,
      true
    );
  END LOOP;

  -- No resolutions: pure no-op.
  IF v_section_resolutions = '{}'::jsonb THEN
    RETURN v_zr;
  END IF;

  -- ── Pass 2: apply resolutions to the correct shape ──
  IF v_zr ? 'seasons' AND jsonb_typeof(v_zr->'seasons') = 'array' THEN
    -- v2 shape: seasons[].regions[]. Merge resolutions into the
    -- FIRST region of each season. HBDB has one season and one
    -- region so this is exactly right. Multi-region-per-season
    -- beaches would need region_name disambiguation on
    -- beach_policy_source — deferred until a beach surfaces it.
    v_new_seasons := '[]'::jsonb;
    FOR v_season IN SELECT * FROM jsonb_array_elements(v_zr->'seasons')
    LOOP
      v_regs := v_season->'regions';
      IF v_regs IS NOT NULL
         AND jsonb_typeof(v_regs) = 'array'
         AND jsonb_array_length(v_regs) > 0
      THEN
        -- Merge: existing sections || resolutions (right side wins
        -- on key conflict, which is what we want — entity-based
        -- data is canonical).
        v_regs := jsonb_set(
          v_regs,
          '{0,sections}',
          COALESCE(v_regs->0->'sections', '{}'::jsonb) || v_section_resolutions,
          true
        );
      ELSE
        -- Season had no regions; create one with our resolutions.
        v_regs := jsonb_build_array(
          jsonb_build_object('name', NULL, 'sections', v_section_resolutions)
        );
      END IF;
      v_new_seasons := v_new_seasons || jsonb_build_array(
        jsonb_set(v_season, '{regions}', v_regs)
      );
    END LOOP;
    v_zr := jsonb_set(v_zr, '{seasons}', v_new_seasons);

  ELSE
    -- v1 shape: top-level regions[]. Merge into the unnamed region.
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
            jsonb_set(
              r,
              '{sections}',
              COALESCE(r->'sections', '{}'::jsonb) || v_section_resolutions,
              true
            )
          ELSE r
        END
      )
      FROM jsonb_array_elements(v_regs) r
    );

    v_zr := jsonb_set(v_zr, '{regions}', v_regs);
  END IF;

  RETURN v_zr;
END $$;

COMMENT ON FUNCTION public._zr_inject_from_policy_sources(jsonb, bigint) IS
  'Phase 3.1 (2026-05-16): handles BOTH zone_rules shapes (v1 '
  'top-level regions[] AND v2 seasons[].regions[]). For v2, merges '
  'into the first region of each season. For v1, merges into the '
  'unnamed region. No-op for beaches without operative '
  'beach_policy_source rows.';

-- ─── Re-fire HBDB so the new behavior takes effect ─────────────────
SELECT public._promote_zone_rules_for_fid(6212);
