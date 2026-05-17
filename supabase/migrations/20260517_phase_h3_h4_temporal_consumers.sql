-- Phase H3 + H4 — consume beach_policy_source_temporal in the injector + promoter.
--
-- H3: extend _zr_inject_from_policy_sources to enrich section payloads
--     with time_windows[] from beach_policy_source_temporal daily windows.
--     Single year-long season still; multi-season emission deferred to
--     follow-up (most beaches don't need it, and seasonal data routes
--     through status_note/modifier text today).
--
-- H4: extend _canonical_dogs_from_policy_sources to also compute
--     dogs_prohibited_start + dogs_prohibited_end (HH:MM) from the
--     longest daily not_allowed window. promote_entity_dogs_to_beach_dog_policy
--     writes these into beach_dog_policy.
--
-- Per docs/consensus_phase_h_temporal_spec.md.

-- ─── H4: extend the canonical-derivation view ─────────────────────
-- Adding 2 new RETURNS TABLE columns requires DROP first.
DROP FUNCTION IF EXISTS public._canonical_dogs_from_policy_sources(bigint);

CREATE FUNCTION public._canonical_dogs_from_policy_sources(p_fid bigint)
RETURNS TABLE(
  has_canonical boolean,
  dogs_allowed text,
  leash_policy text,
  off_leash_flag boolean,
  has_on_leash boolean,
  has_off_leash boolean,
  section_count integer,
  canonical_tier_min smallint,
  dogs_prohibited_start text,
  dogs_prohibited_end text
)
LANGUAGE plpgsql STABLE AS $$
DECLARE v_count int;
BEGIN
  SELECT count(*) INTO v_count
    FROM public.beach_policy_source
   WHERE beach_fid = p_fid AND operative_status = 'operative';

  IF v_count = 0 THEN
    RETURN QUERY SELECT FALSE, NULL::text, NULL::text, FALSE, FALSE, FALSE, 0,
                        NULL::smallint, NULL::text, NULL::text;
    RETURN;
  END IF;

  RETURN QUERY
  WITH section_canonical AS (
    SELECT DISTINCT ON (bps.section)
      bps.section,
      bps.rule,
      public.policy_source_authority(ps.subtype) AS tier
    FROM public.beach_policy_source bps
    JOIN public.policy_source ps ON ps.id = bps.policy_source_id
    WHERE bps.beach_fid = p_fid
      AND bps.operative_status = 'operative'
    ORDER BY
      bps.section,
      public.policy_source_authority(ps.subtype) ASC,
      ps.effective_date DESC NULLS LAST,
      bps.last_verified DESC NULLS LAST
  ),
  agg AS (
    SELECT
      count(*)::int                                                AS n_sections,
      MIN(tier)                                                    AS min_tier,
      bool_or(rule IN ('off_leash','off_leash_voice_control'))     AS any_off_leash,
      bool_or(rule = 'on_leash')                                   AS any_on_leash,
      bool_and(rule = 'not_allowed')                               AS all_not_allowed,
      bool_or(rule = 'not_allowed')                                AS any_not_allowed,
      bool_or(rule IN ('off_leash','off_leash_voice_control','on_leash'))
                                                                   AS any_allowed
    FROM section_canonical
  ),
  -- H4: pick the longest daily not_allowed window for this beach.
  -- Handles wrap-around (end<start = crosses midnight). For non-wrap,
  -- length = end - start; for wrap, length = (24:00 - start) + end.
  daily_prohib AS (
    SELECT
      to_char(daily_start, 'HH24:MI') AS p_start,
      to_char(daily_end,   'HH24:MI') AS p_end,
      CASE
        WHEN daily_end > daily_start
          THEN EXTRACT(EPOCH FROM (daily_end - daily_start))
        ELSE -- wrap-around
          EXTRACT(EPOCH FROM (time '24:00' - daily_start + daily_end))
      END AS span_seconds
    FROM public.beach_policy_source_temporal
    WHERE beach_fid = p_fid
      AND exception_rule = 'not_allowed'
      AND window_kind IN ('daily','seasonal_and_daily')
      AND daily_start IS NOT NULL AND daily_end IS NOT NULL
    ORDER BY span_seconds DESC NULLS LAST, daily_start
    LIMIT 1
  )
  SELECT
    TRUE,
    CASE
      WHEN agg.all_not_allowed THEN 'no'
      WHEN agg.any_allowed AND agg.any_not_allowed THEN 'mixed'
      WHEN agg.any_allowed THEN 'yes'
      ELSE NULL
    END,
    CASE
      WHEN agg.any_off_leash AND agg.any_on_leash THEN 'mixed'
      WHEN agg.any_off_leash THEN 'off_leash'
      WHEN agg.any_on_leash THEN 'on_leash'
      WHEN agg.all_not_allowed THEN 'no_access'
      ELSE NULL
    END,
    COALESCE(agg.any_off_leash, FALSE),
    COALESCE(agg.any_on_leash,  FALSE),
    COALESCE(agg.any_off_leash, FALSE),
    agg.n_sections,
    agg.min_tier,
    (SELECT p_start FROM daily_prohib),
    (SELECT p_end   FROM daily_prohib)
  FROM agg;
END$$;

-- ─── H4: extend the entity promoter to write the new flat columns ─

CREATE OR REPLACE FUNCTION public.promote_entity_dogs_to_beach_dog_policy(p_fid bigint DEFAULT NULL::bigint)
RETURNS TABLE(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
LANGUAGE plpgsql AS $$
DECLARE
  v_inserted bigint := 0;
  v_updated  bigint := 0;
  v_skipped  bigint := 0;
BEGIN
  WITH targets AS (
    SELECT DISTINCT beach_fid
      FROM public.beach_policy_source
     WHERE (p_fid IS NULL OR beach_fid = p_fid)
  ),
  derived AS (
    SELECT
      t.beach_fid AS fid,
      c.dogs_allowed,
      c.leash_policy,
      c.off_leash_flag,
      c.has_on_leash,
      c.has_off_leash,
      c.dogs_prohibited_start,
      c.dogs_prohibited_end,
      c.section_count,
      c.canonical_tier_min
    FROM targets t
    CROSS JOIN LATERAL public._canonical_dogs_from_policy_sources(t.beach_fid) c
    WHERE c.has_canonical
  ),
  protected AS (
    SELECT arena_group_id FROM public.beach_dog_policy WHERE source = 'manual_curator'
  ),
  upsert AS (
    INSERT INTO public.beach_dog_policy
      (arena_group_id, dogs_allowed, leash_policy, off_leash_flag,
       has_on_leash, has_off_leash,
       dogs_prohibited_start, dogs_prohibited_end,
       source, curated_at, notes,
       consensus_confidence, disagreement_flag)
    SELECT
      d.fid, d.dogs_allowed, d.leash_policy, d.off_leash_flag,
      d.has_on_leash, d.has_off_leash,
      d.dogs_prohibited_start::time, d.dogs_prohibited_end::time,
      'entity_promoted', NOW(),
      format(
        'Phase 5 entity-promoted: dogs=%s, has_on=%s, has_off=%s%s. '
        '%s section(s); canonical tier %s.',
        d.dogs_allowed,
        coalesce(d.has_on_leash::text, '?'),
        coalesce(d.has_off_leash::text, '?'),
        CASE WHEN d.dogs_prohibited_start IS NOT NULL
             THEN format(', daily-prohib %s–%s', d.dogs_prohibited_start, d.dogs_prohibited_end)
             ELSE '' END,
        d.section_count,
        d.canonical_tier_min
      ),
      1.0,
      FALSE
    FROM derived d
    WHERE d.fid NOT IN (SELECT arena_group_id FROM protected)
    ON CONFLICT (arena_group_id) DO UPDATE
      SET dogs_allowed          = EXCLUDED.dogs_allowed,
          leash_policy          = EXCLUDED.leash_policy,
          off_leash_flag        = EXCLUDED.off_leash_flag,
          has_on_leash          = EXCLUDED.has_on_leash,
          has_off_leash         = EXCLUDED.has_off_leash,
          dogs_prohibited_start = EXCLUDED.dogs_prohibited_start,
          dogs_prohibited_end   = EXCLUDED.dogs_prohibited_end,
          source                = EXCLUDED.source,
          curated_at            = NOW(),
          notes                 = EXCLUDED.notes,
          consensus_confidence  = EXCLUDED.consensus_confidence,
          disagreement_flag     = EXCLUDED.disagreement_flag
      WHERE beach_dog_policy.source <> 'manual_curator'
        AND (
          beach_dog_policy.dogs_allowed          IS DISTINCT FROM EXCLUDED.dogs_allowed
          OR beach_dog_policy.leash_policy       IS DISTINCT FROM EXCLUDED.leash_policy
          OR beach_dog_policy.off_leash_flag     IS DISTINCT FROM EXCLUDED.off_leash_flag
          OR beach_dog_policy.has_on_leash       IS DISTINCT FROM EXCLUDED.has_on_leash
          OR beach_dog_policy.has_off_leash      IS DISTINCT FROM EXCLUDED.has_off_leash
          OR beach_dog_policy.dogs_prohibited_start IS DISTINCT FROM EXCLUDED.dogs_prohibited_start
          OR beach_dog_policy.dogs_prohibited_end   IS DISTINCT FROM EXCLUDED.dogs_prohibited_end
          OR beach_dog_policy.source             IS DISTINCT FROM EXCLUDED.source
        )
    RETURNING (xmax = 0) AS was_inserted
  )
  SELECT
    count(*) FILTER (WHERE was_inserted = TRUE),
    count(*) FILTER (WHERE was_inserted = FALSE),
    0::bigint
    INTO v_inserted, v_updated, v_skipped
  FROM upsert;

  RETURN QUERY SELECT v_inserted, v_updated, v_skipped;
END$$;

-- ─── H3: extend the zone_rules injector to add section.time_windows[] ──
-- Rebuilds the function; only change vs prior version is the per-section
-- enrichment with time_windows[] derived from beach_policy_source_temporal
-- daily / seasonal_and_daily windows.

CREATE OR REPLACE FUNCTION public._zr_inject_from_policy_sources(p_zr jsonb, p_fid bigint)
RETURNS jsonb
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_zr jsonb := p_zr;
  v_section_resolutions jsonb := '{}'::jsonb;
  v_rec record;
  v_section_data jsonb;
  v_time_windows jsonb;
  v_new_seasons jsonb;
  v_season jsonb;
  v_regs jsonb;
BEGIN
  IF v_zr IS NULL OR p_fid IS NULL THEN
    RETURN v_zr;
  END IF;

  FOR v_rec IN
    SELECT DISTINCT ON (bps.section)
      bps.section,
      bps.rule,
      bps.rule_modifier,
      bps.evidence_verbatim,
      bps.evidence_url,
      ps.subtype,
      ps.citation,
      public.policy_source_authority(ps.subtype) AS tier,
      bps.policy_source_id AS canonical_policy_source_id,
      (
        SELECT jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
          'rule',                  bps2.rule,
          'tier',                  public.policy_source_authority(ps2.subtype),
          'subtype',               ps2.subtype::text,
          'citation',              ps2.citation,
          'quote',                 bps2.evidence_verbatim,
          'source_url',            bps2.evidence_url,
          'operative_status',      bps2.operative_status::text,
          'status_note',           bps2.status_note,
          'status_basis_citation', basis.citation
        )) ORDER BY public.policy_source_authority(ps2.subtype))
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
      public.policy_source_authority(ps.subtype) ASC,
      ps.effective_date DESC NULLS LAST,
      bps.last_verified DESC NULLS LAST
  LOOP
    -- H3: collect daily / seasonal_and_daily time windows for THIS section
    -- (matched via composite bps key). Only daily windows produce
    -- time_windows[] entries; seasonal-only windows are surfaced via
    -- the supplementary_sources array (and a future multi-season pass).
    SELECT jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
      'start', to_char(daily_start, 'HH24:MI'),
      'end',   to_char(daily_end,   'HH24:MI'),
      'rule',  exception_rule,
      'window_kind', window_kind,
      'season_label', season_label,
      'season_anchor_start', anchor_start,
      'season_anchor_end',   anchor_end,
      'effective_from_md', effective_from_md,
      'effective_to_md',   effective_to_md
    )) ORDER BY daily_start NULLS LAST)
      INTO v_time_windows
      FROM public.beach_policy_source_temporal
     WHERE beach_fid = p_fid
       AND policy_source_id = v_rec.canonical_policy_source_id
       AND section = v_rec.section
       AND window_kind IN ('daily','seasonal_and_daily')
       AND (daily_start IS NOT NULL OR daily_end IS NOT NULL
            OR anchor_start IN ('dawn','dusk') OR anchor_end IN ('dawn','dusk'));

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
      'time_windows', v_time_windows,
      'supplementary_sources', v_rec.supplementary
    ));
    v_section_resolutions := jsonb_set(
      v_section_resolutions,
      ARRAY[v_rec.section],
      v_section_data,
      true
    );
  END LOOP;

  IF v_section_resolutions = '{}'::jsonb THEN
    RETURN v_zr;
  END IF;

  -- Pass 2: apply resolutions to the correct shape (unchanged from prior version)
  IF v_zr ? 'seasons' AND jsonb_typeof(v_zr->'seasons') = 'array' THEN
    v_new_seasons := '[]'::jsonb;
    FOR v_season IN SELECT * FROM jsonb_array_elements(v_zr->'seasons')
    LOOP
      v_regs := v_season->'regions';
      IF v_regs IS NOT NULL
         AND jsonb_typeof(v_regs) = 'array'
         AND jsonb_array_length(v_regs) > 0
      THEN
        v_regs := jsonb_set(
          v_regs, '{0,sections}',
          COALESCE(v_regs->0->'sections', '{}'::jsonb) || v_section_resolutions,
          true
        );
      ELSE
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
            jsonb_set(r, '{sections}',
              COALESCE(r->'sections', '{}'::jsonb) || v_section_resolutions, true)
          ELSE r
        END
      )
      FROM jsonb_array_elements(v_regs) r
    );
    v_zr := jsonb_set(v_zr, '{regions}', v_regs);
  END IF;

  RETURN v_zr;
END$$;

COMMENT ON FUNCTION public._canonical_dogs_from_policy_sources(bigint) IS
  'Aggregates beach_policy_source rules per beach into flat columns. H4 '
  '(2026-05-17) adds dogs_prohibited_start/end derived from longest daily '
  'not_allowed window in beach_policy_source_temporal.';

COMMENT ON FUNCTION public._zr_inject_from_policy_sources(jsonb, bigint) IS
  'Entity-aware injector for zone_rules. Picks canonical per-section by '
  'authority tier, builds supplementary_sources array, emits section payload '
  'with evidence + modifier. H3 (2026-05-17) adds section.time_windows[] '
  'from beach_policy_source_temporal daily windows. Multi-season emission '
  'deferred (single-season today).';

-- Re-fire promotion for every beach with bps rows so the new fields populate.
DO $$
DECLARE v_n int;
BEGIN
  SELECT count(*) INTO v_n FROM (
    SELECT public.promote_entity_dogs_to_beach_dog_policy(beach_fid)
      FROM (SELECT DISTINCT beach_fid FROM public.beach_policy_source) t
  ) x;
  RAISE NOTICE 'H3+H4 re-fire: promoted % beaches.', v_n;
END$$;
