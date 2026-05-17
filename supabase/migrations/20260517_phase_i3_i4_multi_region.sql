-- Phase I3 + I4 — multi-region awareness in the promoter + injector.
--
-- I3: extend _canonical_dogs_from_policy_sources to detect multi-region
--     beaches via the new beach_policy_source.region_name column
--     (added in I1, backfilled in I2). When a beach has bps rows
--     spanning ≥2 distinct region values (NULL counts as the "default
--     region") AND any region carries off_leash AND any region carries
--     on_leash, the promoter emits:
--       dogs_allowed   = 'mixed'
--       leash_policy   = 'mixed'
--       has_on_leash   = TRUE
--       has_off_leash  = TRUE
--       off_leash_flag = TRUE
--     This is the "Coronado pattern" — citywide leash law + named
--     off-leash carve-out. Single-region beaches (today's majority)
--     behave exactly as before.
--
-- I4: extend _zr_inject_from_policy_sources to emit one regions[]
--     entry per distinct region_name (instead of folding everything
--     into regions[0]). Each region carries its OWN sections{} payload,
--     preserving section.time_windows[] (H3) + supplementary_sources.
--     Rows with NULL region_name fold into the unnamed/default region;
--     named regions become NEW regions appended to regions[]
--     (or to seasons[].regions[] in the v2 shape).
--     Multi-season emission stays deferred (single-season today).
--
-- Per docs/consensus_phase_i_subarea_spec.md §I3 + §I4.
--
-- Builds on top of 20260517_phase_h3_h4_temporal_consumers.sql.
--
-- Manual_curator protection is preserved (handled by
-- promote_entity_dogs_to_beach_dog_policy + _promote_zone_rules_for_fid).
--
-- Re-fires the entity promoter at the bottom so all beaches with bps
-- rows pick up the new shape immediately (same pattern as H3+H4).

-- ─── I3: extend the canonical-derivation view ─────────────────────
-- No signature change (RETURNS TABLE columns identical to H4).
-- Logic changes are entirely inside the function body. Use CREATE OR
-- REPLACE — DROP not required.

CREATE OR REPLACE FUNCTION public._canonical_dogs_from_policy_sources(p_fid bigint)
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
  WITH
  -- I3: per-(section, region) canonical, picking the highest-authority
  -- operative row. NULL region_name is folded into the "__default__"
  -- bucket so it participates in distinct-region counting.
  section_region_canonical AS (
    SELECT DISTINCT ON (bps.section, COALESCE(bps.region_name, '__default__'))
      bps.section,
      COALESCE(bps.region_name, '__default__')              AS region_key,
      bps.region_name,
      bps.rule,
      public.policy_source_authority(ps.subtype)            AS tier
    FROM public.beach_policy_source bps
    JOIN public.policy_source ps ON ps.id = bps.policy_source_id
    WHERE bps.beach_fid = p_fid
      AND bps.operative_status = 'operative'
    ORDER BY
      bps.section,
      COALESCE(bps.region_name, '__default__'),
      public.policy_source_authority(ps.subtype) ASC,
      ps.effective_date DESC NULLS LAST,
      bps.last_verified DESC NULLS LAST
  ),
  -- Per-section aggregation (today's behavior — picks the most-
  -- restrictive across regions within that section to feed
  -- section_count + the single-section signal).
  -- For multi-region detection we look at distinct region_key
  -- across the whole beach (not per-section), because a multi-region
  -- beach's regions typically share the same section ('sand').
  per_section AS (
    SELECT
      section,
      MIN(tier) AS section_min_tier
    FROM section_region_canonical
    GROUP BY section
  ),
  agg AS (
    SELECT
      (SELECT count(*)::int FROM per_section)                AS n_sections,
      (SELECT MIN(section_min_tier) FROM per_section)        AS min_tier,
      bool_or(rule IN ('off_leash','off_leash_voice_control')) AS any_off_leash,
      bool_or(rule = 'on_leash')                             AS any_on_leash,
      bool_and(rule = 'not_allowed')                         AS all_not_allowed,
      bool_or(rule = 'not_allowed')                          AS any_not_allowed,
      bool_or(rule IN ('off_leash','off_leash_voice_control','on_leash'))
                                                             AS any_allowed,
      -- I3: count of distinct regions (including default sentinel).
      -- Used to detect multi-region beaches.
      count(DISTINCT region_key)                             AS n_regions,
      -- I3: per-region rule flags, used to detect on+off split
      -- across regions specifically (not just across sections).
      bool_or(rule IN ('off_leash','off_leash_voice_control')) FILTER (WHERE region_key <> '__default__') AS named_any_off_leash,
      bool_or(rule = 'on_leash')                                FILTER (WHERE region_key <> '__default__') AS named_any_on_leash,
      bool_or(rule IN ('off_leash','off_leash_voice_control')) FILTER (WHERE region_key  = '__default__') AS default_any_off_leash,
      bool_or(rule = 'on_leash')                                FILTER (WHERE region_key  = '__default__') AS default_any_on_leash
    FROM section_region_canonical
  ),
  -- I3: derive the multi-region override flag. TRUE when
  --   (a) ≥2 distinct region_keys exist, AND
  --   (b) the regions DISAGREE on leash style (at least one off-leash
  --       region AND at least one on-leash region across the regions).
  -- A single-region beach (n_regions=1) NEVER triggers this branch.
  multi_region AS (
    SELECT
      (
        a.n_regions >= 2
        AND a.any_off_leash
        AND a.any_on_leash
      ) AS is_multi_region_split
    FROM agg a
  ),
  -- H4 (unchanged): pick the longest daily not_allowed window.
  daily_prohib AS (
    SELECT
      to_char(daily_start, 'HH24:MI') AS p_start,
      to_char(daily_end,   'HH24:MI') AS p_end,
      CASE
        WHEN daily_end > daily_start
          THEN EXTRACT(EPOCH FROM (daily_end - daily_start))
        ELSE
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
    -- dogs_allowed: I3 override → 'mixed' for multi-region splits
    -- (since some sub-area allows dogs, the beach as a whole isn't
    -- a clean yes/no). Falls back to the H4 behavior otherwise.
    CASE
      WHEN (SELECT is_multi_region_split FROM multi_region) THEN 'mixed'
      WHEN agg.all_not_allowed THEN 'no'
      WHEN agg.any_allowed AND agg.any_not_allowed THEN 'mixed'
      WHEN agg.any_allowed THEN 'yes'
      ELSE NULL
    END,
    -- leash_policy: I3 override → 'mixed' for splits with both rules.
    CASE
      WHEN (SELECT is_multi_region_split FROM multi_region) THEN 'mixed'
      WHEN agg.any_off_leash AND agg.any_on_leash THEN 'mixed'
      WHEN agg.any_off_leash THEN 'off_leash'
      WHEN agg.any_on_leash THEN 'on_leash'
      WHEN agg.all_not_allowed THEN 'no_access'
      ELSE NULL
    END,
    -- off_leash_flag: TRUE if the multi-region split has off-leash
    -- anywhere, OR (single-region) if any operative rule is off-leash.
    COALESCE(agg.any_off_leash, FALSE),
    -- has_on_leash, has_off_leash: per spec, the multi-region case
    -- always sets BOTH true; single-region case mirrors the rule set.
    CASE
      WHEN (SELECT is_multi_region_split FROM multi_region) THEN TRUE
      ELSE COALESCE(agg.any_on_leash, FALSE)
    END,
    CASE
      WHEN (SELECT is_multi_region_split FROM multi_region) THEN TRUE
      ELSE COALESCE(agg.any_off_leash, FALSE)
    END,
    agg.n_sections,
    agg.min_tier,
    (SELECT p_start FROM daily_prohib),
    (SELECT p_end   FROM daily_prohib)
  FROM agg;
END$$;

COMMENT ON FUNCTION public._canonical_dogs_from_policy_sources(bigint) IS
  'Aggregates beach_policy_source rules per beach into flat columns. '
  'H4 (2026-05-17) adds dogs_prohibited_start/end from longest daily '
  'not_allowed window. I3 (2026-05-17) detects multi-region beaches '
  'via region_name and overrides dogs_allowed/leash_policy to ''mixed'' '
  '+ both has_* flags TRUE when ≥2 distinct regions disagree on rule '
  '(e.g., named off-leash carve-out + default on-leash baseline).';

-- ─── I4: extend the zone_rules injector to emit per-region payloads ───
-- Same signature; CREATE OR REPLACE.

CREATE OR REPLACE FUNCTION public._zr_inject_from_policy_sources(p_zr jsonb, p_fid bigint)
RETURNS jsonb
LANGUAGE plpgsql STABLE AS $$
DECLARE
  v_zr jsonb := p_zr;
  -- v_region_payloads: { region_key → { section → section_data } }
  -- where region_key is the literal region_name text, or '__default__'
  -- sentinel for rows with NULL region_name. Built in Pass 1.
  v_region_payloads jsonb := '{}'::jsonb;
  v_rec record;
  v_section_data jsonb;
  v_time_windows jsonb;
  v_region_key text;
  v_existing_sections jsonb;
  v_new_seasons jsonb;
  v_season jsonb;
  v_regs jsonb;
  v_region jsonb;
  v_named_region jsonb;
  v_named_keys text[];
  v_default_sections jsonb;
BEGIN
  IF v_zr IS NULL OR p_fid IS NULL THEN
    RETURN v_zr;
  END IF;

  -- ── Pass 1: build per-(section, region) canonical payloads ──
  -- DISTINCT ON now keys on (section, region_name) so each named
  -- sub-area gets its own canonical row. supplementary[] is also
  -- scoped to the SAME region to keep evidence consistent.
  FOR v_rec IN
    SELECT DISTINCT ON (bps.section, COALESCE(bps.region_name, '__default__'))
      bps.section,
      COALESCE(bps.region_name, '__default__')              AS region_key,
      bps.region_name,
      bps.rule,
      bps.rule_modifier,
      bps.evidence_verbatim,
      bps.evidence_url,
      ps.subtype,
      ps.citation,
      public.policy_source_authority(ps.subtype)            AS tier,
      bps.policy_source_id                                  AS canonical_policy_source_id,
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
          AND COALESCE(bps2.region_name, '__default__')
              = COALESCE(bps.region_name,  '__default__')
          AND bps2.policy_source_id <> bps.policy_source_id
      ) AS supplementary
    FROM public.beach_policy_source bps
    JOIN public.policy_source ps ON ps.id = bps.policy_source_id
    WHERE bps.beach_fid = p_fid
      AND bps.operative_status = 'operative'
    ORDER BY
      bps.section,
      COALESCE(bps.region_name, '__default__'),
      public.policy_source_authority(ps.subtype) ASC,
      ps.effective_date DESC NULLS LAST,
      bps.last_verified DESC NULLS LAST
  LOOP
    -- H3 (unchanged): collect daily / seasonal_and_daily time windows
    -- for THIS section + this canonical ps row.
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

    v_region_key := v_rec.region_key;
    -- Add this section to its region's sections{} bucket.
    v_existing_sections := COALESCE(v_region_payloads->v_region_key, '{}'::jsonb);
    v_existing_sections := jsonb_set(
      v_existing_sections,
      ARRAY[v_rec.section],
      v_section_data,
      true
    );
    v_region_payloads := jsonb_set(
      v_region_payloads,
      ARRAY[v_region_key],
      v_existing_sections,
      true
    );
  END LOOP;

  -- No resolutions: pure no-op.
  IF v_region_payloads = '{}'::jsonb THEN
    RETURN v_zr;
  END IF;

  -- ── Pass 2: apply per-region payloads to the correct shape ──
  --
  -- For each region_key in v_region_payloads:
  --   - '__default__' merges into the existing unnamed region (creating
  --     one if needed). This preserves the today-shape for beaches with
  --     no region_name on any row.
  --   - any other key becomes a NEW region appended to regions[], with
  --     its name = the region_key text and its sections = that bucket.
  --     If a region with the same name already exists, we MERGE
  --     section payloads into it (right-side wins on conflict).
  --
  -- v2 shape (seasons[]): we treat each season independently, applying
  -- the same per-region merge to season.regions[]. Multi-season emission
  -- is still deferred (single year-long season today), so this loop
  -- typically runs once.

  -- Pre-compute the list of named region keys (everything except the
  -- default sentinel) and the default-region sections (or NULL).
  SELECT array_agg(k ORDER BY k)
    INTO v_named_keys
    FROM jsonb_object_keys(v_region_payloads) k
   WHERE k <> '__default__';

  v_default_sections := v_region_payloads->'__default__';  -- may be NULL

  IF v_zr ? 'seasons' AND jsonb_typeof(v_zr->'seasons') = 'array' THEN
    v_new_seasons := '[]'::jsonb;
    FOR v_season IN SELECT * FROM jsonb_array_elements(v_zr->'seasons')
    LOOP
      v_regs := v_season->'regions';
      IF v_regs IS NULL OR jsonb_typeof(v_regs) <> 'array' THEN
        v_regs := jsonb_build_array(
          jsonb_build_object('name', NULL, 'sections', '{}'::jsonb)
        );
      ELSIF jsonb_array_length(v_regs) = 0 THEN
        v_regs := jsonb_build_array(
          jsonb_build_object('name', NULL, 'sections', '{}'::jsonb)
        );
      END IF;

      -- Merge default-region sections into the unnamed region
      -- (or the first region if no unnamed slot exists).
      IF v_default_sections IS NOT NULL THEN
        IF EXISTS (
          SELECT 1 FROM jsonb_array_elements(v_regs) r
           WHERE (r->>'name') IS NULL
        ) THEN
          v_regs := (
            SELECT jsonb_agg(
              CASE
                WHEN (r->>'name') IS NULL THEN
                  jsonb_set(r, '{sections}',
                    COALESCE(r->'sections', '{}'::jsonb) || v_default_sections,
                    true)
                ELSE r
              END
            )
            FROM jsonb_array_elements(v_regs) r
          );
        ELSE
          -- No unnamed slot — fold into regions[0] to preserve the
          -- prior phase 3.1 contract for legacy v2 (HBDB) data.
          v_regs := jsonb_set(
            v_regs, '{0,sections}',
            COALESCE(v_regs->0->'sections', '{}'::jsonb) || v_default_sections,
            true
          );
        END IF;
      END IF;

      -- Now merge / append each named region.
      IF v_named_keys IS NOT NULL THEN
        FOR i IN 1..array_length(v_named_keys, 1)
        LOOP
          v_region_key := v_named_keys[i];
          IF EXISTS (
            SELECT 1 FROM jsonb_array_elements(v_regs) r
             WHERE (r->>'name') = v_region_key
          ) THEN
            v_regs := (
              SELECT jsonb_agg(
                CASE
                  WHEN (r->>'name') = v_region_key THEN
                    jsonb_set(r, '{sections}',
                      COALESCE(r->'sections', '{}'::jsonb)
                        || (v_region_payloads->v_region_key),
                      true)
                  ELSE r
                END
              )
              FROM jsonb_array_elements(v_regs) r
            );
          ELSE
            v_regs := v_regs || jsonb_build_array(jsonb_build_object(
              'name',     v_region_key,
              'sections', v_region_payloads->v_region_key
            ));
          END IF;
        END LOOP;
      END IF;

      v_new_seasons := v_new_seasons || jsonb_build_array(
        jsonb_set(v_season, '{regions}', v_regs)
      );
    END LOOP;
    v_zr := jsonb_set(v_zr, '{seasons}', v_new_seasons);

  ELSE
    -- v1 shape: top-level regions[].
    IF v_zr ? 'regions' AND jsonb_typeof(v_zr->'regions') = 'array' THEN
      v_regs := public._zr_ensure_unnamed_region(v_zr->'regions');
    ELSE
      v_regs := jsonb_build_array(
        jsonb_build_object('name', NULL, 'sections', '{}'::jsonb)
      );
    END IF;

    -- Merge default-region sections into the unnamed region.
    IF v_default_sections IS NOT NULL THEN
      v_regs := (
        SELECT jsonb_agg(
          CASE
            WHEN (r->>'name') IS NULL THEN
              jsonb_set(r, '{sections}',
                COALESCE(r->'sections', '{}'::jsonb) || v_default_sections,
                true)
            ELSE r
          END
        )
        FROM jsonb_array_elements(v_regs) r
      );
    END IF;

    -- Merge / append each named region.
    IF v_named_keys IS NOT NULL THEN
      FOR i IN 1..array_length(v_named_keys, 1)
      LOOP
        v_region_key := v_named_keys[i];
        IF EXISTS (
          SELECT 1 FROM jsonb_array_elements(v_regs) r
           WHERE (r->>'name') = v_region_key
        ) THEN
          v_regs := (
            SELECT jsonb_agg(
              CASE
                WHEN (r->>'name') = v_region_key THEN
                  jsonb_set(r, '{sections}',
                    COALESCE(r->'sections', '{}'::jsonb)
                      || (v_region_payloads->v_region_key),
                    true)
                ELSE r
              END
            )
            FROM jsonb_array_elements(v_regs) r
          );
        ELSE
          v_regs := v_regs || jsonb_build_array(jsonb_build_object(
            'name',     v_region_key,
            'sections', v_region_payloads->v_region_key
          ));
        END IF;
      END LOOP;
    END IF;

    v_zr := jsonb_set(v_zr, '{regions}', v_regs);
  END IF;

  RETURN v_zr;
END$$;

COMMENT ON FUNCTION public._zr_inject_from_policy_sources(jsonb, bigint) IS
  'Entity-aware injector for zone_rules. Picks canonical per-(section, '
  'region) by authority tier, builds supplementary_sources array, emits '
  'section payload with evidence + modifier. H3 (2026-05-17) adds '
  'section.time_windows[] from beach_policy_source_temporal daily '
  'windows. I4 (2026-05-17) emits one regions[] entry per distinct '
  'region_name (NULL folds into the unnamed/default region; named '
  'regions become new regions[] entries). Multi-season emission still '
  'deferred (single-season today).';

-- ─── Re-fire promotion for every beach with bps rows ───────────────
-- Same pattern as the H3+H4 migration, but with an explicit second
-- pass to force zone_rules rebuild even when flat columns are
-- unchanged. The entity promoter UPSERT only fires
-- tg_after_change_dogs_refire_zone_rules when at least one flat
-- column changes; for beaches whose I3 flat-column outcome equals
-- today's value (e.g., single-named-region beaches like 8560/9716
-- that stay off_leash), the zone_rules wouldn't otherwise rebuild
-- and the new named-region payload would never land.

DO $$
DECLARE v_n int;
BEGIN
  -- Pass 1: refresh flat columns (will fire zone_rules trigger only
  -- when a flat column actually changes, which is the common case
  -- for multi-region beaches that now flip to 'mixed').
  SELECT count(*) INTO v_n FROM (
    SELECT public.promote_entity_dogs_to_beach_dog_policy(beach_fid)
      FROM (SELECT DISTINCT beach_fid FROM public.beach_policy_source) t
  ) x;
  RAISE NOTICE 'I3+I4 re-fire pass 1 (flat columns): promoted % beaches.', v_n;

  -- Pass 2: explicitly rebuild zone_rules for every bps beach to
  -- guarantee the named-region payload lands even where Pass 1's
  -- WHERE clause skipped the UPDATE. Manual_curator protection is
  -- inside _promote_zone_rules_for_fid itself.
  SELECT count(*) INTO v_n FROM (
    SELECT public._promote_zone_rules_for_fid(beach_fid)
      FROM (SELECT DISTINCT beach_fid FROM public.beach_policy_source) t
  ) x;
  RAISE NOTICE 'I3+I4 re-fire pass 2 (zone_rules): rebuilt % beaches.', v_n;
END$$;
