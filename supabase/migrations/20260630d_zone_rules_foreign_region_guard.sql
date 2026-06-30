-- 20260630d_zone_rules_foreign_region_guard.sql
--
-- ROOT-CAUSE fix for cross-beach zone_rules.regions contamination (the
-- "areas are not amenities" finding, 2026-06-30; band-aided frontend-side in
-- 84570ce). A shared multi-place policy source (e.g. the GOGA Superintendent's
-- Compendium, linked to 135 beaches) lists many places; extract_and_load_deep
-- emits a beach_policy_source row per listed place, all on the owning beach's
-- fid. The zone_rules injector then turns each region_name into a beach zone,
-- so Fort Funston (6097) carried Baker Beach / Crissy Field / Lands End / Fort
-- Miley / Ocean Beach / GGNRA campgrounds as "areas". 743 in-scope beaches
-- affected. Display-only (scoring reads zone_rules but cross-beach regions
-- don't yield closures), but wrong per [[zone-rules-regions-are-physical-only]].
--
-- The prior injector guard dropped only JURISDICTIONAL region names; other
-- beaches/places slipped through. This generalizes it: a region is FOREIGN
-- unless it is the whole-beach default (NULL), names THIS beach (a physical
-- sub-zone), or is unique to one beach (a genuine sub-feature). The shared-
-- source fingerprint (region_name carried on >=2 beaches AND not self-named)
-- catches the contamination while preserving unique sub-features like
-- "Bluebird Park" on Fisherman's Cove (carried on 1 beach).
--
-- After CREATE OR REPLACE, the data clean re-promotes affected beaches so their
-- stored zone_rules are rebuilt without the foreign regions.

-- index so the shared-count probe in the guard is cheap (called per region row)
CREATE INDEX IF NOT EXISTS idx_bps_region_name_lower
  ON public.beach_policy_source (lower(region_name));

CREATE OR REPLACE FUNCTION public._region_is_foreign_to_beach(p_region_name text, p_fid bigint)
RETURNS boolean
LANGUAGE plpgsql
STABLE
AS $fn$
DECLARE
  v_beach_name  text;
  v_shared_count int;
BEGIN
  -- NULL region_name = the canonical whole-beach default → keep (not foreign).
  IF p_region_name IS NULL THEN RETURN false; END IF;

  SELECT coalesce(g.display_name_override, g.name) INTO v_beach_name
    FROM public.beaches_gold g WHERE g.fid = p_fid;

  -- Self-named: region_name mentions THIS beach → a physical sub-zone → keep.
  IF v_beach_name IS NOT NULL AND length(v_beach_name) >= 4
     AND lower(p_region_name) LIKE '%' || lower(v_beach_name) || '%' THEN
    RETURN false;
  END IF;

  -- Jurisdictional scope (county / city / agency) → foreign.
  IF public._is_jurisdictional_region_name(p_region_name) THEN
    RETURN true;
  END IF;

  -- Shared multi-place-source fingerprint: a genuine sub-zone of THIS beach
  -- appears ONLY on this beach. A region_name carried on >=2 beaches comes from
  -- a shared ordinance that lists many places → foreign.
  SELECT count(DISTINCT beach_fid) INTO v_shared_count
    FROM public.beach_policy_source
   WHERE lower(region_name) = lower(p_region_name);

  RETURN (v_shared_count >= 2);
END
$fn$;

COMMENT ON FUNCTION public._region_is_foreign_to_beach(text, bigint) IS
  'TRUE when a beach_policy_source.region_name does NOT describe a physical sub-zone of THIS beach (jurisdiction, or another beach/place from a shared multi-place source). Used by _zr_inject_from_policy_sources to keep zone_rules.regions physical-only. Keeps: NULL default, self-named, single-beach-unique sub-features.';

-- ── injector with the generalized guard ──────────────────────────────────
CREATE OR REPLACE FUNCTION public._zr_inject_from_policy_sources(p_zr jsonb, p_fid bigint)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE
  v_zr jsonb := p_zr;
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
      public.policy_source_effective_tier_for_beach(bps.policy_source_id, bps.beach_fid)            AS tier,
      bps.policy_source_id                                  AS canonical_policy_source_id,
      (
        SELECT jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
          'rule',                  bps2.rule,
          'tier',                  public.policy_source_effective_tier_for_beach(bps2.policy_source_id, bps2.beach_fid),
          'subtype',               ps2.subtype::text,
          'citation',              ps2.citation,
          'quote',                 bps2.evidence_verbatim,
          'source_url',            bps2.evidence_url,
          'operative_status',      bps2.operative_status::text,
          'status_note',           bps2.status_note,
          'status_basis_citation', basis.citation
        )) ORDER BY public.policy_source_effective_tier_for_beach(bps2.policy_source_id, bps2.beach_fid))
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
      -- Drop FOREIGN region names (jurisdictions + other beaches/places carried
      -- by shared multi-place policy sources) per [[zone-rules-regions-are-physical-only]].
      -- A region must describe a physical sub-zone of THIS beach; NULL region_name
      -- (the whole-beach default) is always kept. Rule in _region_is_foreign_to_beach.
      -- (mig 20260630d — generalizes the prior jurisdictional-only guard.)
      AND NOT public._region_is_foreign_to_beach(bps.region_name, p_fid)
    ORDER BY
      bps.section,
      COALESCE(bps.region_name, '__default__'),
      public.policy_source_effective_tier_for_beach(bps.policy_source_id, bps.beach_fid) ASC,
      ps.effective_date DESC NULLS LAST,
      bps.last_verified DESC NULLS LAST
  LOOP
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

  IF v_region_payloads = '{}'::jsonb THEN
    RETURN v_zr;
  END IF;

  SELECT array_agg(k ORDER BY k)
    INTO v_named_keys
    FROM jsonb_object_keys(v_region_payloads) k
   WHERE k <> '__default__';

  v_default_sections := v_region_payloads->'__default__';

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
          v_regs := jsonb_set(
            v_regs, '{0,sections}',
            COALESCE(v_regs->0->'sections', '{}'::jsonb) || v_default_sections,
            true
          );
        END IF;
      END IF;

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
    IF v_zr ? 'regions' AND jsonb_typeof(v_zr->'regions') = 'array' THEN
      v_regs := public._zr_ensure_unnamed_region(v_zr->'regions');
    ELSE
      v_regs := jsonb_build_array(
        jsonb_build_object('name', NULL, 'sections', '{}'::jsonb)
      );
    END IF;

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
END$function$
