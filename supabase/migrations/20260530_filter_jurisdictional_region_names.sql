-- 20260530_filter_jurisdictional_region_names.sql
--
-- Enforces the [[zone-rules-regions-are-physical-only]] rule at the
-- cascade level. Adds a helper that classifies region names as
-- jurisdictional vs physical, and gates `_zr_inject_from_policy_sources`
-- on it so jurisdictional rows in `beach_policy_source.region_name`
-- can never enter `beach_dog_policy.zone_rules.regions[].name` again.
--
-- Background: 2026-05-30 audit found 350 CA beaches with 54 distinct
-- bogus region names like "San Diego County (at home)",
-- "Unincorporated Monterey County", "City of Aberdeen". LLM extractors
-- had encoded county-wide rule scopes as if they were physical
-- sub-zones of the beach, producing confusing surface displays
-- (Franz: "We do not use counties, states, places in this capacity").
--
-- The underlying BPS rows are kept untouched — they still carry
-- evidence + citation that may be useful for audit / global_notes.
-- They just don't get turned into beach zones.

begin;

-- ── Classifier ───────────────────────────────────────────────────────
create or replace function public._is_jurisdictional_region_name(p_name text)
returns boolean
language sql
immutable
as $$
  select p_name is not null and (
       p_name ~* '\m(county|countywide|statewide|citywide|unincorporated|jurisdiction)\M'
    or p_name ~* '\(at home\)'
    or p_name ~* '\(away from home\)'
    or p_name ~* '^city of '
    or p_name ~* 'all public places'
    or p_name ~* 'within city limits'
    or p_name ~* '\(unincorporated'
  );
$$;

comment on function public._is_jurisdictional_region_name(text) is
  'TRUE when a region name encodes a jurisdictional scope (county, city, "at home", unincorporated) rather than a physical sub-zone of a beach. Per pin [[zone-rules-regions-are-physical-only]] / Franz 2026-05-30. Used by _zr_inject_from_policy_sources to gate region creation.';

-- ── Updated injector ─────────────────────────────────────────────────
-- Same body as before, with one new WHERE clause that filters out BPS
-- rows whose region_name is jurisdictional. Such rows are silently
-- dropped from the regions[] build; their citation is still available
-- in beach_policy_source / beach_enrichment_provenance for audit.
create or replace function public._zr_inject_from_policy_sources(p_zr jsonb, p_fid bigint)
returns jsonb
language plpgsql
stable
as $function$
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
      -- Drop jurisdictional region names per [[zone-rules-regions-are-physical-only]].
      -- These rows still carry valid policy evidence; they just don't get
      -- turned into beach zones (a region must describe a physical sub-zone
      -- of THIS beach, not a county/city/scope). NULL region_name is allowed
      -- (the canonical whole-beach default).
      AND (bps.region_name IS NULL
           OR NOT public._is_jurisdictional_region_name(bps.region_name))
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
END$function$;

commit;
