-- 20260606u_canonical_dogs_cross_section.sql
--
-- Cascade fix: align `dogs_allowed` and `leash_policy` in
-- _canonical_dogs_from_policy_sources with the cross-section aggregate,
-- dropping the sand-priority branch that was generating contradictory
-- rows.
--
-- THE BUG
-- The previous resolver computed `dogs_allowed` from the sand section
-- only (when a sand BPS row existed): if sand was "not_allowed", it set
-- `dogs_allowed='no'` even when OTHER sections (a separate off-leash
-- zone, a leashed trail) allowed dogs. The cross-section
-- `has_off_leash` / `has_on_leash` signals stayed true (intentionally —
-- the function's own comment said "a beach with sand=on_leash but a
-- separate off-leash zone should still surface in the off-leash filter").
--
-- The result was rows like:
--   dogs_allowed='no', has_off_leash=true, notes='...canonical tier 3.'
-- Downstream `beach_location_tier(dogs_allowed='no', ...)` returns
-- '4_no_dogs' — outweighing the has_off_leash=true signal — and
-- `refresh_scoring_tier` correctly demotes the beach to scoring_tier='none'.
-- ~330+ active CA beaches drifted into 'none' this way since 2026-05-11.
--
-- THE FIX
-- Drop the sand-priority branch for both `dogs_allowed` and `leash_policy`.
-- The cross-section aggregate (`agg.*`) already encodes the right
-- semantics:
--   * all sections not_allowed              → 'no'
--   * mix of allowed + not_allowed sections → 'mixed'
--   * any allowed section                   → 'yes'
-- The multi-region-split override (Leo Carrillo: North=allow, South=no)
-- still wins on top.
--
-- Sand-specific semantics still live in `has_off_leash` / `has_on_leash`
-- (which are cross-section) and the existing tier ordering; they were
-- never the problem.
--
-- AFTER MIGRATION
-- The migration re-runs `promote_entity_dogs_to_beach_dog_policy()` for
-- all affected beaches and `refresh_scoring_tier()` to flip the demoted
-- beaches back to 'hourly' / 'daily' where appropriate.

CREATE OR REPLACE FUNCTION public._canonical_dogs_from_policy_sources(p_fid bigint)
RETURNS TABLE(
  has_canonical          boolean,
  dogs_allowed           text,
  leash_policy           text,
  off_leash_flag         boolean,
  has_on_leash           boolean,
  has_off_leash          boolean,
  section_count          integer,
  canonical_tier_min     smallint,
  dogs_prohibited_start  text,
  dogs_prohibited_end    text
)
LANGUAGE plpgsql STABLE AS $function$
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
  section_region_canonical AS (
    SELECT DISTINCT ON (bps.section, COALESCE(bps.region_name, '__default__'))
      bps.section,
      COALESCE(bps.region_name, '__default__')              AS region_key,
      bps.region_name,
      bps.rule,
      public.policy_source_effective_tier_for_beach(bps.policy_source_id, bps.beach_fid) AS tier
    FROM public.beach_policy_source bps
    JOIN public.policy_source ps ON ps.id = bps.policy_source_id
    WHERE bps.beach_fid = p_fid
      AND bps.operative_status = 'operative'
    ORDER BY
      bps.section,
      COALESCE(bps.region_name, '__default__'),
      public.policy_source_effective_tier_for_beach(bps.policy_source_id, bps.beach_fid) ASC,
      ps.effective_date DESC NULLS LAST,
      bps.last_verified DESC NULLS LAST
  ),
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
      count(DISTINCT region_key)                             AS n_regions,
      bool_or(rule IN ('off_leash','off_leash_voice_control')) FILTER (WHERE region_key <> '__default__') AS named_any_off_leash,
      bool_or(rule = 'on_leash')                                FILTER (WHERE region_key <> '__default__') AS named_any_on_leash,
      bool_or(rule IN ('off_leash','off_leash_voice_control')) FILTER (WHERE region_key  = '__default__') AS default_any_off_leash,
      bool_or(rule = 'on_leash')                                FILTER (WHERE region_key  = '__default__') AS default_any_on_leash
    FROM section_region_canonical
  ),
  multi_region AS (
    SELECT
      (
        a.n_regions >= 2
        AND a.any_off_leash
        AND a.any_on_leash
      ) AS is_multi_region_split
    FROM agg a
  ),
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
    -- dogs_allowed: CROSS-SECTION reality, no sand-priority.
    -- Multi-region split (e.g. Leo Carrillo: North=allow, South=no) -> 'mixed'.
    -- Then standard cross-section: any allowed + any not_allowed -> 'mixed';
    -- all not_allowed -> 'no'; any allowed -> 'yes'.
    --
    -- The old sand-priority branch (when sand BPS exists, use sand's rule
    -- only) produced `dogs_allowed='no'` for beaches with sand=not_allowed
    -- + a separate off-leash zone, contradicting the cross-section
    -- has_off_leash=true signal. That contradiction demoted ~330 CA
    -- beaches incorrectly. See 20260606u migration notes.
    CASE
      WHEN (SELECT is_multi_region_split FROM multi_region) THEN 'mixed'
      WHEN agg.all_not_allowed                                THEN 'no'
      WHEN agg.any_allowed AND agg.any_not_allowed            THEN 'mixed'
      WHEN agg.any_allowed                                    THEN 'yes'
      ELSE NULL
    END,
    -- leash_policy: same cross-section semantics as dogs_allowed.
    -- A beach with sand=on_leash + a separate off-leash zone is
    -- accurately 'mixed' (some sections off-leash, some on-leash).
    -- The off-leash-zone existence shows up in the find.html chip
    -- via has_off_leash=true.
    CASE
      WHEN (SELECT is_multi_region_split FROM multi_region)   THEN 'mixed'
      WHEN agg.any_off_leash AND agg.any_on_leash             THEN 'mixed'
      WHEN agg.any_off_leash                                  THEN 'off_leash'
      WHEN agg.any_on_leash                                   THEN 'on_leash'
      WHEN agg.all_not_allowed                                THEN 'no_access'
      ELSE NULL
    END,
    -- Cross-section signals (unchanged):
    COALESCE(agg.any_off_leash, FALSE),
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
END;
$function$;

-- Re-run the entity promoter for every beach with an existing
-- `entity_promoted` row (those are the ones the cascade has authority
-- over; `manual_curator` rows are protected by the promoter's
-- excludes-protected logic).
-- We can't process all at once if statement_timeout bites, but the
-- promoter is fast (set-based) — should finish in a few seconds.
SELECT public.promote_entity_dogs_to_beach_dog_policy(arena_group_id)
  FROM public.beach_dog_policy
 WHERE source IN ('entity_promoted', 'auto_promoted_from_consensus');

-- Then re-classify scoring_tier across the catalog.
SELECT * FROM public.refresh_scoring_tier();
