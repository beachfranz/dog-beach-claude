-- Consensus engine: boost operator_posted_policy to tier 0 when a
-- beach_operator link confirms the operator manages that specific beach.
-- Per Franz "then consensus engine" 2026-05-18 + docs/operate_pipeline_spec.md (v3)
-- + [[walkthrough-hbdb]] layered-authority principle: operator wins
-- per-entity over territorial codify.
--
-- The bug: PSLHD operator_posted_policy (tier 4) was outranked by SLO
-- County municipal_code (tier 1) for Avila/Olde Port/Fisherman's Beach.
-- The cascade applied correctly but consensus picked municipal_code.
-- All 3 beaches showed on_leash when they should show
-- off_leash_voice_control (Avila/Olde Port) and not_allowed (Fisherman's).
--
-- Fix: add a helper that boosts operator_posted_policy to tier 0 (above
-- tier 1 codified) WHEN the operator has a beach_operator link to the
-- specific beach. This encodes the entity-specific-override principle.
--
-- Per [[two-pass-refire-pattern]]: this is a function-body change.
-- After REPLACE, must do two-pass refire for affected beaches (those
-- with operator_posted_policy ps + matching beach_operator link).
-- Today: 5 such beaches (Dog Beach, Avila, Fisherman's, Olde Port, +
-- duplicate Dog Beach ps).

BEGIN;

-- ─── 1. Helper: effective tier for a (ps, beach) pair ────────────────

CREATE OR REPLACE FUNCTION public.policy_source_effective_tier_for_beach(
    p_ps_id bigint, p_beach_fid bigint
) RETURNS smallint
LANGUAGE sql STABLE PARALLEL SAFE AS $$
  SELECT CASE
    WHEN ps.subtype = 'operator_posted_policy'
      AND ps.issuing_operator_id IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM public.beach_operator bo
        WHERE bo.beach_fid    = p_beach_fid
          AND bo.operator_id  = ps.issuing_operator_id
      )
    THEN 0::smallint  -- entity-specific operator override (beats tier 1 codified)
    ELSE public.policy_source_authority(ps.subtype)
  END
  FROM public.policy_source ps
  WHERE ps.id = p_ps_id
$$;

COMMENT ON FUNCTION public.policy_source_effective_tier_for_beach(bigint,bigint) IS
  'Returns the effective tier for a ps row at a specific beach. '
  'Boosts operator_posted_policy from tier 4 to tier 0 when a beach_operator '
  'link confirms the operator manages that beach — entity-specific override '
  'per walkthrough-hbdb layered authority. Otherwise falls back to '
  'policy_source_authority(subtype).';

-- ─── 2. REPLACE _canonical_dogs_from_policy_sources w/ effective tier ─

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
    CASE
      WHEN (SELECT is_multi_region_split FROM multi_region) THEN 'mixed'
      WHEN agg.all_not_allowed THEN 'no'
      WHEN agg.any_allowed AND agg.any_not_allowed THEN 'mixed'
      WHEN agg.any_allowed THEN 'yes'
      ELSE NULL
    END,
    CASE
      WHEN (SELECT is_multi_region_split FROM multi_region) THEN 'mixed'
      WHEN agg.any_off_leash AND agg.any_on_leash THEN 'mixed'
      WHEN agg.any_off_leash THEN 'off_leash'
      WHEN agg.any_on_leash THEN 'on_leash'
      WHEN agg.all_not_allowed THEN 'no_access'
      ELSE NULL
    END,
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
END$$;

-- ─── 3. REPLACE _zr_inject_from_policy_sources w/ effective tier ──────
-- The injector picks DISTINCT ON per (section, region_name) the canonical
-- ps row for zone_rules emission. Same boost applies.

-- Pull current definition + replace the 4 tier callsites.
-- Per pg_get_functiondef there's a single function; we update the SELECT
-- DISTINCT ON cursor inside the FOR v_rec IN loop + its supplementary
-- aggregator below.

DO $do$
DECLARE
  v_src text;
BEGIN
  SELECT pg_get_functiondef(p.oid) INTO v_src
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname = 'public' AND p.proname = '_zr_inject_from_policy_sources';

  -- Replace all 4 instances of policy_source_authority(ps2?.subtype) with
  -- the effective tier helper. Note: the supplementary aggregator (line ~256
  -- + ~264 in the source migration) uses ps2.subtype (a different alias);
  -- both must be substituted.
  v_src := replace(v_src,
    'public.policy_source_authority(ps.subtype)',
    'public.policy_source_effective_tier_for_beach(bps.policy_source_id, bps.beach_fid)'
  );
  v_src := replace(v_src,
    'public.policy_source_authority(ps2.subtype)',
    'public.policy_source_effective_tier_for_beach(bps2.policy_source_id, bps2.beach_fid)'
  );

  EXECUTE v_src;
END $do$;

COMMENT ON FUNCTION public._canonical_dogs_from_policy_sources(bigint) IS
  'Aggregates beach_policy_source rules per beach into flat columns. '
  'H4 (2026-05-17) adds dogs_prohibited_start/end. I3 (2026-05-17) detects '
  'multi-region beaches via region_name. 2026-05-18: operator_posted_policy '
  'with matching beach_operator link → tier 0 (entity-specific override).';

COMMIT;
