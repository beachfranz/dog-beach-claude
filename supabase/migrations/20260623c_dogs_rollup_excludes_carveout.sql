-- 20260623c_dogs_rollup_excludes_carveout.sql
--
-- R1 fix, step 3 of 3 (regression audit 2026-06-23). With carve-out off-leash
-- sections now tagged (beach_policy_source.is_carveout — deterministic 20260623b
-- + LLM classify_offleash_carveout.py), exclude them from the dogs roll-up so a
-- carve-out clause (off-leash at a dog park / on private property / during an
-- activity / a definition / a different named place) no longer flips the BEACH
-- to off_leash_flag / leash_policy='mixed'. The genuine general off-leash rule
-- still wins. Section-level data is untouched.
--
-- Change vs the prior def: `AND bps.is_carveout = false` added to BOTH the
-- winning_source pick AND the section_region_canonical aggregation, so a
-- carve-out can't out-rank a genuine lower-tier rule for the same section, and
-- carve-out rows contribute to none of the bool_or aggregations.
--
-- Apply AFTER the carve-out tagging is validated. After applying, re-promote
-- via promote_entity_dogs_to_beach_dog_policy (chunked) to flip the consumer
-- surface. The bps-mutation trigger does NOT re-fire on a function redefinition,
-- so the re-promote is explicit.

CREATE OR REPLACE FUNCTION public._canonical_dogs_from_policy_sources(p_fid bigint)
 RETURNS TABLE(has_canonical boolean, dogs_allowed text, leash_policy text, off_leash_flag boolean, has_on_leash boolean, has_off_leash boolean, section_count integer, canonical_tier_min smallint, dogs_prohibited_start text, dogs_prohibited_end text)
 LANGUAGE plpgsql
 STABLE
AS $function$
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
  winning_source AS (
    SELECT DISTINCT ON (bps.section, COALESCE(bps.region_name, '__default__'))
      bps.section,
      COALESCE(bps.region_name, '__default__')              AS region_key,
      bps.region_name,
      bps.policy_source_id,
      public.policy_source_effective_tier_for_beach(bps.policy_source_id, bps.beach_fid) AS tier
    FROM public.beach_policy_source bps
    JOIN public.policy_source ps ON ps.id = bps.policy_source_id
    WHERE bps.beach_fid = p_fid
      AND bps.operative_status = 'operative'
      AND bps.is_carveout = false                         -- R1: drop carve-outs
    ORDER BY
      bps.section,
      COALESCE(bps.region_name, '__default__'),
      public.policy_source_effective_tier_for_beach(bps.policy_source_id, bps.beach_fid) ASC,
      ps.effective_date DESC NULLS LAST,
      bps.last_verified DESC NULLS LAST
  ),
  section_region_canonical AS (
    SELECT
      bps.section,
      COALESCE(bps.region_name, '__default__') AS region_key,
      bps.region_name,
      bps.rule,
      ws.tier
    FROM public.beach_policy_source bps
    JOIN winning_source ws
      ON bps.section            = ws.section
     AND COALESCE(bps.region_name, '__default__') = ws.region_key
     AND bps.policy_source_id   = ws.policy_source_id
    WHERE bps.beach_fid          = p_fid
      AND bps.operative_status   = 'operative'
      AND bps.is_carveout = false                          -- R1: drop carve-outs
  ),
  per_section AS (
    SELECT section, MIN(tier) AS section_min_tier
      FROM section_region_canonical GROUP BY section
  ),
  agg AS (
    SELECT
      (SELECT count(*)::int FROM per_section)                AS n_sections,
      (SELECT MIN(section_min_tier) FROM per_section)        AS min_tier,
      bool_or(rule IN ('off_leash','off_leash_voice_control')) AS any_off_leash,
      bool_or(rule IN ('on_leash','on_leash_or_voice'))        AS any_on_leash,
      bool_or(rule IN ('off_leash','off_leash_voice_control',
                       'on_leash','on_leash_or_voice'))        AS any_explicit_allow,
      bool_or(rule = 'not_allowed')                            AS any_not_allowed,
      count(DISTINCT region_key)                               AS n_regions,
      bool_or(rule IN ('off_leash','off_leash_voice_control')) FILTER (WHERE region_key <> '__default__') AS named_any_off_leash,
      bool_or(rule IN ('on_leash','on_leash_or_voice'))        FILTER (WHERE region_key <> '__default__') AS named_any_on_leash,
      bool_or(rule IN ('off_leash','off_leash_voice_control')) FILTER (WHERE region_key  = '__default__') AS default_any_off_leash,
      bool_or(rule IN ('on_leash','on_leash_or_voice'))        FILTER (WHERE region_key  = '__default__') AS default_any_on_leash
    FROM section_region_canonical
  ),
  multi_region AS (
    SELECT
      (a.n_regions >= 2 AND a.any_off_leash AND a.any_on_leash) AS is_multi_region_split
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
      WHEN (SELECT is_multi_region_split FROM multi_region)         THEN 'mixed'
      WHEN agg.any_explicit_allow AND agg.any_not_allowed            THEN 'mixed'
      WHEN agg.any_explicit_allow                                    THEN 'yes'
      WHEN agg.any_not_allowed                                       THEN 'no'
      ELSE NULL
    END,
    CASE
      WHEN (SELECT is_multi_region_split FROM multi_region)   THEN 'mixed'
      WHEN agg.any_off_leash AND agg.any_on_leash             THEN 'mixed'
      WHEN agg.any_off_leash                                  THEN 'off_leash'
      WHEN agg.any_on_leash                                   THEN 'on_leash'
      WHEN agg.any_not_allowed                                THEN 'no_access'
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
END;
$function$;
