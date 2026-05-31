-- 20260531_canonical_dogs_sand_priority.sql
--
-- Patch _canonical_dogs_from_policy_sources to prioritize the SAND section
-- when computing flat-field dogs_allowed and leash_policy. Cross-section
-- aggregation continues to drive has_on_leash / has_off_leash (consumer
-- filter signals).
--
-- Problem this fixes:
-- 559 beaches with zone_rules.sand.rule='not_allowed' but flat
-- dogs_allowed='yes' OR 'mixed' (per audit 2026-05-31).
--
-- Most acute example: Huntington State Beach (fid 8453). Operative BPS:
--   sand=not_allowed (CDPR), bike_path=on_leash, multiuse_trail=on_leash
-- Pre-fix: dogs_allowed='mixed', leash_policy='on_leash'
--   -> find.html "on leash" filter would surface this beach
--   -> consumers think they can bring a leashed dog onto the beach
-- Post-fix: dogs_allowed='no', leash_policy='no_access'
--   -> find.html filters out
--   -> tg_after_change_refresh_scoring_tier auto-demotes from daily->none
--   -> consumers see "dogs not allowed" badge
--
-- Design choice:
-- "Sand" is the consumer surface for a dog-beach app. A beach where
-- you can park or use the bike path but cannot put your dog on the
-- sand is not a dog-friendly beach. Other sections still contribute
-- to has_on_leash / has_off_leash so the off-leash filter can pick
-- up beaches with off-leash sub-zones.
--
-- Fallback: when no operative sand BPS exists, fall back to the
-- existing cross-section aggregation (the function still computes
-- something useful for beaches with only bike_path/parking BPS).
--
-- Sand multi-region case (Leo Carrillo: North=on_leash, South=not_allowed):
-- sand_any_allowed=true AND sand_any_not_allowed=true -> dogs_allowed='mixed',
-- leash_policy='mixed' (matches today's behavior for that case).
--
-- After applying, a bulk re-promote is needed to update existing rows:
--   SELECT public.promote_entity_dogs_to_beach_dog_policy(NULL);
-- This is done in the same migration below.

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
  -- NEW: sand-specific aggregation (consumer-surface signal for dogs_allowed/leash_policy)
  sand_agg AS (
    SELECT
      (COUNT(*) > 0)                                         AS sand_has_rule,
      bool_or(rule IN ('off_leash','off_leash_voice_control')) AS sand_any_off_leash,
      bool_or(rule = 'on_leash')                             AS sand_any_on_leash,
      bool_or(rule = 'not_allowed')                          AS sand_any_not_allowed,
      bool_and(rule = 'not_allowed')                         AS sand_all_not_allowed,
      bool_or(rule IN ('off_leash','off_leash_voice_control','on_leash')) AS sand_any_allowed
    FROM section_region_canonical
    WHERE section = 'sand'
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
    -- dogs_allowed: prefer sand-level rule when sand BPS exists.
    -- Multi-region split overrides (Leo Carrillo parent: North=allow, South=no -> mixed).
    CASE
      WHEN (SELECT is_multi_region_split FROM multi_region) THEN 'mixed'
      WHEN sand_agg.sand_has_rule THEN
        CASE
          WHEN sand_agg.sand_all_not_allowed                            THEN 'no'
          WHEN sand_agg.sand_any_allowed AND sand_agg.sand_any_not_allowed THEN 'mixed'
          WHEN sand_agg.sand_any_allowed                                THEN 'yes'
          ELSE NULL
        END
      WHEN agg.all_not_allowed THEN 'no'
      WHEN agg.any_allowed AND agg.any_not_allowed THEN 'mixed'
      WHEN agg.any_allowed THEN 'yes'
      ELSE NULL
    END,
    -- leash_policy: prefer sand-level rule when sand BPS exists.
    CASE
      WHEN (SELECT is_multi_region_split FROM multi_region) THEN 'mixed'
      WHEN sand_agg.sand_has_rule THEN
        CASE
          WHEN sand_agg.sand_all_not_allowed                                  THEN 'no_access'
          WHEN sand_agg.sand_any_off_leash AND sand_agg.sand_any_on_leash    THEN 'mixed'
          WHEN sand_agg.sand_any_off_leash                                   THEN 'off_leash'
          WHEN sand_agg.sand_any_on_leash                                    THEN 'on_leash'
          ELSE NULL
        END
      WHEN agg.any_off_leash AND agg.any_on_leash THEN 'mixed'
      WHEN agg.any_off_leash THEN 'off_leash'
      WHEN agg.any_on_leash THEN 'on_leash'
      WHEN agg.all_not_allowed THEN 'no_access'
      ELSE NULL
    END,
    -- off_leash_flag, has_on_leash, has_off_leash: cross-section aggregates
    -- (filter signals for find.html — e.g., a beach with sand=on_leash but
    -- a separate off-leash zone should still surface in the "off-leash" filter).
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
  FROM agg, sand_agg;
END;
$function$;

-- Bulk re-promote: re-runs promote_entity_dogs_to_beach_dog_policy for every
-- beach with operative BPS. Pass-2 trigger (20260531_refresh_beaches_from_ps_pass2.sql)
-- ensures zone_rules also stays fresh for any beach whose flat fields change.
-- manual_curator beaches are protected inside the function.
DO $$
DECLARE v_n int;
BEGIN
  WITH r AS (
    SELECT public.promote_entity_dogs_to_beach_dog_policy(beach_fid) AS res
      FROM (SELECT DISTINCT beach_fid FROM public.beach_policy_source) t
  )
  SELECT count(*) INTO v_n FROM r;
  RAISE NOTICE 'Bulk re-promote ran for % beach_fids.', v_n;
END$$;
