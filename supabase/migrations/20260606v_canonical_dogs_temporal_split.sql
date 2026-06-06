-- 20260606v_canonical_dogs_temporal_split.sql
--
-- Cascade fix #2: handle temporal-split rules within a single authority source.
--
-- THE BUG (follow-up to 20260606u)
-- When a single policy source encodes a daily split (e.g. Newport sand:
-- on_leash before 10 a.m. and after 4:30 p.m., not_allowed 10–4:30), the
-- pipeline writes ONE beach_policy_source row per time-window slice
-- (linked back from beach_policy_source_temporal.bps_id). For Santa Ana
-- River Jetties + Newport Municipal Beach, that's two rows per sand/walkway
-- section: one `on_leash`, one `not_allowed`, both from the same source
-- at the same tier.
--
-- The resolver's `DISTINCT ON (section, region)` picked ONE row per
-- (section, region), throwing away the other temporal half. When the
-- pick happened to be `not_allowed`, the resolver concluded
-- `agg.all_not_allowed = TRUE` for the whole beach -> `dogs_allowed='no'`
-- -> beach_location_tier returns `4_no_dogs` -> scoring_tier demotes to
-- 'none'. The temporal window survived in dogs_prohibited_start/end but
-- the consumer surface never saw "dogs allowed outside the window".
--
-- THE FIX
-- Two-step rule picking:
--   1. winning_source: DISTINCT ON (section, region) picks the WINNING
--      POLICY SOURCE per (section, region) by tier priority. This is the
--      original priority logic, preserved intact.
--   2. section_region_canonical: collects ALL rules from the winning
--      source for that (section, region). Temporal-split siblings come
--      through together. Cross-section aggregates (any_allowed,
--      any_not_allowed, etc.) now see the full picture.
--
-- After this fix, Santa Ana River Jetties:
--   sand:    {on_leash, not_allowed} from source 147 -> mixed
--   walkway: {on_leash, not_allowed} from source 147 -> mixed
--   global:  {not_allowed}                            -> not_allowed
--   agg.any_allowed = TRUE (on_leash exists somewhere)
--   agg.any_not_allowed = TRUE
--   -> dogs_allowed = 'mixed'
--   has_on_leash = TRUE (cross-section)
--   beach_location_tier('mixed', has_off=false, has_on=true) -> '3_limited_access'
--   refresh_scoring_tier(tier 3, state_pct=0.7341 >= 0.40) -> 'daily'
--
-- dogs_prohibited_start/end (10:00 / 16:30) already populated -> the
-- consumer surface continues to show the prohibition window.
--
-- Cross-source semantics unchanged: when DIFFERENT sources disagree, the
-- higher-tier source still wins (because winning_source still uses
-- DISTINCT ON by tier).

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
  -- Step 1: per (section, region), find the WINNING policy source by tier.
  --         DISTINCT ON returns one row per (section, region) — preserving
  --         the original priority logic for cross-source disagreements.
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
    ORDER BY
      bps.section,
      COALESCE(bps.region_name, '__default__'),
      public.policy_source_effective_tier_for_beach(bps.policy_source_id, bps.beach_fid) ASC,
      ps.effective_date DESC NULLS LAST,
      bps.last_verified DESC NULLS LAST
  ),
  -- Step 2: pull ALL rules from the winning source for each (section, region).
  --         When the winning source encodes a temporal split (multiple bps
  --         rows for the same section, e.g. on_leash before 10am +
  --         not_allowed 10-4:30 + on_leash after 4:30), all the rules
  --         come through here. Cross-section aggregation in `agg` then
  --         correctly sees both `any_allowed` and `any_not_allowed`.
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
    -- dogs_allowed (cross-section + temporal-split-aware):
    CASE
      WHEN (SELECT is_multi_region_split FROM multi_region) THEN 'mixed'
      WHEN agg.all_not_allowed                                THEN 'no'
      WHEN agg.any_allowed AND agg.any_not_allowed            THEN 'mixed'
      WHEN agg.any_allowed                                    THEN 'yes'
      ELSE NULL
    END,
    -- leash_policy (same semantics):
    CASE
      WHEN (SELECT is_multi_region_split FROM multi_region)   THEN 'mixed'
      WHEN agg.any_off_leash AND agg.any_on_leash             THEN 'mixed'
      WHEN agg.any_off_leash                                  THEN 'off_leash'
      WHEN agg.any_on_leash                                   THEN 'on_leash'
      WHEN agg.all_not_allowed                                THEN 'no_access'
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

-- Re-run the entity promoter for beaches that have temporal-split rules,
-- which is the population most affected by this fix.
SELECT public.promote_entity_dogs_to_beach_dog_policy(arena_group_id)
  FROM public.beach_dog_policy
 WHERE source IN ('entity_promoted', 'auto_promoted_from_consensus');

SELECT * FROM public.refresh_scoring_tier();
