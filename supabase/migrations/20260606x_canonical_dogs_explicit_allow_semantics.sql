-- 20260606x_canonical_dogs_explicit_allow_semantics.sql
--
-- Cascade fix #3: handle beaches where the winning policy source mixes
-- a `not_allowed` rule with neutral/conditional rules like
-- `waste_pickup_required`.
--
-- THE BUG (follow-up to 20260606u + 20260606v)
-- The resolver decides `dogs_allowed='no'` only when
-- `all_not_allowed = bool_and(rule = 'not_allowed')` is TRUE. But many
-- sources publish multiple rules per (section, region) — typically
-- `not_allowed` alongside a meta rule like `waste_pickup_required` or
-- `nuisance_restriction`. Those neutral rules break `bool_and`, so
-- `all_not_allowed` flips to FALSE. The resolver then also can't reach
-- 'yes'/'mixed' because none of the rules are explicit allow rules
-- (`on_leash`, `off_leash`, etc.) — and falls through to NULL.
--
-- 42 famous WA beaches (Olympic NP: Shi Shi, Ruby, Second, Third Beach;
-- Lake Crescent; Seattle "Street End" beaches; etc.) sit at
-- `scoring_tier='none'` for exactly this reason — the cascade can't
-- decide.
--
-- THE FIX
-- Replace `all_not_allowed` semantics with `any_explicit_allow`:
--   * any_explicit_allow = bool_or(rule IN ('off_leash',
--       'off_leash_voice_control','on_leash','on_leash_or_voice'))
--   * any_not_allowed    = bool_or(rule = 'not_allowed')
--
--   dogs_allowed CASE:
--     multi_region_split          -> 'mixed'  (Leo Carrillo)
--     any_explicit_allow + any_not_allowed -> 'mixed'
--     any_explicit_allow                    -> 'yes'
--     any_not_allowed                       -> 'no'
--     else                                  -> NULL
--
-- Neutral/meta rules (waste_pickup_required, nuisance_restriction,
-- collar_tag_required, etc.) no longer disrupt the decision — they just
-- contribute nothing to the dogs_allowed verdict, which is correct (they
-- don't say "dogs allowed" or "dogs not allowed" on their own).
--
-- `on_leash_or_voice` (72 rows in BPS) is now recognized as an explicit
-- allow rule alongside `on_leash` — it was previously missed by the
-- `any_on_leash` filter. Same for the leash_policy CASE.
--
-- Shi Shi Beach after fix: any_not_allowed=TRUE, any_explicit_allow=FALSE
-- -> dogs_allowed='no' (correct — Olympic NP prohibits dogs on these
-- beaches). beach_location_tier returns '4_no_dogs'; scoring_tier stays
-- 'none', but now for the RIGHT reason (legit no-dogs beach), not a
-- cascade-stuck-NULL silent failure.
--
-- Other beaches with on_leash + waste_pickup_required (etc.) at the same
-- source will now correctly resolve to 'yes'.

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
    SELECT section, MIN(tier) AS section_min_tier
      FROM section_region_canonical GROUP BY section
  ),
  agg AS (
    SELECT
      (SELECT count(*)::int FROM per_section)                AS n_sections,
      (SELECT MIN(section_min_tier) FROM per_section)        AS min_tier,
      -- Off-leash includes voice-control variant.
      bool_or(rule IN ('off_leash','off_leash_voice_control')) AS any_off_leash,
      -- On-leash includes the "leash-or-voice" variant.
      bool_or(rule IN ('on_leash','on_leash_or_voice'))        AS any_on_leash,
      -- Any rule that EXPLICITLY allows dogs (allow tier).
      bool_or(rule IN ('off_leash','off_leash_voice_control',
                       'on_leash','on_leash_or_voice'))        AS any_explicit_allow,
      -- Any explicit prohibition.
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
    -- dogs_allowed: explicit-allow vs explicit-deny semantics.
    -- Neutral rules (waste_pickup_required, etc.) no longer cause the
    -- resolver to fall through to NULL.
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

-- Re-promote all entity_promoted / consensus rows so the new semantics
-- propagate to beach_dog_policy.
SELECT public.promote_entity_dogs_to_beach_dog_policy(arena_group_id)
  FROM public.beach_dog_policy
 WHERE source IN ('entity_promoted', 'auto_promoted_from_consensus');

SELECT * FROM public.refresh_scoring_tier();
