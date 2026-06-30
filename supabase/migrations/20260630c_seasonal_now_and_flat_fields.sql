-- 20260630c_seasonal_now_and_flat_fields.sql
--
-- W1 SEASONAL CORRECTNESS (2026-06-30). The consumer-facing v3 score already
-- honors seasonal dog-closures end-to-end (compute_beach_hourly_v2 →
-- _beach_closures_from_policy → _v2_is_hour_closed reads zone_rules.seasons[],
-- with Oct–Mar wraparound; verified on Playa Tortuga fid 6429: midday rows NULL
-- in-season). Two narrower gaps remain, both rooted in the SEASON-LESS flat
-- beach_dog_policy.dogs_prohibited_start/end fields:
--
--   1. The NOW card (get-beach-now, TS engine) derives isProhibited from the
--      flat fields → in OFF-season it would wrongly show a seasonal beach
--      closed. Fix: get-beach-now calls dogs_closed_for_hour() (added here),
--      which reuses the SAME seasonal closure helpers as the v3 SQL path —
--      zero logic drift.
--   2. The aggregator stamps the daily window of a SEASONAL ('seasonal_and_
--      daily') prohibition into the flat fields with no season attached, so
--      beach.html's Hours line shows it year-round next to the (correct)
--      zone-rules block. Fix: gate the daily_prohib CTE to year-round-only
--      ('daily') windows. Seasonal closures stay surfaced via zone_rules + the
--      v3 score + dogs_closed_for_hour(); the flat fields become honest
--      (year-round daily prohibitions only). beach.html then auto-stops showing
--      season-less seasonal windows.
--
-- Scope = NOW path + display (Franz 2026-06-30). daily-beach-refresh's TS v2
-- path stays season-blind on purpose — v2 is not consumer-facing (v3, written
-- by apply_v2_best_window_to_beach_recommendations_bulk, is), and gating the
-- flat fields does NOT touch v3.

-- 1. seasonal-aware closure RPC for the NOW path ---------------------------
-- Reuses _beach_closures_from_policy + _v2_is_hour_closed (the exact helpers
-- the v3 SQL score uses) so the NOW card and the stored score never disagree.
-- Returns false when the beach has no policy/closures.
CREATE OR REPLACE FUNCTION public.dogs_closed_for_hour(
  p_fid bigint, p_local_date date, p_local_hour integer)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
  SELECT COALESCE((
    SELECT public._v2_is_hour_closed(
             public._beach_closures_from_policy(bdp.dogs_allowed, bdp.zone_rules),
             p_local_date, p_local_hour)
      FROM public.beach_dog_policy bdp
     WHERE bdp.arena_group_id = p_fid
     LIMIT 1
  ), false)
$$;

COMMENT ON FUNCTION public.dogs_closed_for_hour(bigint, date, integer) IS
  'Seasonal-aware "are dogs closed at this beach on this local date+hour?" for the NOW path (get-beach-now). Reuses _beach_closures_from_policy + _v2_is_hour_closed so it matches the v3 SQL score exactly. False when no policy/closure.';

-- 2. gate the flat dogs_prohibited fields to YEAR-ROUND windows only --------
-- Only change vs live: daily_prohib CTE window_kind IN ('daily',
-- 'seasonal_and_daily') → = 'daily'. Everything else is the live def verbatim.
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
      AND window_kind = 'daily'   -- W1 (20260630c): year-round only; seasonal windows surface via zone_rules + v3 + dogs_closed_for_hour
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
