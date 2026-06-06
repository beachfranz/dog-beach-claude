-- 20260606r_find_beaches_aggregating_rpc.sql
--
-- Aggregating-RPC pattern for find_beaches — option 3 of today's PostgREST
-- silent-truncation triage (sibling fix to yesterday's daily-beach-refresh
-- blackout, encoded as [[paired-functions-port-fixes-both-sides]]).
--
-- Problem before this migration:
--   * find_beaches RPC has no ORDER BY when lat/lng absent and no default
--     LIMIT. With ~3,700 active beaches it always returns the full set →
--     PostgREST silently caps at 1,000 (db-max-rows). The slice returned
--     is whatever Postgres happens to emit first (undefined order).
--   * Edge fn then does .in("arena_group_id", [1,000 ids]) against
--     beach_day_hourly_scores. With ~14 daylight hours per beach,
--     ~14,000 rows are expected → PostgREST again caps at 1,000 →
--     only ~71 beaches receive their hour data. The edge fn synthesizes
--     `bestWindowLabel = "No good window remaining"` for the other ~929,
--     making find.html show "No good window" on almost every card.
--
-- Fix shape (Franz: option 3):
--   * Server-side ORDER BY: distance ASC when lat/lng present (existing
--     KNN), composite_score_v2 DESC NULLS LAST otherwise. The client
--     displays the top ~10 anyway; returning the highest-score head is
--     what users actually see.
--   * Server-side default LIMIT 500. Lets clients pass an explicit p_limit
--     to override (e.g. the spatial-bounded NEAREST_LIMIT=10 path on find.html).
--     The 500 default is well under PostgREST's 1,000 cap and large enough
--     to survive client-side .filter() of distance/leash chips.
--   * NEW parameter `p_now_hour integer DEFAULT NULL` — when supplied
--     (today's view), the RPC computes the best REMAINING window from
--     that hour onward using gap-and-island detection on
--     beach_day_hourly_scores, replacing the edge fn's per-beach TS
--     findBestRemainingWindow loop. When NULL, falls back to stored
--     dr.best_window_label (the day's full best window).
--   * Adds public._format_hour() + public._format_hour_window() helpers
--     mirroring the TS formatHour() / buildWindowLabel() formatters.
--     The TS versions live in get-beaches-find/index.ts:264-272;
--     this migration ports them to SQL so the RPC can emit final labels
--     directly (no per-row formatting in the edge fn).
--
-- Edge fn cleanup (in follow-up TS edit, not this migration):
--   * Drop the beach_day_hourly_scores .in() entirely
--   * Drop the per-fid scoresByFid synthesis loop
--   * Pass `p_now_hour` when date == today (Pacific)
--   * Per-beach unused component scores (tide_score/wind_score/.../temp_score)
--     also dropped from edge fn response — find.html doesn't read them
--     (detail.html uses per-hour versions from get-beach-detail).

-- ─── Hour-formatting helpers (ports of TS formatHour + buildWindowLabel) ───
CREATE OR REPLACE FUNCTION public._format_hour(h integer)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN h % 24 = 0  THEN '12am'
    WHEN h % 24 = 12 THEN '12pm'
    WHEN h % 24 < 12 THEN (h % 24)::text || 'am'
    ELSE ((h % 24) - 12)::text || 'pm'
  END;
$$;

CREATE OR REPLACE FUNCTION public._format_hour_window(start_h integer, end_h integer)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  -- Matches TS buildWindowLabel(): "${formatHour(start)}–${formatHour(end+1)}"
  SELECT public._format_hour(start_h) || '–' || public._format_hour(end_h + 1);
$$;

-- ─── find_beaches RPC rebuild ────────────────────────────────────────────
DROP FUNCTION IF EXISTS public.find_beaches(date, double precision, double precision, text, integer, boolean);
DROP FUNCTION IF EXISTS public.find_beaches(date, double precision, double precision, text, integer, boolean, integer);

CREATE OR REPLACE FUNCTION public.find_beaches(
  p_date          date,
  p_lat           double precision DEFAULT NULL,
  p_lng           double precision DEFAULT NULL,
  p_leash         text             DEFAULT 'any',
  p_limit         integer          DEFAULT 500,
  p_scored_only   boolean          DEFAULT false,
  p_now_hour      integer          DEFAULT NULL
)
RETURNS TABLE (
  arena_group_id        bigint,
  location_id           text,
  display_name          text,
  latitude              double precision,
  longitude             double precision,
  access_rule           text,
  has_on_leash          boolean,
  has_off_leash         boolean,
  dogs_allowed          text,
  dogs_prohibited_start text,
  location_tier         text,
  distance_m            double precision,
  composite_score_v2    integer,
  best_window_label     text,
  best_window_status    text,
  bacteria_risk         text,
  summary_weather       text,
  weather_code          integer,
  lowest_tide_height    numeric,
  avg_temp              numeric,
  avg_wind              numeric,
  busyness_category     text,
  go_hours_count        integer,
  avg_tide_height       numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = 'public'
AS $function$
  WITH
  -- 1. Per-beach base join (no aggregation, one row per beach).
  base AS (
    SELECT
      g.fid AS arena_group_id,
      g.location_id,
      coalesce(g.display_name_override, g.name) AS display_name,
      g.lat::double precision AS latitude,
      g.lon::double precision AS longitude,
      dp.access_rule,
      dp.has_on_leash,
      dp.has_off_leash,
      dp.dogs_allowed,
      dp.dogs_prohibited_start::text AS dogs_prohibited_start,
      public.beach_location_tier(
        dp.dogs_allowed, dp.has_off_leash, dp.has_on_leash,
        dp.dogs_prohibited_start::text
      ) AS location_tier,
      CASE
        WHEN p_lat IS NOT NULL AND p_lng IS NOT NULL
        THEN ST_Distance(g.geom::geography, ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography)
        ELSE NULL
      END AS distance_m,
      dr.composite_score_v2,
      dr.best_window_label AS stored_best_window_label,
      dr.best_window_status,
      dr.bacteria_risk,
      dr.summary_weather,
      dr.weather_code,
      dr.lowest_tide_height,
      dr.avg_temp,
      dr.avg_wind,
      dr.busyness_category,
      dr.go_hours_count,
      dr.avg_tide_height,
      g.geom AS geom
    FROM public.beaches_gold g
    LEFT JOIN public.beach_day_recommendations dr
           ON dr.arena_group_id = g.fid
          AND dr.local_date     = p_date
    LEFT JOIN public.beach_dog_policy dp
           ON dp.arena_group_id = g.fid
    WHERE g.is_active = true
      AND (p_leash = 'any' OR dp.access_rule = p_leash OR dp.access_rule IS NULL)
      AND (NOT p_scored_only OR dr.composite_score_v2 IS NOT NULL)
  ),

  -- 2. Best REMAINING window for "today" — gap-and-island detection on
  --    candidate-window hours from p_now_hour onward. Skipped entirely
  --    when p_now_hour IS NULL (non-today view).
  remaining_islands AS (
    SELECT
      h.arena_group_id,
      h.local_hour,
      h.local_hour - row_number() OVER (
        PARTITION BY h.arena_group_id ORDER BY h.local_hour
      ) AS grp
    FROM public.beach_day_hourly_scores h
    WHERE p_now_hour IS NOT NULL
      AND h.local_date = p_date
      AND h.is_daylight = true
      AND h.is_candidate_window = true
      AND h.local_hour >= p_now_hour
  ),
  remaining_runs AS (
    SELECT
      arena_group_id,
      grp,
      min(local_hour) AS run_start,
      max(local_hour) AS run_end,
      count(*) AS run_len
    FROM remaining_islands
    GROUP BY arena_group_id, grp
  ),
  best_remaining AS (
    -- One row per beach: the longest remaining run (ties broken by
    -- earliest start, matching TS findBestRemainingWindow behavior
    -- which picks the first-occurring peak-anchored window).
    SELECT DISTINCT ON (arena_group_id)
      arena_group_id, run_start, run_end
    FROM remaining_runs
    ORDER BY arena_group_id, run_len DESC, run_start ASC
  )

  SELECT
    b.arena_group_id,
    b.location_id,
    b.display_name,
    b.latitude,
    b.longitude,
    b.access_rule,
    b.has_on_leash,
    b.has_off_leash,
    b.dogs_allowed,
    b.dogs_prohibited_start,
    b.location_tier,
    b.distance_m,
    b.composite_score_v2,
    -- best_window_label:
    --   p_now_hour given + has a remaining run → format that run
    --   p_now_hour given + no run             → 'No good window remaining'
    --   p_now_hour NULL  → stored day-level label from beach_day_recommendations
    CASE
      WHEN p_now_hour IS NOT NULL THEN
        CASE
          WHEN br.run_start IS NOT NULL
            THEN public._format_hour_window(br.run_start::int, br.run_end::int)
          ELSE 'No good window remaining'
        END
      ELSE b.stored_best_window_label
    END AS best_window_label,
    b.best_window_status,
    b.bacteria_risk,
    b.summary_weather,
    b.weather_code,
    b.lowest_tide_height,
    b.avg_temp,
    b.avg_wind,
    b.busyness_category,
    b.go_hours_count,
    b.avg_tide_height
  FROM base b
  LEFT JOIN best_remaining br ON br.arena_group_id = b.arena_group_id
  ORDER BY
    -- distance ASC when lat/lng present (KNN through GIST on g.geom),
    -- composite_score_v2 DESC otherwise (top-scored slice for ghost user).
    CASE
      WHEN p_lat IS NOT NULL AND p_lng IS NOT NULL
      THEN b.geom::geography <-> ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography
      ELSE NULL
    END NULLS LAST,
    CASE
      WHEN p_lat IS NULL AND p_lng IS NULL
      THEN b.composite_score_v2
      ELSE NULL
    END DESC NULLS LAST
  LIMIT COALESCE(NULLIF(p_limit, 0), 500);
$function$;

GRANT EXECUTE ON FUNCTION public.find_beaches(
  date, double precision, double precision, text, integer, boolean, integer
) TO anon, authenticated;
