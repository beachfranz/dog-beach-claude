-- NOW-refresh selection RPCs — mirror of daily-refresh RPCs (20260605b).
--
-- Why: 2026-06-05 LATE — lifting the MVP+ state hardcode in
-- daily-dog-park-refresh + get-dog-park-now (commit 57865e4) expanded
-- the dog-park pool from ~782 to ~2,589 nationally. get-dog-park-now's
-- `from("dog_parks_gold").eq("is_active",true).eq("is_scoreable",true)`
-- now returns ~2,589 → silently capped at PostgREST db-max-rows=1000.
-- ~1,589 parks invisible to NOW refresh.
--
-- get-beach-now's equivalent query loads only `scoring_tier='hourly'`
-- (~702 beaches today) so it's under the cap, but future scoring tier
-- expansion will trip the same bug. Building both sides in lockstep
-- per [[paired-functions-port-fixes-both-sides]].
--
-- NOW-specific freshness: looks at
-- {beach,dog_park}_day_hourly_scores.updated_at WHERE is_now=true
-- (the timestamp of the LAST NOW refresh for that entity). This is
-- different from the daily-refresh RPCs which look at recommendations
-- .generated_at.

CREATE OR REPLACE FUNCTION public.dog_parks_due_for_now_refresh(
  p_target_fids       bigint[] DEFAULT NULL,
  p_state_filter      text[]   DEFAULT NULL,
  p_skip_recent_hours int      DEFAULT NULL,
  p_limit             int      DEFAULT NULL
)
RETURNS TABLE (
  fid          bigint,
  last_now_at  timestamptz
)
LANGUAGE sql
STABLE
AS $$
  WITH eligible AS (
    SELECT dpg.fid
    FROM public.dog_parks_gold dpg
    WHERE dpg.is_active AND dpg.is_scoreable
      AND (p_target_fids IS NULL OR dpg.fid = ANY(p_target_fids))
      AND (p_state_filter IS NULL OR dpg.state = ANY(p_state_filter))
  ),
  freshness AS (
    SELECT
      e.fid,
      max(s.updated_at) FILTER (WHERE s.is_now) AS last_now_at
    FROM eligible e
    LEFT JOIN public.dog_park_day_hourly_scores s
      ON s.dog_park_fid = e.fid
    GROUP BY e.fid
  )
  SELECT f.fid, f.last_now_at
  FROM freshness f
  WHERE p_skip_recent_hours IS NULL
     OR f.last_now_at IS NULL
     OR f.last_now_at < now() - (p_skip_recent_hours * interval '1 hour')
  ORDER BY f.last_now_at NULLS FIRST
  LIMIT COALESCE(p_limit, 100000);
$$;

COMMENT ON FUNCTION public.dog_parks_due_for_now_refresh(bigint[], text[], int, int) IS
'NOW-refresh selection: returns ≤ p_limit dog park fids ordered by oldest is_now hourly_scores row, after applying is_active+is_scoreable + optional fid/state filters + optional skip_recent_hours (against hourly_scores.updated_at WHERE is_now=true). Mirrors dog_parks_due_for_refresh shape but uses NOW-specific freshness signal.';


CREATE OR REPLACE FUNCTION public.beaches_due_for_now_refresh(
  p_target_fids        bigint[] DEFAULT NULL,
  p_target_location_ids text[]  DEFAULT NULL,
  p_skip_recent_hours  int      DEFAULT NULL,
  p_limit              int      DEFAULT NULL
)
RETURNS TABLE (
  fid          bigint,
  last_now_at  timestamptz
)
LANGUAGE sql
STABLE
AS $$
  WITH eligible AS (
    SELECT bg.fid
    FROM public.beaches_gold bg
    WHERE bg.is_active
      AND bg.scoring_tier = 'hourly'
      AND (p_target_fids IS NULL OR bg.fid = ANY(p_target_fids))
      AND (p_target_location_ids IS NULL
           OR bg.location_id = ANY(p_target_location_ids))
  ),
  freshness AS (
    SELECT
      e.fid,
      max(s.updated_at) FILTER (WHERE s.is_now) AS last_now_at
    FROM eligible e
    LEFT JOIN public.beach_day_hourly_scores s
      ON s.arena_group_id = e.fid
    GROUP BY e.fid
  )
  SELECT f.fid, f.last_now_at
  FROM freshness f
  WHERE p_skip_recent_hours IS NULL
     OR f.last_now_at IS NULL
     OR f.last_now_at < now() - (p_skip_recent_hours * interval '1 hour')
  ORDER BY f.last_now_at NULLS FIRST
  LIMIT COALESCE(p_limit, 100000);
$$;

COMMENT ON FUNCTION public.beaches_due_for_now_refresh(bigint[], text[], int, int) IS
'Mirror of dog_parks_due_for_now_refresh for beaches. Pre-emptive: beach hourly-tier pool is under the cap today (~702) but will trip the bug as scoring_tier=hourly grows. Per [[paired-functions-port-fixes-both-sides]].';
