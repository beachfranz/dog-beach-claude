-- Fix NOW-refresh RPCs:
--   1. p_skip_recent_hours: int → numeric (cron passes 0.9 = 54 min)
--   2. Filter hourly_scores BEFORE the join (was timing out; LEFT JOIN
--      + FILTER over ~1.7M beach_day_hourly_scores rows hits statement
--      timeout. Restructured to pre-filter is_now=true into a CTE so
--      the LEFT JOIN sees ≤ pool-size rows.)
--
-- Bug 1 introduced by 20260605h: typed p_skip_recent_hours as int but
-- cron payload is 0.9. PostgreSQL fails: 'invalid input syntax for
-- type integer: "0.9"'. 96 HTTP 500s in 4h; both NOW jobs offline
-- since 2026-06-05 17:50.
--
-- Bug 2 surfaced after fixing #1: my original LEFT JOIN + FILTER
-- pattern on hourly_scores aggregated over ALL rows per fid (any
-- is_now value). On 1.7M rows × 700 beaches that times out.
-- Pre-filtering to is_now=true in a CTE drops the LEFT JOIN feed
-- to ~700 rows total.
--
-- DROP the int-typed overloads first; CREATE OR REPLACE only replaces
-- when the signature matches exactly. Without these DROPs we end up
-- with two overloads and PostgREST can't choose.
--
-- Per [[paired-functions-port-fixes-both-sides]]: both NOW RPCs
-- patched together.

DROP FUNCTION IF EXISTS public.dog_parks_due_for_now_refresh(
  bigint[], text[], int, int);
DROP FUNCTION IF EXISTS public.beaches_due_for_now_refresh(
  bigint[], text[], int, int);


CREATE OR REPLACE FUNCTION public.dog_parks_due_for_now_refresh(
  p_target_fids       bigint[] DEFAULT NULL,
  p_state_filter      text[]   DEFAULT NULL,
  p_skip_recent_hours numeric  DEFAULT NULL,
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
  recent AS (
    SELECT dog_park_fid AS fid, max(updated_at) AS last_now_at
    FROM public.dog_park_day_hourly_scores
    WHERE is_now
      AND dog_park_fid IN (SELECT fid FROM eligible)
    GROUP BY dog_park_fid
  )
  SELECT e.fid, r.last_now_at
  FROM eligible e
  LEFT JOIN recent r ON r.fid = e.fid
  WHERE p_skip_recent_hours IS NULL
     OR r.last_now_at IS NULL
     OR r.last_now_at < now() - (p_skip_recent_hours * interval '1 hour')
  ORDER BY r.last_now_at NULLS FIRST
  LIMIT COALESCE(p_limit, 100000);
$$;


CREATE OR REPLACE FUNCTION public.beaches_due_for_now_refresh(
  p_target_fids        bigint[] DEFAULT NULL,
  p_target_location_ids text[]  DEFAULT NULL,
  p_skip_recent_hours  numeric  DEFAULT NULL,
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
  recent AS (
    SELECT arena_group_id AS fid, max(updated_at) AS last_now_at
    FROM public.beach_day_hourly_scores
    WHERE is_now
      AND arena_group_id IN (SELECT fid FROM eligible)
    GROUP BY arena_group_id
  )
  SELECT e.fid, r.last_now_at
  FROM eligible e
  LEFT JOIN recent r ON r.fid = e.fid
  WHERE p_skip_recent_hours IS NULL
     OR r.last_now_at IS NULL
     OR r.last_now_at < now() - (p_skip_recent_hours * interval '1 hour')
  ORDER BY r.last_now_at NULLS FIRST
  LIMIT COALESCE(p_limit, 100000);
$$;
