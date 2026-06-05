-- All-in-one selection RPCs for the daily-refresh edge functions.
--
-- Supersedes 20260605_recommendations_freshness_rpc.sql in the
-- refresh-function call path. The freshness RPC still works as a
-- general utility but the refresh functions now use these.
--
-- Root cause being fixed (deeper than the freshness RPC):
-- PostgREST's `db-max-rows` cap (Supabase default = 1000) applies to
-- BOTH table SELECTs AND function results. The daily-beach-refresh
-- edge function's `from("beaches_gold")` query was silently capped at
-- 1000 of 2889 scoreable beaches — beaches with fids outside the first
-- 1000 (by PostgREST order = PK ASC) were invisible to the function
-- entirely. That explains why fid 6947591 (Luhr Jensen, OR) sat 14
-- days stale and why OR/WA/NH/MI/OH/RI/VA beaches show 80-99%
-- staleness while CA/MA/MD/DE (lower fid ranges) are healthy.
--
-- These RPCs do the entire selection DB-side and return just the ≤limit
-- fids the function should process this fire. Since the result is always
-- ≤ p_limit (typically 40), the PostgREST cap never engages. The edge
-- function then loads details via `.in("fid", returnedFids)` which is
-- also bounded to ≤40 rows.
--
-- Encoded per [[paired-functions-port-fixes-both-sides]]: both RPCs
-- ship together.

CREATE OR REPLACE FUNCTION public.beaches_due_for_refresh(
  p_target_fids        bigint[] DEFAULT NULL,
  p_target_location_ids text[]  DEFAULT NULL,
  p_skip_recent_hours  int      DEFAULT NULL,
  p_limit              int      DEFAULT NULL
)
RETURNS TABLE (
  fid               bigint,
  last_generated_at timestamptz
)
LANGUAGE sql
STABLE
AS $$
  WITH eligible AS (
    SELECT bg.fid
    FROM public.beaches_gold bg
    WHERE bg.is_active
      AND bg.scoring_tier IN ('daily','hourly')
      AND (p_target_fids IS NULL OR bg.fid = ANY(p_target_fids))
      AND (p_target_location_ids IS NULL
           OR bg.location_id = ANY(p_target_location_ids))
  ),
  freshness AS (
    SELECT e.fid, max(r.generated_at) AS last_gen
    FROM eligible e
    LEFT JOIN public.beach_day_recommendations r
      ON r.arena_group_id = e.fid
    GROUP BY e.fid
  )
  SELECT f.fid, f.last_gen
  FROM freshness f
  WHERE p_skip_recent_hours IS NULL
     OR f.last_gen IS NULL
     OR f.last_gen < now() - (p_skip_recent_hours * interval '1 hour')
  ORDER BY f.last_gen NULLS FIRST
  LIMIT COALESCE(p_limit, 100000);
$$;

COMMENT ON FUNCTION public.beaches_due_for_refresh(bigint[], text[], int, int) IS
'All-in-one selection for daily-beach-refresh. Returns up to p_limit fids '
'sorted oldest-rec-first, after applying scoring_tier + is_active filters, '
'optional target_fids/location_ids filters, and optional skip_recent_hours '
'gate (on generated_at). Bypasses PostgREST 1000-row cap by doing selection '
'DB-side and returning only the small set the function needs to process.';


CREATE OR REPLACE FUNCTION public.dog_parks_due_for_refresh(
  p_target_fids       bigint[] DEFAULT NULL,
  p_state_filter      text[]   DEFAULT NULL,
  p_skip_recent_hours int      DEFAULT NULL,
  p_limit             int      DEFAULT NULL
)
RETURNS TABLE (
  fid               bigint,
  last_generated_at timestamptz
)
LANGUAGE sql
STABLE
AS $$
  WITH eligible AS (
    SELECT dpg.fid
    FROM public.dog_parks_gold dpg
    WHERE dpg.is_active
      AND dpg.is_scoreable
      AND (p_target_fids IS NULL OR dpg.fid = ANY(p_target_fids))
      AND (p_state_filter IS NULL OR dpg.state = ANY(p_state_filter))
  ),
  freshness AS (
    SELECT e.fid, max(r.generated_at) AS last_gen
    FROM eligible e
    LEFT JOIN public.dog_park_day_recommendations r
      ON r.dog_park_fid = e.fid
    GROUP BY e.fid
  )
  SELECT f.fid, f.last_gen
  FROM freshness f
  WHERE p_skip_recent_hours IS NULL
     OR f.last_gen IS NULL
     OR f.last_gen < now() - (p_skip_recent_hours * interval '1 hour')
  ORDER BY f.last_gen NULLS FIRST
  LIMIT COALESCE(p_limit, 100000);
$$;

COMMENT ON FUNCTION public.dog_parks_due_for_refresh(bigint[], text[], int, int) IS
'Mirror of beaches_due_for_refresh for dog parks. Same pagination-bypass '
'rationale. p_state_filter is the MVP+ state list (defaults to no filter '
'if NULL; cron passes [CA, OR, WA, MD, UT]).';
