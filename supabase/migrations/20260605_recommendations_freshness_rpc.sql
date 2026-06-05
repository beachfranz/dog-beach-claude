-- Aggregating freshness RPCs for the daily-refresh edge functions.
--
-- Root cause being fixed: daily-beach-refresh + daily-dog-park-refresh
-- both queried beach_day_recommendations / dog_park_day_recommendations
-- via PostgREST `.select("arena_group_id, generated_at, updated_at")`
-- to drive skip-recent + limit-sort. Each beach has ~10 rec rows (one
-- per local_date), so the scoreable pool produces ~51k beach rec rows
-- + ~7.8k dog-park rec rows. PostgREST's default 1000-row cap
-- truncated both queries silently — ~50k rows were invisible to the
-- skip-recent filter AND the limit-sort age comparison.
--
-- Effect: beaches outside the 1000-row window had ageBy[]=undefined,
-- defaulted to epoch=0, all tied for "oldest" in the limit=40 sort,
-- and were never deterministically picked. Result: ~1,889 beaches
-- stuck >7 days stale today (mostly OR/WA/NH/MI/OH/RI/VA — the states
-- whose rows live outside the 1000-row PostgREST window).
--
-- These RPCs return ONE aggregated row per fid (max generated_at,
-- max updated_at), eliminating the pagination issue entirely. Edge
-- functions switch to `supabase.rpc(...)` and read both columns from
-- the same call. Comment carries the root cause for future readers.
--
-- Encoded per [[paired-functions-port-fixes-both-sides]]: the same bug
-- shape exists on both pipelines; both RPCs ship together.

CREATE OR REPLACE FUNCTION public.beach_recommendations_freshness(
  p_fids bigint[]
)
RETURNS TABLE (
  arena_group_id bigint,
  last_generated_at timestamptz,
  last_updated_at timestamptz
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    arena_group_id,
    max(generated_at) AS last_generated_at,
    max(updated_at)   AS last_updated_at
  FROM public.beach_day_recommendations
  WHERE arena_group_id = ANY(p_fids)
  GROUP BY arena_group_id;
$$;

COMMENT ON FUNCTION public.beach_recommendations_freshness(bigint[]) IS
'Per-fid aggregated freshness for daily-beach-refresh. Returns one row '
'per input fid with max(generated_at) and max(updated_at) across all '
'local_dates. Used to drive skip_recent_hours filter + limit-sort '
'ordering. Avoids the PostgREST 1000-row pagination truncation that '
'previously caused beaches outside the page to stick stale indefinitely.';


CREATE OR REPLACE FUNCTION public.dog_park_recommendations_freshness(
  p_fids bigint[]
)
RETURNS TABLE (
  dog_park_fid bigint,
  last_generated_at timestamptz,
  last_updated_at timestamptz
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    dog_park_fid,
    max(generated_at) AS last_generated_at,
    max(updated_at)   AS last_updated_at
  FROM public.dog_park_day_recommendations
  WHERE dog_park_fid = ANY(p_fids)
  GROUP BY dog_park_fid;
$$;

COMMENT ON FUNCTION public.dog_park_recommendations_freshness(bigint[]) IS
'Mirror of beach_recommendations_freshness for dog parks. Same '
'pagination-avoidance rationale; per [[paired-functions-port-fixes-both-sides]].';
