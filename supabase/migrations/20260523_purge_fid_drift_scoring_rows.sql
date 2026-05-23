-- Purge fid-drift orphan rows in beach_day_hourly_scores +
-- beach_day_recommendations (gap #37 / #185, 2026-05-23 LATE).
--
-- Discovery: DE virgin-state test ran promote, which re-fid'd 5 DE beaches
-- via GNIS ingestion (location_ids were already in beach_day_hourly_scores
-- + beach_day_recommendations from prior runs, but under their OLD
-- arena_group_id). The daily-beach-refresh edge function then tried to
-- upsert today's rows under the NEW arena_group_id; ON CONFLICT
-- (arena_group_id, forecast_ts) missed because arena_group_id differed,
-- so it fell through to INSERT — which then tripped the partial unique
-- index on (location_id, forecast_ts) WHERE location_id IS NOT NULL.
--
-- Net: 1204 stale beach_day_hourly_scores rows + 50 stale
-- beach_day_recommendations rows for 5 DE beaches, blocking refresh of
-- the new fids.
--
-- This migration:
--   1) Deletes rows where the (location_id, arena_group_id) pair no
--      longer matches beaches_gold.fid — i.e. the arena_group_id is
--      stale relative to the current canonical fid for that location.
--   2) Creates a v_scoring_fid_drift view so future drift is visible
--      to audit tooling without re-deriving the join.
--
-- DOES NOT fix the upstream cause (promote phase doesn't sweep
-- downstream scoring tables when it re-fids a beach — gap #39 / task
-- #186). The edge function gets a self-healing DELETE-then-upsert in a
-- parallel change so future drift doesn't reproduce this failure mode.
--
-- Reversibility: data is gone after this runs. The stale rows held
-- scoring outputs from pre-re-fid pipelines; current scoring runs will
-- regenerate them under the correct arena_group_id.

BEGIN;

-- 1a. Purge stale beach_day_hourly_scores
WITH stale AS (
  SELECT bhs.ctid
  FROM public.beach_day_hourly_scores bhs
  JOIN public.beaches_gold bg ON bg.location_id = bhs.location_id
  WHERE bhs.arena_group_id <> bg.fid
)
DELETE FROM public.beach_day_hourly_scores bhs
USING stale s
WHERE bhs.ctid = s.ctid;

-- 1b. Purge stale beach_day_recommendations
WITH stale AS (
  SELECT bdr.ctid
  FROM public.beach_day_recommendations bdr
  JOIN public.beaches_gold bg ON bg.location_id = bdr.location_id
  WHERE bdr.arena_group_id <> bg.fid
)
DELETE FROM public.beach_day_recommendations bdr
USING stale s
WHERE bdr.ctid = s.ctid;

-- 2. Drift-detection view (audit-friendly)
CREATE OR REPLACE VIEW public.v_scoring_fid_drift AS
SELECT
  'beach_day_hourly_scores'::text AS table_name,
  bhs.location_id,
  bhs.arena_group_id AS stale_arena_group_id,
  bg.fid AS current_fid,
  count(*) AS stale_row_count
FROM public.beach_day_hourly_scores bhs
JOIN public.beaches_gold bg ON bg.location_id = bhs.location_id
WHERE bhs.arena_group_id <> bg.fid
GROUP BY bhs.location_id, bhs.arena_group_id, bg.fid
UNION ALL
SELECT
  'beach_day_recommendations'::text AS table_name,
  bdr.location_id,
  bdr.arena_group_id AS stale_arena_group_id,
  bg.fid AS current_fid,
  count(*) AS stale_row_count
FROM public.beach_day_recommendations bdr
JOIN public.beaches_gold bg ON bg.location_id = bdr.location_id
WHERE bdr.arena_group_id <> bg.fid
GROUP BY bdr.location_id, bdr.arena_group_id, bg.fid;

COMMENT ON VIEW public.v_scoring_fid_drift IS
  'Rows in scoring tables whose arena_group_id no longer matches the '
  'canonical beaches_gold.fid for the same location_id. Non-empty result '
  'means the promote phase re-fid''d a beach without sweeping downstream '
  '(gap #39 / task #186). The daily-beach-refresh edge function delete-'
  'before-upsert pattern handles new churn going forward; this view '
  'catches accumulated drift from older promote runs.';

COMMIT;
