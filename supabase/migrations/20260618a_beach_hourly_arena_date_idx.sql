-- find.html perf: beach_day_hourly_scores is keyed only on location_id, but
-- find_beaches joins every hourly enrichment CTE (peaks, current_hour_score,
-- best-window picker) on arena_group_id + local_date. With no composite index
-- the planner bitmap-scans ~99k rows-by-date per step (peaks alone ~8s on a
-- 2.93M-row table; full no-geo call ~36s cold).
--
-- This mirrors the dog-park sibling's dpdhs_local_date_idx (dog_park_fid,
-- local_date) — the beach table never got parity after the path-3b PK swap
-- (location_id -> arena_group_id).
--
-- CONCURRENTLY (and therefore NO transaction wrapper): the table takes hourly
-- cron writes; a plain CREATE INDEX would hold a SHARE lock and block them.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_beach_day_hourly_scores_arena_date
  ON public.beach_day_hourly_scores (arena_group_id, local_date);
