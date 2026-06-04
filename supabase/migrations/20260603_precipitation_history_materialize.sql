-- Materialize public.precipitation_history as a real table.
--
-- Why: this is step 1 of a 4-step paring-down of the weather grid pipeline.
-- The picker query in refresh-weather-grid is timing out because we re-fetch
-- past_days=3 every hour just to keep the bacteria_risk signal alive. The
-- end goal (step 4) is to drop past_days=3 from the hourly weather fetch.
--
-- Step 1 (this migration) prepares for that by giving past-observation data
-- its own storage instead of being a view over weather_grid_hourly. After
-- this lands, behavior is unchanged — the table holds exactly the same
-- aggregation the view computed. But once a separate daily refresh job
-- (step 2) starts populating it directly, weather_grid_hourly is no longer
-- the source of truth for past observations, and we can drop past_days=3.
--
-- Existing in-production consumer of this chain:
--   daily-dog-park-refresh/index.ts:294 — mud-caution check on dirt-surface
--   parks via supabase.rpc("precip_72h_for_point"). After this migration the
--   RPC reads the table instead of the view; same contract, same values.
--
-- Future consumer (step 3):
--   daily-beach-refresh will be wired to call precip_72h_for_point() for
--   bacteria_risk, replacing its current inline computePrecipForDay() 72h sum.
--
-- Baseline before migration (for post-apply verification):
--   53,734 rows · 4,350 distinct cells · dates 2026-05-21 → 2026-06-03

BEGIN;

-- Capture the view's current output before we drop it.
CREATE TEMP TABLE _precip_hist_backfill AS
SELECT * FROM public.precipitation_history;

DROP VIEW public.precipitation_history;

CREATE TABLE public.precipitation_history (
  grid_lat         numeric     NOT NULL,
  grid_lon         numeric     NOT NULL,
  observation_date date        NOT NULL,
  precip_mm        numeric     NOT NULL,
  computed_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (grid_lat, grid_lon, observation_date)
);

COMMENT ON TABLE public.precipitation_history IS
  'Daily PT-local precipitation totals per weather_grid cell. Materialized 2026-06-03 from the view of the same name (which aggregated weather_grid_hourly WHERE is_observed=true). Once the step-2 refresh-precipitation-history edge function ships, this table is owned by that job — weather_grid_hourly will stop carrying past observations. Read via precip_72h_for_point(lat, lng, anchor) RPC.';

-- Index for the precip_72h_for_point() filter pattern: it scans by
-- (grid_lat, grid_lon) and observation_date range. PK already covers
-- the cell-then-date sort; this descending-date index speeds the
-- "recent 3 days" trailing window.
CREATE INDEX precipitation_history_recent_idx
  ON public.precipitation_history (grid_lat, grid_lon, observation_date DESC);

-- Backfill from the snapshot.
INSERT INTO public.precipitation_history
  (grid_lat, grid_lon, observation_date, precip_mm)
SELECT grid_lat, grid_lon, observation_date, precip_mm
  FROM _precip_hist_backfill;

-- The view was anon-readable as part of public via the default-grant
-- pattern. The table inherits no GRANTs — leave it that way; only
-- service_role + edge functions need to read it via the RPC, which is
-- already SECURITY DEFINER per its STABLE definition. Anon does not need
-- direct table access. If a future browser-side feature needs it, grant
-- explicitly then.

COMMIT;
