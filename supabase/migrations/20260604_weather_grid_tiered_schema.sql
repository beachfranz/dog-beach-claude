-- Tiered weather-grid refresh schema.
--
-- Replaces the single `last_fetched_at` model (and the LATERAL-aggregate picker
-- _orch_pick_stale_weather_cells that was timing out at ~14s on 1.9M
-- weather_grid_hourly rows) with three per-cell timestamps tracking when each
-- forecast horizon was last refreshed:
--
--   last_fetched_t1 — next 48h forecast, target refresh every 1h
--   last_fetched_t2 — hours 48-96 forecast, target every 6h
--   last_fetched_t3 — hours 96-168 forecast + past_days=3 observations, every 12h
--
-- Each tier's picker is a flat scan of weather_grid (4,350 rows) on its column.
-- Per-tier btree index keeps it sub-100ms.

BEGIN;

ALTER TABLE public.weather_grid
  ADD COLUMN IF NOT EXISTS last_fetched_t1 timestamptz,
  ADD COLUMN IF NOT EXISTS last_fetched_t2 timestamptz,
  ADD COLUMN IF NOT EXISTS last_fetched_t3 timestamptz;

COMMENT ON COLUMN public.weather_grid.last_fetched_t1 IS
  'Last fetched-at for the next-48h forecast window. Bumped by refresh-weather-grid tier=t1.';
COMMENT ON COLUMN public.weather_grid.last_fetched_t2 IS
  'Last fetched-at for hours 48-96 of the forecast window. Bumped by refresh-weather-grid tier=t2.';
COMMENT ON COLUMN public.weather_grid.last_fetched_t3 IS
  'Last fetched-at for hours 96-168 of the forecast window + past_days=3 observations. Bumped by refresh-weather-grid tier=t3.';

-- NULLS FIRST so brand-new (never-fetched) cells drain first on each cron.
CREATE INDEX IF NOT EXISTS weather_grid_t1_stale_idx
  ON public.weather_grid (last_fetched_t1 NULLS FIRST);
CREATE INDEX IF NOT EXISTS weather_grid_t2_stale_idx
  ON public.weather_grid (last_fetched_t2 NULLS FIRST);
CREATE INDEX IF NOT EXISTS weather_grid_t3_stale_idx
  ON public.weather_grid (last_fetched_t3 NULLS FIRST);

-- ── Per-tier picker functions ────────────────────────────────────────
-- Three near-duplicate functions instead of one parameterized one so the
-- planner can pick a clean index scan per call. STABLE PARALLEL SAFE.

CREATE OR REPLACE FUNCTION public._orch_pick_stale_weather_cells_t1(
  p_limit int DEFAULT 600,
  p_state text DEFAULT NULL,
  p_force boolean DEFAULT false
) RETURNS TABLE(grid_lat numeric, grid_lon numeric)
LANGUAGE sql STABLE PARALLEL SAFE
AS $$
  SELECT g.grid_lat, g.grid_lon
    FROM public.weather_grid g
   WHERE (p_state IS NULL OR g.state = p_state)
     AND (p_force
          OR g.last_fetched_t1 IS NULL
          OR g.last_fetched_t1 < now() - interval '1 hour')
   ORDER BY g.last_fetched_t1 NULLS FIRST
   LIMIT p_limit;
$$;

CREATE OR REPLACE FUNCTION public._orch_pick_stale_weather_cells_t2(
  p_limit int DEFAULT 600,
  p_state text DEFAULT NULL,
  p_force boolean DEFAULT false
) RETURNS TABLE(grid_lat numeric, grid_lon numeric)
LANGUAGE sql STABLE PARALLEL SAFE
AS $$
  SELECT g.grid_lat, g.grid_lon
    FROM public.weather_grid g
   WHERE (p_state IS NULL OR g.state = p_state)
     AND (p_force
          OR g.last_fetched_t2 IS NULL
          OR g.last_fetched_t2 < now() - interval '6 hours')
   ORDER BY g.last_fetched_t2 NULLS FIRST
   LIMIT p_limit;
$$;

CREATE OR REPLACE FUNCTION public._orch_pick_stale_weather_cells_t3(
  p_limit int DEFAULT 600,
  p_state text DEFAULT NULL,
  p_force boolean DEFAULT false
) RETURNS TABLE(grid_lat numeric, grid_lon numeric)
LANGUAGE sql STABLE PARALLEL SAFE
AS $$
  SELECT g.grid_lat, g.grid_lon
    FROM public.weather_grid g
   WHERE (p_state IS NULL OR g.state = p_state)
     AND (p_force
          OR g.last_fetched_t3 IS NULL
          OR g.last_fetched_t3 < now() - interval '12 hours')
   ORDER BY g.last_fetched_t3 NULLS FIRST
   LIMIT p_limit;
$$;

COMMIT;
