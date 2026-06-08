-- 20260607j_tide_grid_reference_layer.sql
--
-- Tide as a reference layer — sibling pattern to weather_grid (W2) and
-- marine_grid. Per Franz 2026-06-07:
--
-- BEFORE: daily-beach-refresh fetched NOAA CO-OPS tide predictions
-- PER BEACH inside its sequential per-beach loop. 3,899 active scoring
-- beaches × ~1s NOAA call = ~65 minutes of work crammed into one edge
-- fn invocation that times out at 150s. Result: 11% CA tide coverage
-- (79 of 718) — the slice the loop got through before dying. Per
-- Franz: "I don't think it runs frequently."
--
-- AFTER: this migration creates the grid. A separate edge fn
-- refresh-tide-stations fetches per-STATION (not per-beach) — 296
-- distinct stations cover all 3,899 beaches (13× redundancy). Weekly
-- cron, ~150 stations / minute = ~2-3 min total. daily-beach-refresh
-- then reads tide from the grid in pure DB (sub-ms). Mirrors the W2
-- weather-grid + marine-grid pattern.
--
-- Coverage scope: ALL stations referenced by any active+scoring beach,
-- including HI (Franz: "include HI"). The legacy weekly_tide_refresh
-- orch job's scope_states list is retired by the new loader, which
-- picks stations via beaches_gold join — implicitly covering every
-- state the catalog includes.
--
-- NOAA CO-OPS predictions are deterministic harmonic computations
-- (not observations), so the cache horizon can be long. Loader writes
-- 14 days forward + 1 day back; weekly refresh keeps it warm.
--
-- Architecture follows [[weather-grid-reference-layer]] +
-- [[grid-consumers-require-cell-warmth-gate]] except the gate threshold
-- is laxer here (tide harmonics don't drift like weather forecasts).

CREATE TABLE IF NOT EXISTS public.tide_grid_hourly (
  station_id      text         NOT NULL,
  forecast_ts     timestamptz  NOT NULL,
  tide_height_ft  numeric(6,3) NOT NULL,
  tide_direction  text,                              -- 'rising' | 'falling' | null
  fetched_at      timestamptz  NOT NULL DEFAULT now(),
  source          text         NOT NULL DEFAULT 'noaa_coops_predictions',
  PRIMARY KEY (station_id, forecast_ts)
);

-- Inventory table — one row per station the catalog references. The
-- loader picks from this; it's auto-populated from beaches_gold by the
-- trigger below. Mirrors weather_grid's first_seen / last_fetched
-- pattern; tide is deterministic so we only need one tier (no t1/t2/t3).
CREATE TABLE IF NOT EXISTS public.tide_station_inventory (
  station_id      text         PRIMARY KEY,
  first_seen      timestamptz  NOT NULL DEFAULT now(),
  last_fetched_at timestamptz,
  last_error      text
);

CREATE INDEX IF NOT EXISTS idx_tide_grid_fetched
  ON public.tide_grid_hourly (fetched_at);

-- ─── Trigger: keep tide_station_inventory in sync with beaches_gold ───
-- Whenever a beach gets a noaa_station_id (insert OR update), ensure
-- the station appears in the inventory. Same shape as
-- tg_compute_weather_grid_cell.
CREATE OR REPLACE FUNCTION public.tg_register_tide_station()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.noaa_station_id IS NOT NULL AND NEW.noaa_station_id <> '' THEN
    INSERT INTO public.tide_station_inventory (station_id)
    VALUES (NEW.noaa_station_id)
    ON CONFLICT (station_id) DO NOTHING;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tg_beaches_gold_register_tide_station ON public.beaches_gold;
CREATE TRIGGER tg_beaches_gold_register_tide_station
  AFTER INSERT OR UPDATE OF noaa_station_id
  ON public.beaches_gold
  FOR EACH ROW
  EXECUTE FUNCTION public.tg_register_tide_station();

-- Backfill from existing beaches.
INSERT INTO public.tide_station_inventory (station_id)
SELECT DISTINCT noaa_station_id
  FROM public.beaches_gold
 WHERE is_active
   AND scoring_tier IN ('daily','hourly')
   AND noaa_station_id IS NOT NULL
   AND noaa_station_id <> ''
ON CONFLICT (station_id) DO NOTHING;


-- ─── Helpers ─────────────────────────────────────────────────────────────

-- Per-station hourly read. Direct consumer entry-point for callers
-- that already know the station_id.
CREATE OR REPLACE FUNCTION public.tide_for_station(
  p_station_id text,
  p_start_ts   timestamptz,
  p_end_ts     timestamptz
)
RETURNS SETOF public.tide_grid_hourly
LANGUAGE sql STABLE PARALLEL SAFE SECURITY DEFINER SET search_path TO 'public'
AS $$
  SELECT *
  FROM public.tide_grid_hourly
  WHERE station_id = p_station_id
    AND forecast_ts >= p_start_ts
    AND forecast_ts <  p_end_ts
  ORDER BY forecast_ts;
$$;

-- Per-beach hourly read. Resolves the beach's noaa_station_id then
-- delegates. Returns 0 rows when the beach has no station (inland) or
-- the cache is cold for that station.
CREATE OR REPLACE FUNCTION public.tide_for_beach(
  p_fid      bigint,
  p_start_ts timestamptz,
  p_end_ts   timestamptz
)
RETURNS SETOF public.tide_grid_hourly
LANGUAGE sql STABLE PARALLEL SAFE SECURITY DEFINER SET search_path TO 'public'
AS $$
  SELECT t.*
  FROM public.beaches_gold g
  JOIN public.tide_grid_hourly t
    ON t.station_id = g.noaa_station_id
  WHERE g.fid = p_fid
    AND g.noaa_station_id IS NOT NULL
    AND t.forecast_ts >= p_start_ts
    AND t.forecast_ts <  p_end_ts
  ORDER BY t.forecast_ts;
$$;

-- Warmth check — used by consumers to gate against partial coverage
-- per [[grid-consumers-require-cell-warmth-gate]]. Returns true when
-- the station has ≥20 of the next 24 hours covered (laxer than the 22
-- threshold for weather since tide is harmonic and rarely partial-
-- written; the cache is either full or empty per the loader contract).
CREATE OR REPLACE FUNCTION public.tide_station_is_warm(
  p_station_id text,
  p_horizon_h  int DEFAULT 24
)
RETURNS boolean
LANGUAGE sql STABLE PARALLEL SAFE SECURITY DEFINER SET search_path TO 'public'
AS $$
  SELECT count(*) >= 20
  FROM public.tide_grid_hourly
  WHERE station_id = p_station_id
    AND forecast_ts >= now()
    AND forecast_ts <  now() + (p_horizon_h || ' hours')::interval;
$$;

-- Loader picker: stations whose cache is stale (older than 6 days)
-- OR has never been fetched. Loader fires this then iterates the
-- returned set, fetching each from NOAA.
CREATE OR REPLACE FUNCTION public._tide_picker_stale_stations(
  p_limit int  DEFAULT 500,
  p_force boolean DEFAULT false
)
RETURNS TABLE (station_id text)
LANGUAGE sql STABLE PARALLEL SAFE SECURITY DEFINER SET search_path TO 'public'
AS $$
  SELECT i.station_id
  FROM public.tide_station_inventory i
  WHERE p_force
     OR i.last_fetched_at IS NULL
     OR i.last_fetched_at < now() - interval '6 days'
  ORDER BY COALESCE(i.last_fetched_at, '1970-01-01'::timestamptz) ASC
  LIMIT p_limit;
$$;

GRANT EXECUTE ON FUNCTION public.tide_for_station(text, timestamptz, timestamptz)   TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.tide_for_beach(bigint, timestamptz, timestamptz)   TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.tide_station_is_warm(text, int)                    TO anon, authenticated;
-- _tide_picker_stale_stations stays service-role only (loader uses it).

-- Anon-readable for browser consumers
ALTER TABLE public.tide_grid_hourly ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tide_grid_hourly_anon_read ON public.tide_grid_hourly;
CREATE POLICY tide_grid_hourly_anon_read ON public.tide_grid_hourly
  FOR SELECT TO anon, authenticated USING (true);

ALTER TABLE public.tide_station_inventory ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tide_station_inventory_anon_read ON public.tide_station_inventory;
CREATE POLICY tide_station_inventory_anon_read ON public.tide_station_inventory
  FOR SELECT TO anon, authenticated USING (true);
