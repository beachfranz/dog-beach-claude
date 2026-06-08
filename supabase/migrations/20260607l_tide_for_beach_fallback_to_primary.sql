-- 20260607l_tide_for_beach_fallback_to_primary.sql
--
-- Defensive fallback for tide reads:
--   * If a beach is mapped to a subordinate station and the cache for
--     that subordinate has no rows in the requested window, fall back
--     to the parent reference primary (noaa_stations.reference_id).
--   * To make the fallback actually succeed, the trigger that
--     auto-registers stations also registers the parent so the loader
--     keeps it warm.
--
-- Why we need it: when we widen with subordinate stations (Phase 2),
-- some subordinates will:
--   * Return empty from NOAA (data not published for that station yet)
--   * Have temporarily cold caches between weekly fires
--   * Or just be wrong for a beach (mis-assigned during widening)
-- Silent fallback to the primary is better than a missing tide curve.
-- Per Franz 2026-06-07: "If the update shows no tide data for the sub,
-- revert to the primary for tide."
--
-- Behavior contract:
--   * 0 rows for assigned station → try parent → return parent's rows
--   * If parent is also empty → return empty (no silent corruption)
--   * If beach has no station_id → return empty (unchanged)
--   * The caller can't tell which station the data came from — that's
--     intentional, the consumer surface just wants A reasonable curve.

CREATE OR REPLACE FUNCTION public.tide_for_beach(
  p_fid      bigint,
  p_start_ts timestamptz,
  p_end_ts   timestamptz
)
RETURNS SETOF public.tide_grid_hourly
LANGUAGE plpgsql STABLE PARALLEL SAFE SECURITY DEFINER SET search_path TO 'public'
AS $$
DECLARE
  v_station       text;
  v_parent        text;
  v_has_assigned  boolean;
BEGIN
  SELECT g.noaa_station_id, NULLIF(s.reference_id, g.noaa_station_id)
    INTO v_station, v_parent
    FROM public.beaches_gold g
    LEFT JOIN public.noaa_stations s ON s.station_id = g.noaa_station_id
   WHERE g.fid = p_fid
   LIMIT 1;

  IF v_station IS NULL OR v_station = '' THEN
    RETURN;
  END IF;

  -- Does the assigned station have ANY rows in the requested window?
  SELECT EXISTS (
    SELECT 1 FROM public.tide_grid_hourly
     WHERE station_id = v_station
       AND forecast_ts >= p_start_ts
       AND forecast_ts <  p_end_ts
  ) INTO v_has_assigned;

  IF v_has_assigned OR v_parent IS NULL THEN
    -- Primary path: assigned station has data, OR no parent to fall back to.
    RETURN QUERY
      SELECT *
        FROM public.tide_grid_hourly
       WHERE station_id = v_station
         AND forecast_ts >= p_start_ts
         AND forecast_ts <  p_end_ts
       ORDER BY forecast_ts;
  ELSE
    -- Fallback: assigned subordinate is empty; pull from the parent
    -- reference primary. The primary's curve is less locally accurate
    -- than the subordinate's would have been (no per-location time
    -- offset / height ratio applied) but it's directionally correct
    -- and far better than an empty rendering.
    RETURN QUERY
      SELECT *
        FROM public.tide_grid_hourly
       WHERE station_id = v_parent
         AND forecast_ts >= p_start_ts
         AND forecast_ts <  p_end_ts
       ORDER BY forecast_ts;
  END IF;
END;
$$;

GRANT EXECUTE ON FUNCTION public.tide_for_beach(bigint, timestamptz, timestamptz)
  TO anon, authenticated;


-- ─── Trigger upgrade — also register the parent ──────────────────────
-- Without this, registering a subordinate doesn't automatically warm
-- its parent. If the subordinate fetch fails and we fall back, the
-- parent might not be in inventory → loader hasn't warmed it → empty.
CREATE OR REPLACE FUNCTION public.tg_register_tide_station()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  v_parent text;
BEGIN
  IF NEW.noaa_station_id IS NOT NULL AND NEW.noaa_station_id <> '' THEN
    INSERT INTO public.tide_station_inventory (station_id)
    VALUES (NEW.noaa_station_id)
    ON CONFLICT (station_id) DO NOTHING;

    -- If the registered station is a subordinate (has a non-self
    -- reference_id), also register the parent reference primary so the
    -- loader warms it for the fallback path.
    SELECT NULLIF(s.reference_id, NEW.noaa_station_id)
      INTO v_parent
      FROM public.noaa_stations s
     WHERE s.station_id = NEW.noaa_station_id
     LIMIT 1;
    IF v_parent IS NOT NULL AND v_parent <> '' THEN
      INSERT INTO public.tide_station_inventory (station_id)
      VALUES (v_parent)
      ON CONFLICT (station_id) DO NOTHING;
    END IF;
  END IF;
  RETURN NEW;
END;
$$;

-- Backfill: register parents of any subordinates currently in inventory.
-- No-op for primaries (reference_id is null / self).
INSERT INTO public.tide_station_inventory (station_id)
SELECT DISTINCT NULLIF(s.reference_id, i.station_id) AS parent_id
  FROM public.tide_station_inventory i
  JOIN public.noaa_stations s ON s.station_id = i.station_id
 WHERE s.reference_id IS NOT NULL
   AND s.reference_id <> i.station_id
   AND s.reference_id <> ''
ON CONFLICT (station_id) DO NOTHING;
