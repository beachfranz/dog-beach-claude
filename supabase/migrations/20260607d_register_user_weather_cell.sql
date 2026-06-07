-- 20260607d_register_user_weather_cell.sql
--
-- On-demand user-location warmup for the weather_grid reference layer.
--
-- find.html's "Walk from here" strip needs hourly walkability forecast
-- for the user's own coords (not a beach). Per the W2 architecture
-- ([[weather-grid-reference-layer]]) all consumers MUST read from
-- weather_grid_hourly via weather_for_point, gated on per-cell warmth
-- ([[grid-consumers-require-cell-warmth-gate]]). Direct Open-Meteo
-- fetches in the consumer path re-introduce the per-entity cost the
-- W2 cutover eliminated.
--
-- This RPC is the on-demand-register half of the pattern: when a
-- visitor's geolocation resolves, the client hits this fn which
-- idempotently inserts the bin-floored cell into weather_grid (status
-- = cold, all fetch timestamps NULL). The companion edge function
-- ensure-user-weather-cell then does the one-shot Open-Meteo fetch
-- for the cell if it's cold, populating weather_grid_hourly. The
-- regular hourly loader picks it up from then on if any consumer
-- entity (beach / dog park / future trail) shares the cell.
--
-- Idempotent: ON CONFLICT DO NOTHING. Returns warmth so the client
-- can decide whether to also kick the edge fn or skip straight to
-- weather_for_point.
--
-- Anon-grantable + SECURITY DEFINER so the client can call without
-- the service role. Bounded surface: writes ONE row max per call,
-- in a table whose other writes are loader/trigger only.

CREATE OR REPLACE FUNCTION public.register_user_weather_cell(
  p_lat double precision,
  p_lng double precision
)
RETURNS TABLE (
  out_grid_lat numeric,
  out_grid_lon numeric,
  out_is_warm  boolean
)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = 'public'
AS $function$
DECLARE
  v_glat numeric;
  v_glon numeric;
  v_warm boolean;
BEGIN
  -- Defensive: reject NULLs / out-of-range coords. Better to no-op than
  -- pollute the inventory with bogus cells.
  IF p_lat IS NULL OR p_lng IS NULL
     OR p_lat < -90  OR p_lat > 90
     OR p_lng < -180 OR p_lng > 180
  THEN
    RETURN;
  END IF;

  v_glat := public.weather_grid_bin_lat(p_lat);
  v_glon := public.weather_grid_bin_lon(p_lng);

  -- Idempotent registration. first_seen captures user-initiated cells
  -- so a future cleanup job can age out user-only entries that never
  -- pick up an entity association.
  INSERT INTO public.weather_grid (grid_lat, grid_lon, geom, first_seen)
  VALUES (
    v_glat,
    v_glon,
    ST_SetSRID(ST_MakePoint(v_glon::double precision, v_glat::double precision), 4326),
    now()
  )
  ON CONFLICT (grid_lat, grid_lon) DO NOTHING;

  -- Warmth check: ≥22 hourly rows in [now, now+24h] per the canonical
  -- gate threshold ([[grid-consumers-require-cell-warmth-gate]]).
  SELECT count(*) >= 22
  INTO v_warm
  FROM public.weather_grid_hourly h
  WHERE h.grid_lat = v_glat
    AND h.grid_lon = v_glon
    AND h.forecast_ts >= now()
    AND h.forecast_ts <  now() + interval '24 hours';

  out_grid_lat := v_glat;
  out_grid_lon := v_glon;
  out_is_warm  := COALESCE(v_warm, false);
  RETURN NEXT;
END;
$function$;

GRANT EXECUTE ON FUNCTION public.register_user_weather_cell(double precision, double precision)
  TO anon, authenticated;
