-- Enable PostGIS and add spatial index to beaches
-- This is the foundation for efficient nearest-neighbor queries at scale.

CREATE EXTENSION IF NOT EXISTS postgis;

-- Add geography column (lon, lat order — PostGIS convention)
ALTER TABLE public.beaches
  ADD COLUMN IF NOT EXISTS location geography(Point, 4326);

-- Populate from existing lat/lng columns
UPDATE public.beaches
  SET location = ST_MakePoint(longitude::float8, latitude::float8)::geography
  WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- GiST spatial index — enables KNN queries in O(log n) instead of O(n)
CREATE INDEX IF NOT EXISTS beaches_location_gist
  ON public.beaches USING GIST (location);

-- ─── find_beaches RPC ─────────────────────────────────────────────────────────
-- Single query: beaches + day recommendations.
-- Distance computed via PostGIS when lat/lng are provided.
-- Sorting and remaining-window logic handled in the edge function.

CREATE OR REPLACE FUNCTION public.find_beaches(
  p_date  date,
  p_lat   double precision DEFAULT NULL,
  p_lng   double precision DEFAULT NULL,
  p_leash text             DEFAULT 'any'
)
RETURNS TABLE (
  location_id         text,
  display_name        text,
  latitude            numeric,
  longitude           numeric,
  access_rule         text,
  distance_m          double precision,
  day_status          text,
  best_window_label   text,
  best_window_status  text,
  bacteria_risk       text,
  summary_weather     text,
  weather_code        integer,
  lowest_tide_height  numeric,
  avg_temp            numeric,
  avg_wind            numeric,
  busyness_category   text,
  go_hours_count      integer,
  avg_tide_height     numeric
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public
AS $$
  SELECT
    b.location_id,
    b.display_name,
    b.latitude,
    b.longitude,
    b.access_rule,
    CASE
      WHEN p_lat IS NOT NULL AND p_lng IS NOT NULL AND b.location IS NOT NULL
      THEN ST_Distance(b.location, ST_MakePoint(p_lng, p_lat)::geography)
      ELSE NULL
    END                              AS distance_m,
    dr.day_status,
    dr.best_window_label,
    dr.best_window_status,
    dr.bacteria_risk,
    dr.summary_weather,
    dr.weather_code,
    dr.lowest_tide_height,
    dr.avg_temp,
    dr.avg_wind,
    dr.busyness_category,
    dr.go_hours_count,
    dr.avg_tide_height
  FROM public.beaches b
  LEFT JOIN public.beach_day_recommendations dr
         ON dr.location_id = b.location_id
        AND dr.local_date  = p_date
  WHERE b.is_active = true
    AND (p_leash = 'any' OR b.access_rule = p_leash OR b.access_rule IS NULL)
$$;

GRANT EXECUTE ON FUNCTION public.find_beaches TO anon, authenticated;
