-- RPC: nearest_dog_parks — spatial-nearest fetcher for find.html DP mode.
--
-- Per Phase (c) Phase 3 ship 2026-05-19. Mirrors get-beaches-find's contract
-- (lat/lng/limit + scored-vs-all toggle) but simpler — no day metrics,
-- just dog_parks_gold + dog_park_dog_policy + operator name.
--
-- Used by find.html when dest=dog_park (or dest=both).

CREATE OR REPLACE FUNCTION public.nearest_dog_parks(
  p_lat   double precision DEFAULT NULL,
  p_lng   double precision DEFAULT NULL,
  p_limit integer          DEFAULT 10,
  p_state text             DEFAULT NULL
)
RETURNS TABLE (
  fid                     bigint,
  name                    text,
  state                   text,
  address_city            text,
  lat                     double precision,
  lon                     double precision,
  distance_miles          double precision,
  has_fence               boolean,
  has_drinking_water      boolean,
  surface                 text,
  leash_policy            text,
  hours_text              text,
  hours_open_time         text,
  hours_close_time        text,
  additional_rules        text,
  source                  text,
  source_url              text,
  consensus_confidence    numeric,
  operator_id             bigint,
  operator_name           text,
  operator_short          text
)
LANGUAGE sql STABLE PARALLEL SAFE AS $$
  WITH user_pt AS (
    SELECT CASE WHEN p_lat IS NOT NULL AND p_lng IS NOT NULL
                THEN ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography
                ELSE NULL::geography END AS pt
  )
  SELECT
    g.fid, g.name, g.state, g.address_city, g.lat, g.lon,
    CASE WHEN up.pt IS NOT NULL
         THEN ST_Distance(up.pt, g.geom::geography) / 1609.344
         ELSE NULL::double precision
    END AS distance_miles,
    g.has_fence, g.has_drinking_water, g.surface,
    p.leash_policy, p.hours_text, p.hours_open_time, p.hours_close_time, p.additional_rules,
    p.source, p.source_url, p.consensus_confidence,
    p.operator_id, o.name AS operator_name, o.short_name AS operator_short
  FROM public.dog_parks_gold g
  JOIN public.dog_park_dog_policy p ON p.dog_park_fid = g.fid
  CROSS JOIN user_pt up
  LEFT JOIN public.operator o ON o.id = p.operator_id
  WHERE g.is_active = true
    AND (p_state IS NULL OR g.state = p_state)
  ORDER BY
    CASE WHEN up.pt IS NOT NULL THEN ST_Distance(up.pt, g.geom::geography) END NULLS LAST,
    g.name
  LIMIT GREATEST(COALESCE(p_limit, 10), 1);
$$;

COMMENT ON FUNCTION public.nearest_dog_parks(double precision, double precision, integer, text) IS
  'Spatial-nearest dog park fetcher for find.html. Returns dog_parks_gold + '
  'dog_park_dog_policy + operator name joined. If lat/lng provided, sorts by '
  'distance + computes distance_miles; else returns first N by name. '
  'Created 2026-05-19 for Phase (c) destination toggle.';

GRANT EXECUTE ON FUNCTION public.nearest_dog_parks(double precision, double precision, integer, text) TO anon, authenticated;
