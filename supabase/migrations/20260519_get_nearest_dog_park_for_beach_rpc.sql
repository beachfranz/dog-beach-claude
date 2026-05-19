-- RPC: get_nearest_dog_park_for_beach — single-call fetcher used by
-- beach.html + mobile-beach.html DP suggestion component.
--
-- Reads beaches_gold.nearest_dog_park_fid (materialized 2026-05-19),
-- joins to dog_parks_gold + dog_park_dog_policy + operator. Returns
-- empty (0 rows) when the beach has no DP within the 25mi cap.

CREATE OR REPLACE FUNCTION public.get_nearest_dog_park_for_beach(p_beach_fid bigint)
RETURNS TABLE (
  dp_fid               bigint,
  dp_name              text,
  dp_lat               double precision,
  dp_lon               double precision,
  dp_city              text,
  dp_state             text,
  distance_miles       double precision,
  leash_policy         text,
  hours_text           text,
  hours_open_time      text,
  hours_close_time     text,
  additional_rules     text,
  source               text,
  source_url           text,
  consensus_confidence numeric,
  operator_id          bigint,
  operator_name        text,
  operator_short       text
)
LANGUAGE sql STABLE PARALLEL SAFE AS $$
  SELECT
    g.fid, g.name, g.lat, g.lon, g.address_city, g.state,
    b.nearest_dog_park_distance_m / 1609.344 AS distance_miles,
    p.leash_policy, p.hours_text, p.hours_open_time, p.hours_close_time,
    p.additional_rules, p.source, p.source_url, p.consensus_confidence,
    p.operator_id, o.name AS operator_name, o.short_name AS operator_short
  FROM public.beaches_gold b
  JOIN public.dog_parks_gold g       ON g.fid = b.nearest_dog_park_fid
  JOIN public.dog_park_dog_policy p  ON p.dog_park_fid = g.fid
  LEFT JOIN public.operator o        ON o.id = p.operator_id
  WHERE b.fid = p_beach_fid;
$$;

COMMENT ON FUNCTION public.get_nearest_dog_park_for_beach(bigint) IS
  'Single-call DP fetcher for beach detail page. Returns 0 rows when the '
  'beach has no nearest_dog_park_fid set (no DP within 25mi cap). '
  'Created 2026-05-19 for Phase (d) DP-suggestion component.';

GRANT EXECUTE ON FUNCTION public.get_nearest_dog_park_for_beach(bigint) TO anon, authenticated;
