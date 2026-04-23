-- Classify arbitrary (fid, lat, lng) points to the nearest US state.
-- Uses the PostGIS <-> KNN operator on the states.geom GIST index, so
-- any number of points is O(N log 52). Returns state_code plus the
-- distance in meters so callers can flag bad/offshore points if needed.
--
-- Used by admin-classify-points to enrich US_beaches.csv with a STATE
-- column — a one-off backfill that lets downstream consumers of the
-- CSV skip the spatial lookup entirely.

create or replace function public.classify_points_to_state(p_points jsonb)
returns jsonb
language sql
security definer
as $$
  with candidates as (
    select
      r->>'fid'                            as fid,
      (r->>'latitude')::double precision   as lat,
      (r->>'longitude')::double precision  as lon
    from jsonb_array_elements(p_points) as r
  )
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'fid',        c.fid,
        'state_code', nearest.state_code,
        'distance_m', round(nearest.distance_m)::int
      )
    ),
    '[]'::jsonb
  )
  from candidates c
  left join lateral (
    select
      s.state_code,
      ST_Distance(
        s.geom::geography,
        ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)::geography
      ) as distance_m
    from public.states s
    order by s.geom <-> ST_SetSRID(ST_MakePoint(c.lon, c.lat), 4326)
    limit 1
  ) nearest on true;
$$;

revoke all on function public.classify_points_to_state(jsonb) from public, anon, authenticated;
grant  execute on function public.classify_points_to_state(jsonb) to service_role;
