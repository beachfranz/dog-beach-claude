-- SQL function backing get-beaches-nearest edge function.
--
-- Returns the N active beaches closest to the given point, with distance.
-- Uses the GIST index on beaches.location (created 20260422) via the
-- geography <-> operator for KNN ordering.
--
-- Exposed via PostgREST RPC: supabase.rpc('beaches_nearest', { p_lat, p_lon, p_limit })

create or replace function public.beaches_nearest(
  p_lat   double precision,
  p_lon   double precision,
  p_limit integer
)
returns table (
  location_id  text,
  display_name text,
  latitude     numeric,
  longitude    numeric,
  distance_m   double precision
)
language sql
stable
as $$
  select
    b.location_id,
    b.display_name,
    b.latitude,
    b.longitude,
    ST_Distance(
      b.location,
      ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326)::geography
    ) as distance_m
  from public.beaches b
  where b.is_active = true
  order by b.location <-> ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326)::geography
  limit p_limit;
$$;

comment on function public.beaches_nearest is
  'Return the N active beaches closest to (lat, lon), ordered by geographic distance. Used by get-beaches-nearest edge function.';

-- Allow the anon and authenticated roles to call this via PostgREST RPC.
-- The underlying beaches.SELECT RLS policy already restricts row visibility.
grant execute on function public.beaches_nearest(double precision, double precision, integer) to anon, authenticated, service_role;
