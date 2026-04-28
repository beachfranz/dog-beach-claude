-- RPC: return CCC (California Coastal Commission) access points for the
-- admin map overlay. Security definer so anon can call without an RLS
-- policy on the underlying table.
--
-- Filtered to non-archived rows with valid lat/lng. ~1,600 points.

create or replace function public.ccc_access_points_for_map()
returns table (
  objectid       integer,
  name           text,
  county         text,
  district       text,
  latitude       double precision,
  longitude      double precision,
  dog_friendly   text,
  parking        text,
  restrooms      text,
  fee            text,
  open_to_public text
)
language sql stable security definer as $$
  select objectid, name, county, district, latitude, longitude,
         dog_friendly, parking, restrooms, fee, open_to_public
  from public.ccc_access_points
  where (archived is null or archived <> 'Yes')
    and latitude is not null and longitude is not null;
$$;

grant execute on function public.ccc_access_points_for_map() to anon, authenticated;
