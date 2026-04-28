-- Returns the set of CCC objectids and CPAD unit_ids that are within
-- p_meters of each other AND match the active map filter (sandy_beach,
-- "beach" in name). Used by the location-editor's "Pair filter" toggle.

create or replace function public.paired_ccc_cpad(p_meters integer default 200)
returns json
language sql stable security definer as $$
  with sandy_named as (
    select objectid, geom
    from public.ccc_access_points
    where (archived is null or archived <> 'Yes')
      and latitude is not null and longitude is not null
      and sandy_beach = 'Yes' and name ilike '%beach%'
  ),
  coastal as (
    select cu.unit_id, cu.geom
    from public.cpad_units cu
    join public.cpad_units_coastal cc using(unit_id)
  ),
  pairs as (
    select distinct s.objectid as ccc_id, c.unit_id as cpad_id
    from sandy_named s
    join coastal c
      on st_dwithin(c.geom, s.geom::geometry, p_meters / 111320.0)
  )
  select json_build_object(
    'ccc_ids',  coalesce((select array_agg(distinct ccc_id)  from pairs), '{}'),
    'cpad_ids', coalesce((select array_agg(distinct cpad_id) from pairs), '{}')
  );
$$;

grant execute on function public.paired_ccc_cpad(integer) to anon, authenticated;
