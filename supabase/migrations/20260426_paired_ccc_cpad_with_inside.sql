-- Add an `is_inside` flag to the paired matview so the map filter can
-- exclude pairs where the CCC point is strictly contained by the CPAD
-- polygon. Those are the "already-attributed" cases; what's interesting
-- for review is the "near-miss" pairs (within 200m but outside).

drop materialized view if exists public.paired_ccc_cpad_200m;

create materialized view public.paired_ccc_cpad_200m as
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
  )
  select distinct
    s.objectid as ccc_id,
    c.unit_id  as cpad_id,
    st_contains(c.geom, s.geom::geometry) as is_inside
  from sandy_named s
  join coastal c on st_dwithin(c.geom, s.geom::geometry, 0.0018);  -- ~200m

create index paired_ccc_cpad_200m_ccc_idx
  on public.paired_ccc_cpad_200m (ccc_id);
create index paired_ccc_cpad_200m_cpad_idx
  on public.paired_ccc_cpad_200m (cpad_id);
create index paired_ccc_cpad_200m_inside_idx
  on public.paired_ccc_cpad_200m (is_inside);

grant select on public.paired_ccc_cpad_200m to anon, authenticated;

-- RPC now returns only outside-the-polygon pairs (the near-miss set).
create or replace function public.paired_ccc_cpad(p_meters integer default 200)
returns json
language sql stable security definer as $$
  with outside_only as (
    select ccc_id, cpad_id from public.paired_ccc_cpad_200m where is_inside = false
  )
  select json_build_object(
    'ccc_ids',  coalesce((select array_agg(distinct ccc_id)  from outside_only), '{}'),
    'cpad_ids', coalesce((select array_agg(distinct cpad_id) from outside_only), '{}')
  );
$$;
