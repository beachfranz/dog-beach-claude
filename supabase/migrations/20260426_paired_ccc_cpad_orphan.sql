-- Refine the paired matview: tag each pair row with whether the CCC
-- is inside ANY CPAD (not just the one it's paired with). This lets
-- the map filter narrow to CCC points that aren't attributed to ANY
-- CPAD anywhere — the truly orphan-from-CPAD set.

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
  ),
  ccc_inside_any as (
    select distinct s.objectid
    from sandy_named s
    join coastal c on st_contains(c.geom, s.geom::geometry)
  ),
  pairs as (
    select distinct
      s.objectid as ccc_id,
      c.unit_id  as cpad_id,
      st_contains(c.geom, s.geom::geometry) as is_inside
    from sandy_named s
    join coastal c on st_dwithin(c.geom, s.geom::geometry, 0.0018)
  )
  select
    p.ccc_id, p.cpad_id, p.is_inside,
    (p.ccc_id in (select objectid from ccc_inside_any)) as ccc_inside_any_cpad
  from pairs p;

create index paired_ccc_cpad_200m_ccc_idx       on public.paired_ccc_cpad_200m (ccc_id);
create index paired_ccc_cpad_200m_cpad_idx      on public.paired_ccc_cpad_200m (cpad_id);
create index paired_ccc_cpad_200m_inside_idx    on public.paired_ccc_cpad_200m (is_inside);
create index paired_ccc_cpad_200m_anyinside_idx on public.paired_ccc_cpad_200m (ccc_inside_any_cpad);

grant select on public.paired_ccc_cpad_200m to anon, authenticated;

-- Filter: near-miss pair (not inside this CPAD) AND CCC isn't inside
-- any other CPAD either. The "really should be attributed somewhere"
-- review queue.
create or replace function public.paired_ccc_cpad(p_meters integer default 200)
returns json
language sql stable security definer as $$
  with eligible as (
    select ccc_id, cpad_id
    from public.paired_ccc_cpad_200m
    where is_inside = false
      and ccc_inside_any_cpad = false
  )
  select json_build_object(
    'ccc_ids',  coalesce((select array_agg(distinct ccc_id)  from eligible), '{}'),
    'cpad_ids', coalesce((select array_agg(distinct cpad_id) from eligible), '{}')
  );
$$;
