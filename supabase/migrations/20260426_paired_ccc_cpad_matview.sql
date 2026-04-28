-- Precomputed pair set for "CCC ↔ CPAD within 200m" map filter. The
-- live query takes seconds against full cpad_units; matview drops it
-- to a millisecond read. Refresh after CCC coord drags or CPAD changes.

create materialized view if not exists public.paired_ccc_cpad_200m as
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
  select distinct s.objectid as ccc_id, c.unit_id as cpad_id
  from sandy_named s
  join coastal c on st_dwithin(c.geom, s.geom::geometry, 0.0018);  -- ~200m

create index if not exists paired_ccc_cpad_200m_ccc_idx
  on public.paired_ccc_cpad_200m (ccc_id);
create index if not exists paired_ccc_cpad_200m_cpad_idx
  on public.paired_ccc_cpad_200m (cpad_id);

grant select on public.paired_ccc_cpad_200m to anon, authenticated;

-- Replace the slow on-the-fly RPC with one that reads the matview.
create or replace function public.paired_ccc_cpad(p_meters integer default 200)
returns json
language sql stable security definer as $$
  select json_build_object(
    'ccc_ids',  coalesce((select array_agg(distinct ccc_id)  from public.paired_ccc_cpad_200m), '{}'),
    'cpad_ids', coalesce((select array_agg(distinct cpad_id) from public.paired_ccc_cpad_200m), '{}')
  );
$$;
