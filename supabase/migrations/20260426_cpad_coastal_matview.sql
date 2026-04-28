-- Materialize the coastal-CPAD-with-simplified-geom set so PostgREST
-- can read it inside the anon statement timeout. ~537 rows; refresh
-- when locations_stage or cpad_units changes meaningfully.

create materialized view if not exists public.cpad_units_coastal as
  with relevant as (
    select distinct c.unit_id
    from public.locations_stage s
    join public.cpad_units c
      on st_dwithin(c.geom::geography, s.geom::geography, 200)
    where s.is_active = true
  )
  select
    c.unit_id, c.unit_name, c.mng_agncy, c.mng_ag_lev,
    c.park_url, c.agncy_web,
    st_asgeojson(st_simplify(c.geom, 0.0003))::jsonb as geom_geojson
  from relevant r
  join public.cpad_units c on c.unit_id = r.unit_id;

create unique index if not exists cpad_units_coastal_pkey
  on public.cpad_units_coastal (unit_id);

-- The matview inherits the table's RLS visibility; expose to anon.
grant select on public.cpad_units_coastal to anon, authenticated;

-- Replace the slow RPC with a fast one that just reads the matview.
create or replace function public.cpad_units_near_active_beaches(
  p_meters integer default 200  -- kept for signature compatibility; ignored
)
returns table (
  unit_id      integer,
  unit_name    text,
  mng_agncy    text,
  mng_ag_lev   text,
  park_url     text,
  agncy_web    text,
  geom_geojson jsonb
)
language sql stable security definer as $$
  select unit_id, unit_name, mng_agncy, mng_ag_lev, park_url, agncy_web, geom_geojson
  from public.cpad_units_coastal;
$$;

grant execute on function public.cpad_units_near_active_beaches(integer) to anon, authenticated;
