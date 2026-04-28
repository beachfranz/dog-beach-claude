-- Detail RPCs for the polymorphic left-pane viewer in the location
-- editor. Security definer so anon can read CCC + CPAD without an
-- explicit RLS policy.

create or replace function public.ccc_access_point_detail(p_objectid integer)
returns setof public.ccc_access_points
language sql stable security definer as $$
  select * from public.ccc_access_points where objectid = p_objectid limit 1;
$$;
grant execute on function public.ccc_access_point_detail(integer) to anon, authenticated;

create or replace function public.cpad_unit_detail(p_unit_id integer)
returns table (
  unit_id    integer,
  unit_name  text,
  mng_agncy  text,
  mng_ag_lev text,
  agncy_name text,
  agncy_lev  text,
  agncy_typ  text,
  agncy_web  text,
  park_url   text,
  access_typ text,
  site_name  text,
  label_name text,
  area_sqkm  numeric
)
language sql stable security definer as $$
  select c.unit_id, c.unit_name, c.mng_agncy, c.mng_ag_lev,
         c.agncy_name, c.agncy_lev, c.agncy_typ, c.agncy_web,
         c.park_url, c.access_typ, c.site_name, c.label_name,
         round((st_area(c.geom::geography) / 1e6)::numeric, 3) as area_sqkm
  from public.cpad_units c
  where c.unit_id = p_unit_id
  limit 1;
$$;
grant execute on function public.cpad_unit_detail(integer) to anon, authenticated;
