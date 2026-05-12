-- 20260511_get_nearest_beaches_rpc.sql
--
-- Returns the N closest active beaches to a given fid with name +
-- distance + dogs_allowed. Used by generate_beach_descriptions.py to
-- give Scout the spatial neighbor context that makes californiabeaches.com
-- prose work ("north of here is Bolsa Chica where dogs aren't allowed").

begin;

create or replace function public.get_nearest_beaches(p_fid bigint, p_limit integer default 2)
returns table(
  fid bigint,
  name text,
  distance_m integer,
  direction text,
  dogs_allowed text
)
language sql
stable
security definer
set search_path to 'public', 'pg_catalog'
as $$
  with src as (
    select fid, geom, state from public.beaches_gold where fid = p_fid
  ),
  candidates as (
    select g.fid,
           coalesce(g.display_name_override, g.name) as name,
           st_distance(g.geom::geography, s.geom::geography)::integer as distance_m,
           degrees(st_azimuth(s.geom, g.geom)) as bearing_deg,
           bdp.dogs_allowed
      from public.beaches_gold g
      cross join src s
      left join public.beach_dog_policy bdp on bdp.arena_group_id = g.group_id
     where g.is_active
       and g.fid != p_fid
       and g.state = s.state
       and g.geom is not null
     order by g.geom <-> s.geom
     limit greatest(p_limit, 1)
  )
  select fid, name, distance_m,
         case
           when bearing_deg <  22.5 or bearing_deg >= 337.5 then 'N'
           when bearing_deg <  67.5 then 'NE'
           when bearing_deg < 112.5 then 'E'
           when bearing_deg < 157.5 then 'SE'
           when bearing_deg < 202.5 then 'S'
           when bearing_deg < 247.5 then 'SW'
           when bearing_deg < 292.5 then 'W'
           else                          'NW'
         end as direction,
         dogs_allowed
    from candidates;
$$;

grant execute on function public.get_nearest_beaches(bigint, integer) to anon, authenticated, service_role;

commit;
