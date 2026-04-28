-- Extend cpad_units_near_active_beaches to surface the dogs_allowed
-- answer per unit (LEFT JOIN cpad_unit_response_current). One round-
-- trip — no extra request per CPAD click.

create or replace function public.cpad_units_near_active_beaches(
  p_meters integer default 200
)
returns table (
  unit_id           integer,
  unit_name         text,
  mng_agncy         text,
  mng_ag_lev        text,
  park_url          text,
  agncy_web         text,
  geom_geojson      jsonb,
  dogs_allowed      text,
  dogs_confidence   numeric
)
language sql stable security definer as $$
  select
    cc.unit_id, cc.unit_name, cc.mng_agncy, cc.mng_ag_lev,
    cc.park_url, cc.agncy_web, cc.geom_geojson,
    d.response_value      as dogs_allowed,
    d.response_confidence as dogs_confidence
  from public.cpad_units_coastal cc
  left join public.cpad_unit_response_current d
    on d.cpad_unit_id = cc.unit_id
   and d.response_scope = 'dogs_allowed';
$$;

grant execute on function public.cpad_units_near_active_beaches(integer) to anon, authenticated;
