-- 20260513_pad_us_units_by_mng_type_helper.sql
--
-- Generic replacement for the narrow coastal_pad_us_units_for_state.
-- Takes a state code + array of mng_type codes. Optionally restricts
-- to PAD-US units that touch an active+scoreable beach (for Pass 6 PVT
-- where the unfiltered set is unmanageably large — 1,887 distinct
-- loc_mang values across CA/OR/WA).
--
-- coastal_pad_us_units_for_state is retained for backward compat with
-- any callers that still rely on the coastal-recreational name filter.

begin;

create or replace function public.pad_us_units_by_mng_type_for_state(
  p_state               text,
  p_mng_types           text[],
  p_beach_touching_only boolean default false
) returns table(
  unit_id   bigint,
  mng_name  text,
  mng_type  text,
  unit_name text,
  loc_mang  text,
  loc_own   text,
  geom      geometry
) language sql stable as $function$
  select pu.unit_id::bigint,
         pu.mng_name::text,
         pu.mng_type::text,
         pu.unit_name::text,
         pu.loc_mang::text,
         pu.loc_own::text,
         pu.geom
    from public.pad_us_units pu
   where pu.state = p_state
     and pu.mng_type = ANY(p_mng_types)
     and (
       not p_beach_touching_only
       or exists (
         select 1 from public.beaches_gold g
          where g.state = p_state
            and g.is_active
            and g.is_scoreable
            and st_intersects(g.geom, pu.geom)
       )
     );
$function$;

grant execute on function public.pad_us_units_by_mng_type_for_state(text, text[], boolean)
  to anon, authenticated, service_role;

commit;
