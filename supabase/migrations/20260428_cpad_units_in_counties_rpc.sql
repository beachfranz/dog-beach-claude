-- Returns CPAD unit_ids whose polygons intersect any beach_locations
-- row whose centroid falls inside the given counties. Used by
-- admin/cpad-unit-policy-viewer.html to scope the visible set.

create or replace function public.cpad_units_in_counties_for_805(
  p_counties text[]
) returns table (unit_id integer)
language sql stable security definer as $$
  with bl_sc as (
    select bl.geom from public.beach_locations bl
    join public.counties c on st_intersects(c.geom, bl.geom)
    where c.name = any(p_counties)
  )
  select distinct cu.unit_id
  from bl_sc bl
  join public.cpad_units cu on st_intersects(cu.geom, bl.geom);
$$;

grant execute on function public.cpad_units_in_counties_for_805(text[]) to anon, authenticated;
