-- Patch load_state_feature RPC to apply ST_MakeValid + ST_CollectionExtract
-- at ingest time, matching the pattern used by load_cpad_batch,
-- load_counties_batch, etc. Prevents invalid MultiPolygons from landing
-- in the states table when the feeding loader builds rings naively.
--
-- Bug found 2026-04-24: the hi-res TIGER state load built GeoJSON
-- polygons by flattening all shapefile rings as outer rings of a single
-- Polygon. For archipelago states (CA, FL, HI, PR, RI, ME, AK, etc.)
-- this produced invalid topology — "Hole lies outside shell" errors —
-- and made every spatial check (Contains/Intersects/DWithin) falsely
-- return false for boundary points (island shorelines).
--
-- ST_MakeValid repairs the topology; ST_CollectionExtract(g, 3) keeps
-- only polygon parts in case MakeValid emits a GeometryCollection.

create or replace function public.load_state_feature(
  p_state_code text,
  p_state_name text,
  p_geojson    text
) returns boolean
language plpgsql
security definer
as $$
declare
  existed boolean;
begin
  select true into existed from public.states where state_code = p_state_code;
  insert into public.states (state_code, state_name, geom)
  values (
    p_state_code,
    p_state_name,
    ST_Multi(ST_CollectionExtract(
      ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON(p_geojson), 4326)),
      3
    ))
  )
  on conflict (state_code) do update
    set state_name = excluded.state_name,
        geom      = excluded.geom,
        loaded_at = now();
  return not coalesce(existed, false);
end;
$$;

revoke all on function public.load_state_feature(text, text, text) from public, anon, authenticated;
grant  execute on function public.load_state_feature(text, text, text) to service_role;
