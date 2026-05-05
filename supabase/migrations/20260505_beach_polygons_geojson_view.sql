-- 20260505_beach_polygons_geojson_view.sql
--
-- Expose public.beach_polygons via PostgREST as GeoJSON-shaped data so the
-- consumer/admin maps can fetch and render with Leaflet's L.geoJSON. Anon
-- read-only — polygon data is public (OSM derivative + CPAD public domain).

begin;

-- 1. Enable RLS + anon read on the underlying table
alter table public.beach_polygons enable row level security;

drop policy if exists beach_polygons_anon_read on public.beach_polygons;
create policy beach_polygons_anon_read on public.beach_polygons
  for select to anon using (true);

-- 2. View that emits geometry as GeoJSON (PostgREST returns plain EWKB
--    for raw geometry columns, which JS can't render). security_invoker
--    so the underlying RLS policy applies through the view.
drop view if exists public.beach_polygons_geojson;
create view public.beach_polygons_geojson with (security_invoker = true) as
select bp.fid,
       bp.source_tier,
       bp.is_split_loser,
       bp.area_m2,
       st_asgeojson(bp.geom)::jsonb as geom
  from public.beach_polygons bp;

grant select on public.beach_polygons_geojson to anon, authenticated;

commit;
