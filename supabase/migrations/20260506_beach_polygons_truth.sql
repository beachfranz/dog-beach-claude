-- 20260506_beach_polygons_truth.sql
--
-- Hand-drawn ground-truth beach polygons. Curator draws these in QGIS
-- (or any tool with PostGIS write access) over Esri World Imagery, saves
-- the polygon following the actual sand-water boundary as they see it.
--
-- Two roles:
--   1) CANONICAL OVERRIDE — when a truth polygon exists for a fid, it
--      wins over any auto-extracted polygon. Frontends should prefer
--      truth → approved auto → curated curve. Override semantics are
--      enforced in the resolver, not here.
--   2) TEST SET — for measuring algorithm changes via IoU against truth.
--
-- One truth polygon per fid. Re-drawing replaces the prior row.

begin;

create table if not exists public.beach_polygons_truth (
  id          bigserial primary key,
  fid         int not null references public.beaches_gold(fid) on delete cascade,
  geom        geometry(Geometry, 4326) not null,
  area_m2     double precision generated always as (st_area(geom::geography)) stored,

  -- provenance
  drawn_at    timestamptz default now() not null,
  drawn_by    text default 'curator',
  source      text default 'qgis_hand_drawn'
              check (source in ('qgis_hand_drawn','admin_ui','imported')),
  notes       text,

  -- one truth row per beach (curator can re-draw; old rows get replaced
  -- via DELETE + INSERT or via UPDATE)
  unique (fid),
  constraint geom_polygon_only check (
    st_geometrytype(geom) in ('ST_Polygon','ST_MultiPolygon')
  ),
  constraint geom_is_valid check (st_isvalid(geom))
);

create index if not exists beach_polygons_truth_geom_idx
  on public.beach_polygons_truth using gist(geom);

-- RLS: anon SELECT (so admin maps can render truth as overlays);
-- writes go via service role (QGIS connects as service_role / postgres).
alter table public.beach_polygons_truth enable row level security;

drop policy if exists bpt_anon_read on public.beach_polygons_truth;
create policy bpt_anon_read
  on public.beach_polygons_truth for select
  to anon, authenticated
  using (true);

-- GeoJSON view so PostgREST can serve geom directly to the frontend
drop view if exists public.beach_polygons_truth_geojson;
create view public.beach_polygons_truth_geojson as
  select id, fid, area_m2, drawn_at, drawn_by, source, notes,
         st_asgeojson(geom)::jsonb as geom
    from public.beach_polygons_truth;

grant select on public.beach_polygons_truth_geojson to anon, authenticated;

-- Convenience: which fids have truth polygons (used by the resolver later
-- to gate auto-extracted promotion + by IoU eval scripts).
drop view if exists public.beach_polygons_truth_index;
create view public.beach_polygons_truth_index as
  select fid, area_m2, drawn_at, drawn_by, notes
    from public.beach_polygons_truth
   order by drawn_at desc;

grant select on public.beach_polygons_truth_index to anon, authenticated;

commit;
