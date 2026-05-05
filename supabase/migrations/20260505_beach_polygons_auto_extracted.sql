-- 20260505_beach_polygons_auto_extracted.sql
--
-- Algorithmically-extracted beach polygons (from sand-color image
-- detection on rendered basemaps). Separate from the canonical
-- public.beach_polygons resolver because:
--   - the trust profile is different (algorithmic, not surveyed)
--   - we'll iterate the algorithm + thresholds + basemaps; rebuilds
--     should not touch the canonical layer
--   - a curator must approve before promotion
--
-- Wiring into refresh_beach_polygons() is deferred until at least one
-- row is approved.

begin;

create table if not exists public.beach_polygons_auto_extracted (
  id            bigserial primary key,
  fid           int not null references public.beaches_gold(fid) on delete cascade,
  geom          geometry(Polygon, 4326) not null,
  area_m2       double precision generated always as
                   (st_area(geom::geography)) stored,

  -- provenance
  extracted_at      timestamptz default now() not null,
  algorithm_version text not null,         -- e.g. 'sand_v1_z16_V0.93'
  tile_zoom         int,
  basemap_source    text default 'esri_world_topo',
  source_image_path text,                  -- e.g. 'share/seal_beach_sand_only.png'
  pipeline_run_id   uuid default gen_random_uuid(),
  -- notes from the extraction itself
  vertex_count int,
  pixel_count  int,
  osm_iou      double precision,           -- intersection-over-union with OSM
                                           --  beach polygon if one exists; null otherwise

  -- review gate
  review_status text default 'pending' not null
                check (review_status in ('pending','approved','rejected')),
  reviewed_by   text,
  reviewed_at   timestamptz,
  reviewer_notes text,

  -- ensures at most one APPROVED row per fid (multiple pending OK).
  -- a partial unique index because Postgres doesn't have FK-like row
  -- restrictions on a CHECK; this guards the resolver promise.
  constraint geom_is_valid check (st_isvalid(geom))
);

create unique index if not exists beach_polygons_auto_extracted_one_approved_per_fid
  on public.beach_polygons_auto_extracted (fid)
  where review_status = 'approved';

create index if not exists beach_polygons_auto_extracted_geom_idx
  on public.beach_polygons_auto_extracted using gist(geom);
create index if not exists beach_polygons_auto_extracted_status_idx
  on public.beach_polygons_auto_extracted (review_status);
create index if not exists beach_polygons_auto_extracted_fid_idx
  on public.beach_polygons_auto_extracted (fid);

-- RLS: anon can SELECT (so the review UI can read),
-- writes restricted to service role.
alter table public.beach_polygons_auto_extracted enable row level security;

drop policy if exists pae_anon_read on public.beach_polygons_auto_extracted;
create policy pae_anon_read
  on public.beach_polygons_auto_extracted for select
  to anon, authenticated
  using (true);

-- (writes happen via service role / edge function — no anon-write policy.)

-- GeoJSON view so frontends can fetch shape without ST_AsGeoJSON RPC
drop view if exists public.beach_polygons_auto_extracted_geojson;
create view public.beach_polygons_auto_extracted_geojson as
  select id, fid, area_m2, vertex_count, pixel_count, osm_iou,
         review_status, algorithm_version, tile_zoom, source_image_path,
         extracted_at, reviewed_at, reviewed_by, reviewer_notes,
         st_asgeojson(geom)::jsonb as geom
    from public.beach_polygons_auto_extracted;

grant select on public.beach_polygons_auto_extracted_geojson to anon, authenticated;

commit;
