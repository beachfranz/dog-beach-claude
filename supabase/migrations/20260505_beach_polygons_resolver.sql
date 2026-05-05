-- 20260505_beach_polygons_resolver.sql
--
-- Per-fid polygon resolver. Materializes one row per active gold fid with
-- the best-available polygon for visual rendering, via a 3-tier waterfall:
--
--   Tier 1 (osm)    — OSM beach polygon (natural=beach). Smallest containing
--                     polygon wins when multiple match. When ONE OSM polygon
--                     contains >1 gold fid (~23 sibling-pair cases), the fid
--                     whose lat/lon is closest to the polygon's centroid wins;
--                     the other falls through to lower tiers.
--   Tier 2 (cpad)   — CPAD unit polygon assigned via beaches_gold.cpad_unit_id.
--                     Coarser than OSM (managing-unit broad — includes parking,
--                     campground, etc.) but better than nothing for state parks
--                     OSM hasn't polygonized.
--   Tier 3 (buffer) — 250m circle buffered around the gold fid's lat/lon.
--                     Last-resort placeholder.
--
-- Polygon split for 1:N OSM cases is intentionally NOT done here (the loser
-- of the closest-centroid attribution falls back to CPAD or buffer). When
-- curator review surfaces split-cases that hurt UX, we revisit with Voronoi
-- (PostGIS ST_VoronoiPolygons) — for now keep it simple.
--
-- Refresh: call public.refresh_beach_polygons() after gold/osm/cpad updates.

begin;

drop table if exists public.beach_polygons cascade;

create table public.beach_polygons (
  fid              bigint primary key references public.beaches_gold(fid),
  source_tier      text   not null check (source_tier in ('osm','cpad','buffer')),
  source_osm_id    bigint,
  source_cpad_unit text,
  geom             geometry(Geometry, 4326) not null,
  area_m2          double precision,
  is_split_loser   boolean default false,  -- true if an OSM poly was awarded to a sibling
  computed_at      timestamptz default now()
);
create index beach_polygons_geom_gix on public.beach_polygons using gist (geom);
create index beach_polygons_tier_idx on public.beach_polygons (source_tier);

create or replace function public.refresh_beach_polygons()
returns table(rows_loaded bigint, n_osm bigint, n_cpad bigint, n_buffer bigint, n_split_losers bigint)
language plpgsql as $$
declare _n_osm bigint; _n_cpad bigint; _n_buffer bigint; _n_loser bigint;
begin
  truncate public.beach_polygons;

  -- 1. OSM attribution: for each OSM polygon, pick the closest-centroid gold fid
  --    (handles the 1:N split case by giving the polygon to one fid only).
  with osm_x_gold as (
    select o.id as osm_id, o.geom as osm_geom, o.area_m2 as osm_area,
           g.fid, g.geom as gold_geom,
           st_distance(g.geom::geography,
                       st_centroid(o.geom)::geography) as dist_m
      from public.osm_beach_polys o
      join public.beaches_gold g on st_intersects(o.geom, g.geom)
     where g.is_active
  ),
  osm_winners as (
    -- Per OSM polygon: keep only the closest fid (split-case handling)
    select osm_id, osm_geom, osm_area, fid
      from (
        select *, row_number() over (partition by osm_id order by dist_m asc) as rn
          from osm_x_gold
      ) z where rn = 1
  ),
  osm_per_fid as (
    -- Per fid: of the OSM polygons it WON, pick the smallest-area
    select fid, osm_id, osm_geom, osm_area
      from (
        select *, row_number() over (partition by fid order by osm_area asc) as rn
          from osm_winners
      ) z where rn = 1
  )
  insert into public.beach_polygons (fid, source_tier, source_osm_id, geom, area_m2)
  select fid, 'osm', osm_id, osm_geom, osm_area from osm_per_fid;

  get diagnostics _n_osm = row_count;

  -- 2. CPAD fallback for fids not yet covered, via assigned cpad_unit_id
  insert into public.beach_polygons (fid, source_tier, source_cpad_unit, geom, area_m2)
  select g.fid, 'cpad', cu.unit_name, cu.geom,
         st_area(cu.geom::geography)
    from public.beaches_gold g
    join public.cpad_units cu on cu.unit_id = g.cpad_unit_id
   where g.is_active
     and not exists (select 1 from public.beach_polygons bp where bp.fid = g.fid);

  get diagnostics _n_cpad = row_count;

  -- 3. Buffered-point fallback for everything else (250m circle as geography)
  insert into public.beach_polygons (fid, source_tier, geom, area_m2)
  select g.fid, 'buffer',
         st_buffer(g.geom::geography, 250)::geometry,
         st_area(st_buffer(g.geom::geography, 250)) -- already in m^2 from geography
    from public.beaches_gold g
   where g.is_active
     and not exists (select 1 from public.beach_polygons bp where bp.fid = g.fid);

  get diagnostics _n_buffer = row_count;

  -- 4. Mark split-losers — fids that LOST an OSM-poly arbitration and ended up
  --    in cpad/buffer despite an OSM poly containing them. Useful for the
  --    moderation UI to surface these as candidates for Voronoi-split later.
  update public.beach_polygons bp
     set is_split_loser = true
   where bp.source_tier <> 'osm'
     and exists (
       select 1 from public.osm_beach_polys o
        join public.beaches_gold g on g.fid = bp.fid
       where st_intersects(o.geom, g.geom)
     );

  get diagnostics _n_loser = row_count;

  return query select
    (select count(*) from public.beach_polygons),
    _n_osm, _n_cpad, _n_buffer, _n_loser;
end $$;

comment on function public.refresh_beach_polygons() is
  'Rebuilds public.beach_polygons via 3-tier waterfall: OSM beach poly (closest-'
  'centroid winner per OSM poly + smallest-area per fid) -> CPAD unit poly via '
  'cpad_unit_id -> 250m buffer around gold lat/lon. Returns counts. Idempotent. '
  'Run after beaches_gold / osm_beach_polys / cpad_units changes.';

-- Initial population
select * from public.refresh_beach_polygons();

commit;
