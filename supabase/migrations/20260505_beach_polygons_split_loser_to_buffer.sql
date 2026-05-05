-- 20260505_beach_polygons_split_loser_to_buffer.sql
--
-- Refinement to the 3-tier polygon resolver: when a beach is a split-loser
-- (its OSM polygon was awarded to a sibling — e.g. Coronado Dog Beach lost
-- the Coronado Beach OSM polygon to fid 8715), DON'T fall to the CPAD
-- managing-unit polygon. Use the 250m buffer instead.
--
-- Reason: split-loser CPAD polygons are almost always the broader managing
-- unit (Coronado Municipal Beach, Huntington City Beach, Pismo State Beach
-- etc) which spans the entire beach including the parts where dogs are
-- explicitly prohibited. Painting the carve-out fid's "off-leash" rule
-- across the whole CPAD polygon visually creates a false-allow.
--
-- Non-split-loser beaches with CPAD fallback (Carpinteria, Garrapata, etc)
-- still use CPAD — those polygons typically ARE the right scope (the beach
-- IS the park).

begin;

create or replace function public.refresh_beach_polygons()
returns table(rows_loaded bigint, n_osm bigint, n_cpad bigint, n_buffer bigint, n_split_losers bigint)
language plpgsql as $$
declare _n_osm bigint; _n_cpad bigint; _n_buffer bigint; _n_loser bigint;
begin
  truncate public.beach_polygons;

  -- 1. OSM attribution (closest-centroid winner per OSM poly, smallest-area per fid)
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
    select osm_id, osm_geom, osm_area, fid
      from (
        select *, row_number() over (partition by osm_id order by dist_m asc) as rn
          from osm_x_gold
      ) z where rn = 1
  ),
  osm_per_fid as (
    select fid, osm_id, osm_geom, osm_area
      from (
        select *, row_number() over (partition by fid order by osm_area asc) as rn
          from osm_winners
      ) z where rn = 1
  )
  insert into public.beach_polygons (fid, source_tier, source_osm_id, geom, area_m2)
  select fid, 'osm', osm_id, osm_geom, osm_area from osm_per_fid;

  get diagnostics _n_osm = row_count;

  -- 2. Pre-flag split-losers so they SKIP the CPAD step. A split-loser is
  --    a fid that lives inside an OSM polygon but didn't win it. Those
  --    beaches go straight to buffer in step 3.
  create temp table _split_losers on commit drop as
    select distinct g.fid
      from public.osm_beach_polys o
      join public.beaches_gold g on st_intersects(o.geom, g.geom) and g.is_active
     where not exists (select 1 from public.beach_polygons bp where bp.fid = g.fid);

  -- 3. CPAD fallback — but ONLY for non-split-losers. Split-losers fall
  --    through to buffer in step 4.
  insert into public.beach_polygons (fid, source_tier, source_cpad_unit, geom, area_m2)
  select g.fid, 'cpad', cu.unit_name, cu.geom,
         st_area(cu.geom::geography)
    from public.beaches_gold g
    join public.cpad_units cu on cu.unit_id = g.cpad_unit_id
   where g.is_active
     and not exists (select 1 from public.beach_polygons bp where bp.fid = g.fid)
     and not exists (select 1 from _split_losers sl where sl.fid = g.fid);

  get diagnostics _n_cpad = row_count;

  -- 4. Buffered-point fallback — catches split-losers AND beaches with no CPAD
  insert into public.beach_polygons (fid, source_tier, geom, area_m2)
  select g.fid, 'buffer',
         st_buffer(g.geom::geography, 250)::geometry,
         st_area(st_buffer(g.geom::geography, 250))
    from public.beaches_gold g
   where g.is_active
     and not exists (select 1 from public.beach_polygons bp where bp.fid = g.fid);

  get diagnostics _n_buffer = row_count;

  -- 5. Mark split-losers
  update public.beach_polygons bp
     set is_split_loser = true
   where exists (select 1 from _split_losers sl where sl.fid = bp.fid);

  get diagnostics _n_loser = row_count;

  return query select
    (select count(*) from public.beach_polygons),
    _n_osm, _n_cpad, _n_buffer, _n_loser;
end $$;

select * from public.refresh_beach_polygons();

commit;
