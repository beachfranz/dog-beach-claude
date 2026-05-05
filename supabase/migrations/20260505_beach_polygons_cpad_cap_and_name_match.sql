-- 20260505_beach_polygons_cpad_cap_and_name_match.sql
--
-- Two refinements to refresh_beach_polygons():
--
-- 1. CPAD area cap. If the assigned CPAD unit polygon is > 500K m² (0.5
--    sq km), it's almost certainly broader than a single beach (Mission
--    Bay Park, large state-park complexes, etc). Fall to buffer instead
--    of using the over-broad CPAD polygon. Caught 2026-05-05 on Fiesta
--    Island (CPAD = 15.8 km² Mission Bay Park, polygon was 26x bigger
--    than the actual island).
--
-- 2. Name-preferred OSM attribution. When multiple OSM polygons contain
--    a gold fid, prefer the one whose name fuzzy-matches the gold beach
--    name (trigram similarity ≥ 0.4 OR substring containment). Falls
--    back to closest-centroid + smallest-area when no name match. Caught
--    2026-05-05 on Del Mar Dog Beach (named OSM poly "Del Mar Dog Beach"
--    sat unused because an unnamed polygon was 233m closer to the gold
--    lat/lon).

begin;

create or replace function public.refresh_beach_polygons()
returns table(rows_loaded bigint, n_osm bigint, n_cpad bigint, n_buffer bigint, n_split_losers bigint)
language plpgsql as $$
declare _n_osm bigint; _n_cpad bigint; _n_buffer bigint; _n_loser bigint;
declare _cpad_max_area double precision := 500000;  -- 500K m² ceiling
begin
  truncate public.beach_polygons;

  -- 1. OSM attribution with name-match preference.
  --    For each fid, score each containing OSM polygon by:
  --      - name_match_bonus: 1.0 if OSM name fuzzy-matches gold name, 0 otherwise
  --      - inverse area (smaller = better, normalized)
  --      - inverse distance to gold centroid (closer = better)
  --    Pick the highest-scoring OSM poly for the fid.
  with osm_x_gold as (
    select o.id as osm_id, o.geom as osm_geom, o.area_m2 as osm_area, o.name as osm_name,
           g.fid, g.geom as gold_geom, g.name as gold_name,
           st_distance(g.geom::geography,
                       st_centroid(o.geom)::geography) as dist_m,
           -- Name match: trigram similarity OR substring containment (case-insensitive)
           case
             when o.name is null then 0.0
             when similarity(lower(o.name), lower(g.name)) >= 0.4 then 1.0
             when lower(g.name) like '%' || lower(o.name) || '%' then 1.0
             when lower(o.name) like '%' || lower(g.name) || '%' then 1.0
             else 0.0
           end as name_match_score
      from public.osm_beach_polys o
      join public.beaches_gold g on st_intersects(o.geom, g.geom)
     where g.is_active
  ),
  osm_winners as (
    -- Per OSM polygon: keep only the closest fid (split-case handling)
    select osm_id, osm_geom, osm_area, fid, name_match_score
      from (
        select *, row_number() over (partition by osm_id order by dist_m asc) as rn
          from osm_x_gold
      ) z where rn = 1
  ),
  osm_per_fid as (
    -- Per fid: of the OSM polygons it WON, prefer name-match, then smallest area
    select fid, osm_id, osm_geom, osm_area
      from (
        select *, row_number() over (
          partition by fid
          order by name_match_score desc, osm_area asc
        ) as rn
          from osm_winners
      ) z where rn = 1
  )
  insert into public.beach_polygons (fid, source_tier, source_osm_id, geom, area_m2)
  select fid, 'osm', osm_id, osm_geom, osm_area from osm_per_fid;

  get diagnostics _n_osm = row_count;

  -- 2. Pre-flag split-losers
  create temp table _split_losers on commit drop as
    select distinct g.fid
      from public.osm_beach_polys o
      join public.beaches_gold g on st_intersects(o.geom, g.geom) and g.is_active
     where not exists (select 1 from public.beach_polygons bp where bp.fid = g.fid);

  -- 3. CPAD fallback — only for non-split-losers AND only when CPAD polygon
  --    is under the area ceiling. CPAD polygons larger than 500K m² are
  --    almost always managing-unit-broad (e.g. Mission Bay Park 15.8 km²)
  --    rather than beach-specific.
  insert into public.beach_polygons (fid, source_tier, source_cpad_unit, geom, area_m2)
  select g.fid, 'cpad', cu.unit_name, cu.geom,
         st_area(cu.geom::geography)
    from public.beaches_gold g
    join public.cpad_units cu on cu.unit_id = g.cpad_unit_id
   where g.is_active
     and not exists (select 1 from public.beach_polygons bp where bp.fid = g.fid)
     and not exists (select 1 from _split_losers sl where sl.fid = g.fid)
     and st_area(cu.geom::geography) < _cpad_max_area;

  get diagnostics _n_cpad = row_count;

  -- 4. Buffered-point fallback — catches split-losers, beaches with no CPAD,
  --    and beaches whose CPAD polygon exceeded the area cap.
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
