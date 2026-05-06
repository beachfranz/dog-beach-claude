-- 20260505_auto_snap_to_osm_centroid.sql
--
-- One-shot snap of catalog points to their nearest OSM beach polygon
-- centroid, gated by name-match and three safety guards. Logs every
-- mutation to admin_audit so any specific row can be reverted.
--
-- Eligibility (must satisfy ALL):
--   1. There's an OSM polygon within 500m of the gold point
--   2. Name match: trigram_similarity ≥ 0.50 OR substring containment
--      (matches the protocol used by the beach_polygons resolver in
--      20260505_beach_polygons_cpad_cap_and_name_match.sql)
--   3. EITHER intersects AND >200m from centroid (Tier A)
--           OR doesn't intersect AND ≤500m away      (Tier B/C)
--
-- Guards (must satisfy ALL):
--   collision: no OTHER eligible gold row points at the same OSM polygon
--   bystander: no OTHER intersecting active gold row already covers it
--   self-distance: snap distance ≥ 5m (skip no-op micro-snaps)

begin;

-- Materialize the snap list into a temp table so we can audit + apply
-- atomically and emit row counts.
create temp table _snap_list on commit drop as
with best_osm as (
  select bg.fid, bg.name as gold_name,
         bg.lat as old_lat, bg.lon as old_lon, bg.geom as gold_geom,
         op.id as osm_id, op.name as osm_name, op.geom as osm_geom,
         st_intersects(op.geom, bg.geom) as intersects,
         st_distance(bg.geom::geography, op.geom::geography) as dist_m,
         st_distance(bg.geom::geography, st_centroid(op.geom)::geography) as centroid_dist_m,
         similarity(lower(coalesce(op.name, '')), lower(bg.name)) as sim,
         st_y(st_centroid(op.geom)) as new_lat,
         st_x(st_centroid(op.geom)) as new_lon
    from public.beaches_gold bg
    join lateral (
      select op.id, op.name, op.geom
        from public.osm_beach_polys op
       where st_dwithin(op.geom::geography, bg.geom::geography, 500)
       order by bg.geom <-> op.geom
       limit 1
    ) op on true
   where bg.is_active
),
name_matched as (
  select * from best_osm
   where (sim >= 0.50
          or (osm_name is not null and lower(gold_name) like '%' || lower(osm_name) || '%')
          or (osm_name is not null and lower(osm_name) like '%' || lower(gold_name) || '%'))
),
distance_eligible as (
  select * from name_matched
   where (intersects and centroid_dist_m > 200)
      or (not intersects and dist_m <= 500)
),
no_collision as (
  select * from distance_eligible
   where osm_id not in (
     select osm_id from distance_eligible group by osm_id having count(*) > 1
   )
),
no_bystander as (
  select nc.*
    from no_collision nc
   where not exists (
     select 1 from public.beaches_gold bg2
      where bg2.fid <> nc.fid
        and bg2.is_active
        and st_intersects(nc.osm_geom, bg2.geom)
   )
)
select fid, gold_name, osm_id, osm_name,
       old_lat, old_lon, new_lat, new_lon,
       round(st_distance(
         st_setsrid(st_makepoint(old_lon, old_lat), 4326)::geography,
         st_setsrid(st_makepoint(new_lon, new_lat), 4326)::geography
       )::numeric, 1) as snap_distance_m,
       round(sim::numeric, 2) as sim,
       intersects
  from no_bystander
 where st_distance(
         st_setsrid(st_makepoint(old_lon, old_lat), 4326)::geography,
         st_setsrid(st_makepoint(new_lon, new_lat), 4326)::geography
       ) >= 5;  -- skip no-op micro-snaps

-- Log totals so the migration output shows what happened
do $$
declare
  n_total int;
begin
  select count(*) into n_total from _snap_list;
  raise notice 'auto_snap_to_osm_centroid: % rows eligible after all guards', n_total;
end $$;

-- Apply the snap. RETURNING into a CTE feeds the audit insert.
with applied as (
  update public.beaches_gold bg
     set lat  = sl.new_lat,
         lon  = sl.new_lon,
         geom = st_setsrid(st_makepoint(sl.new_lon, sl.new_lat), 4326)
    from _snap_list sl
   where bg.fid = sl.fid
   returning
     bg.fid, bg.name as bg_name,
     sl.old_lat, sl.old_lon,
     sl.new_lat, sl.new_lon,
     sl.osm_id, sl.osm_name, sl.sim, sl.snap_distance_m
)
insert into public.admin_audit (function_name, action, before, after, success)
select 'auto_snap_to_osm_centroid', 'update',
       jsonb_build_object(
         'fid', fid, 'name', bg_name,
         'lat', old_lat, 'lon', old_lon
       ),
       jsonb_build_object(
         'fid', fid, 'name', bg_name,
         'lat', new_lat, 'lon', new_lon,
         'osm_id', osm_id, 'osm_name', osm_name,
         'trigram_sim', sim,
         'snap_distance_m', snap_distance_m,
         '__resolution_mode', 'auto_snap_to_osm_centroid'
       ),
       true
  from applied;

commit;
