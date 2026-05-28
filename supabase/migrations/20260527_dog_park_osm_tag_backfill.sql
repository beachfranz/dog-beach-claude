-- 20260527_dog_park_osm_tag_backfill.sql
--
-- Backfill OSM raw tags into dog_park_dog_policy where the column is
-- still NULL (Franz 2026-05-27 LATE — "leverage all osm tags, few as
-- they are"). Only fills NULL cells; LLM-extracted values take priority.
--
-- Tag inventory observed on CA active dog parks (409 total):
--   barrier=fence:      40 parks → has_fence=true
--   lit=yes/no:          6 parks → lighting=true|false
--   surface=grass/dirt/sand/woodchips/mixed/artificial_turf: 17 parks
-- Other amenity tags (drinking_water, picnic_table, dog_agility,
-- shelter, swimming_pool, toilets, bench) are essentially absent in
-- the data so don't help.
--
-- This is idempotent — re-running the UPDATE only writes when the
-- target column is still NULL.

begin;

with src as (
  select dpg.fid,
         tags->>'barrier'  as osm_barrier,
         tags->>'lit'      as osm_lit,
         tags->>'surface'  as osm_surface
  from public.dog_parks_gold dpg
  where dpg.is_active = true and tags is not null
)
update public.dog_park_dog_policy dpp
   set has_fence       = case when dpp.has_fence is null
                                and src.osm_barrier = 'fence' then true
                                else dpp.has_fence end,
       lighting        = case when dpp.lighting is null
                                and src.osm_lit = 'yes' then true
                              when dpp.lighting is null
                                and src.osm_lit = 'no'  then false
                                else dpp.lighting end,
       surface_overlay = case when dpp.surface_overlay is null
                                and src.osm_surface in ('grass','dirt','sand','mixed','artificial_turf') then src.osm_surface
                              when dpp.surface_overlay is null
                                and src.osm_surface = 'woodchips' then 'wood_chips'
                                else dpp.surface_overlay end
from src
where dpp.dog_park_fid = src.fid
  and (
    (dpp.has_fence is null and src.osm_barrier = 'fence')
    or (dpp.lighting is null and src.osm_lit in ('yes','no'))
    or (dpp.surface_overlay is null and src.osm_surface in ('grass','dirt','sand','mixed','artificial_turf','woodchips'))
  );

commit;
