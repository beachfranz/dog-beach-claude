-- Tiny + nameless OSM beach polygons are noise: pocket-beach mappings
-- by OSM volunteers tagging stretches of coast as natural=beach without
-- naming them. ~9% had any UBP corroboration; these aren't real named
-- destinations.
--
-- Audit done 2026-04-27 against LA+OC+SD scope:
--   54 tiny (<1k m²) beach polygons in LA+OC+SD
--   30 of those (56%) had no name
--   only 9% of tiny polys overall had any UBP nearby
-- Statewide: 211 tiny+nameless polygons; all flipped to admin_inactive.
--
-- Re-run is safe — only flips active rows that still match the rule.

update public.osm_features
   set admin_inactive = true
 where feature_type in ('beach','dog_friendly_beach')
   and geom_full is not null
   and (name is null or name = '')
   and st_area(geom_full::geography) < 1000
   and (admin_inactive is null or admin_inactive = false);
