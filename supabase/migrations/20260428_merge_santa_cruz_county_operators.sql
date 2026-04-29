-- Merge duplicate Santa Cruz County operators.
-- op 504 "Santa Cruz County" had policy extracted from a wrong URL
-- (CDFW's Moss Landing Wildlife Area page) — bad data.
-- op 1584 "Santa Cruz County Parks, County of" had policy correctly
-- extracted from CDPR's master county-by-county dog reference PDF.
-- Canonical = 1584. Migrate refs and drop 504 + its bad evidence.

begin;

-- 1. Migrate operator_id references on every table that FKs to operators
update public.us_beach_points                  set operator_id = 1584 where operator_id = 504;
update public.osm_features                     set operator_id = 1584 where operator_id = 504;
update public.ccc_access_points                set operator_id = 1584 where operator_id = 504;
update public.locations_stage                  set operator_id = 1584 where operator_id = 504;
-- Polygon caches: drop op-504 rows (they'll regenerate from cpad_units
-- via the cache rebuild when needed, with the new agency association).
delete from public.operator_polygons_cache           where operator_id = 504;
delete from public.operator_polygons_by_county_cache where operator_id = 504;

-- 2. Drop op 504's bad policy data (wrong URL, wrong agency)
delete from public.operator_policy_extractions where operator_id = 504;
delete from public.operator_dogs_policy        where operator_id = 504;

-- 3. Drop the duplicate operator row
delete from public.operators where id = 504;

commit;
