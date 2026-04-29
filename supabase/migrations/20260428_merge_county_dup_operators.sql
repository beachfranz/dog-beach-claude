-- Merge three more duplicate county-level operator pairs identified
-- via the same-level normalized-name audit (after the SC County merge).
--
--   Marin     518  → 1346 (Marin County Parks Department, County of)
--   Del Norte 532  → 1148 (Del Norte County Parks, County of)
--   Yolo      1734 → 484  (Yolo County)
--
-- Same migration shape as the SC County merge: migrate FK refs across
-- all 8 tables that reference operators(id), drop polygon caches for
-- the dup, drop dup's policy/extraction rows, drop the dup row.

begin;

-- ── Marin: 518 → 1346 ─────────────────────────────────────────────
update public.us_beach_points                  set operator_id = 1346 where operator_id = 518;
update public.osm_features                     set operator_id = 1346 where operator_id = 518;
update public.ccc_access_points                set operator_id = 1346 where operator_id = 518;
update public.locations_stage                  set operator_id = 1346 where operator_id = 518;
delete from public.operator_polygons_cache           where operator_id = 518;
delete from public.operator_polygons_by_county_cache where operator_id = 518;
delete from public.operator_policy_extractions       where operator_id = 518;
delete from public.operator_dogs_policy              where operator_id = 518;
delete from public.operators where id = 518;

-- ── Del Norte: 532 → 1148 ─────────────────────────────────────────
update public.us_beach_points                  set operator_id = 1148 where operator_id = 532;
update public.osm_features                     set operator_id = 1148 where operator_id = 532;
update public.ccc_access_points                set operator_id = 1148 where operator_id = 532;
update public.locations_stage                  set operator_id = 1148 where operator_id = 532;
delete from public.operator_polygons_cache           where operator_id = 532;
delete from public.operator_polygons_by_county_cache where operator_id = 532;
delete from public.operator_policy_extractions       where operator_id = 532;
delete from public.operator_dogs_policy              where operator_id = 532;
delete from public.operators where id = 532;

-- ── Yolo: 1734 → 484 ──────────────────────────────────────────────
update public.us_beach_points                  set operator_id = 484 where operator_id = 1734;
update public.osm_features                     set operator_id = 484 where operator_id = 1734;
update public.ccc_access_points                set operator_id = 484 where operator_id = 1734;
update public.locations_stage                  set operator_id = 484 where operator_id = 1734;
delete from public.operator_polygons_cache           where operator_id = 1734;
delete from public.operator_polygons_by_county_cache where operator_id = 1734;
delete from public.operator_policy_extractions       where operator_id = 1734;
delete from public.operator_dogs_policy              where operator_id = 1734;
delete from public.operators where id = 1734;

commit;
