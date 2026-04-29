-- Cleanup #1: pretend CCC didn't exist (active pipeline only).
--
-- Three actions, all scoped to the active CCC-free pipeline. The
-- ccc-friendly comparison fork (compute_dogs_verdict_ccc_friendly +
-- recompute_all_dogs_verdicts_ccc_friendly) doesn't depend on any of
-- these — it reads ccc_access_points.dog_friendly + geo_entity_response_current
-- entity_type='ccc' directly, so this cleanup leaves the fork intact.
--
--   1. Drop populate_from_ccc(). Reads CCC dog_friendly/parking/etc.
--      and writes evidence rows to beach_enrichment_provenance with
--      source='ccc'. Joins to locations_stage which is decommissioned;
--      the function is dead in the active path.
--
--   2. Delete beach_enrichment_provenance rows with source='ccc'.
--      ~916 evidence rows that fed the locations_stage resolver
--      (decommissioned). Stale data; nothing reads it.
--
--   3. Rebuild cpad_unit_for_beach to drop the CCC-orphan UNION
--      branch. Universe becomes beach_locations only. The precompute
--      currently has 1,294 rows of which 353 are CCC-orphan-keyed
--      (some duplicating beach_locations entries). After: 1,081 rows,
--      all keyed on beach_locations.origin_key.

begin;

-- 1. Drop populate_from_ccc.
drop function if exists public.populate_from_ccc(uuid);
drop function if exists public.populate_from_ccc(integer);
drop function if exists public.populate_from_ccc(bigint);
drop function if exists public.populate_from_ccc();

-- 2. Delete CCC-source evidence in beach_enrichment_provenance.
delete from public.beach_enrichment_provenance where source = 'ccc';

-- 3. Rebuild cpad_unit_for_beach over beach_locations only (no CCC orphans).
truncate public.cpad_unit_for_beach;

insert into public.cpad_unit_for_beach (origin_key, beach_name, beach_county, lat, lng, unit_id, unit_area_m2)
  select bl.origin_key,
         bl.name,
         (select c.name from public.counties c where st_intersects(c.geom, bl.geom) limit 1),
         st_y(bl.geom)::float8,
         st_x(bl.geom)::float8,
         cu.unit_id,
         cu.area_m2
    from public.beach_locations bl
    left join lateral (
      select cu2.unit_id, st_area(cu2.geom::geography) as area_m2
        from public.cpad_units cu2
       where st_contains(cu2.geom, bl.geom)
       order by
         (cu2.unit_name ~* '\m(marine park|marine protected|marine conservation|marine reserve|ecological reserve|wildlife area|wildlife refuge)\M')::int asc,
         similarity(public.clean_beach_name(coalesce(bl.name, '')),
                    public.clean_beach_name(coalesce(cu2.unit_name, ''))) desc,
         (cu2.unit_name ~* '\mbeach\M')::int desc,
         st_area(cu2.geom) asc
       limit 1
    ) cu on true;

commit;
