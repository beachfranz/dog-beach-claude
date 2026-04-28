-- Decouple osm_features from beach_locations entirely.
--
-- OSM polygons were swallowing distinct CCC-named beaches via the
-- spatial-containment dedupe. The fundamental issue: OSM features
-- are crowdsourced geometric shapes, not curated beach identities —
-- their boundaries don't match how California's coastal-access
-- agencies (CCC, CSP, county parks departments) define beaches.
--
-- New 805 = CCC named beaches + UBP California beaches only.
-- OSM features remain available for:
--   - Geometry overlays on maps (osm_features table direct)
--   - Operator attribution (osm_features.operator_id used by cascade)
--   - Smart-matching to CCC for verdict inheritance
--   - beach_access_features (the parallel access-infrastructure layer)
--
-- Dedupe rule between CCC and UBP: when a UBP point sits within 500m
-- of a similarly-named CCC point, drop the UBP — CCC's per-access-point
-- naming is more authoritative for CA.

drop view if exists public.beach_locations cascade;

create view public.beach_locations
with (security_invoker = true) as

-- CCC branch — sandy named beaches and named_beach inferred type
select 'ccc/' || c.objectid::text as origin_key,
       'ccc'::text as origin_source,
       c.name,
       'ccc'::text as name_source,
       'beach'::text as feature_type,
       c.geom,
       null::geometry as geom_full,
       c.operator_id,
       c.managing_agency_source as operator_source,
       c.admin_inactive,
       null::text as address_clean, null::text as address_street,
       null::text as address_city,  null::text as address_state,
       null::text as address_postal
  from public.ccc_access_points c
 where (c.archived is null or c.archived <> 'Yes')
   and (c.admin_inactive is null or c.admin_inactive = false)
   and c.inferred_type in ('beach','named_beach')
   and c.latitude is not null and c.longitude is not null
   and not public.is_beach_neighbor_name(c.name)

union all

-- UBP branch — CA points not already covered by a similarly-named CCC
select 'ubp/' || u.fid::text as origin_key,
       'ubp'::text as origin_source,
       u.name,
       'ubp'::text as name_source,
       'beach'::text as feature_type,
       u.geom,
       null::geometry as geom_full,
       u.operator_id,
       u.managing_agency_source as operator_source,
       u.admin_inactive,
       u.address_clean, u.address_street,
       u.address_city,  u.address_state, u.address_postal
  from public.us_beach_points u
 where u.state = 'CA'
   and (u.admin_inactive is null or u.admin_inactive = false)
   and not exists (
     select 1 from public.ccc_access_points c
      where (c.archived is null or c.archived <> 'Yes')
        and (c.admin_inactive is null or c.admin_inactive = false)
        and c.inferred_type in ('beach','named_beach')
        and c.latitude is not null
        and not public.is_beach_neighbor_name(c.name)
        and st_dwithin(u.geom::geography, c.geom::geography, 500)
        and similarity(coalesce(u.name,''), coalesce(c.name,'')) >= 0.5
        and (u.name ilike '%dog beach%') = (coalesce(c.name,'') ilike '%dog beach%')
   );
