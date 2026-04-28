-- Extend beach_locations (805) with a third leg: CCC-only rows.
-- CCC access points classified as beach/named_beach that have no
-- OSM 'same_beach' association AND no name-proximity UBP partner.
-- These are beaches OSM/UBP haven't catalogued — without this leg
-- they'd be invisible in the unified inventory.
--
-- The auto-update property holds: if a future smart-match adds an
-- OSM association for a CCC row, that CCC row drops out of the
-- ccc_only leg automatically. Likewise if OSM gains a polygon
-- containing the point, or a UBP record is added nearby.

create or replace view public.beach_locations
with (security_invoker = true)
as
with osm_active as (
  select * from public.osm_features
  where feature_type in ('beach', 'dog_friendly_beach')
    and (admin_inactive is null or admin_inactive = false)
),
ubp_active as (
  select * from public.us_beach_points
  where state = 'CA'
    and (admin_inactive is null or admin_inactive = false)
)
-- ── OSM rows (spine) ────────────────────────────────────────────────
select
  'osm/' || o.osm_type || '/' || o.osm_id::text as origin_key,
  'osm'::text                                   as origin_source,
  coalesce(nullif(o.name, ''), backfill.ubp_name) as name,
  case
    when nullif(o.name, '') is not null  then 'osm'
    when backfill.ubp_name is not null   then 'ubp_borrow'
    else null
  end                                           as name_source,
  o.feature_type,
  o.geom,
  o.geom_full,
  o.operator_id,
  o.managing_agency_source                      as operator_source,
  o.admin_inactive,
  backfill.address_clean,
  backfill.address_street,
  backfill.address_city,
  backfill.address_state,
  backfill.address_postal
from osm_active o
left join lateral (
  select u.name as ubp_name,
         u.address_clean, u.address_street, u.address_city,
         u.address_state, u.address_postal
  from ubp_active u
  where o.geom_full is not null
    and st_contains(o.geom_full, u.geom)
  order by similarity(coalesce(o.name, ''), coalesce(u.name, '')) desc nulls last,
           st_distance(o.geom, u.geom) asc
  limit 1
) backfill on true

union all

-- ── UBP-only rows (gaps OSM hasn't mapped) ──────────────────────────
select
  'ubp/' || u.fid::text                         as origin_key,
  'ubp_only'::text                              as origin_source,
  u.name,
  'ubp_only'::text                              as name_source,
  'beach'::text                                 as feature_type,
  u.geom,
  null::geometry                                as geom_full,
  u.operator_id,
  u.managing_agency_source                      as operator_source,
  u.admin_inactive,
  u.address_clean,
  u.address_street,
  u.address_city,
  u.address_state,
  u.address_postal
from ubp_active u
where not exists (
  select 1
  from osm_active o
  where o.geom_full is not null
    and (
      st_contains(o.geom_full, u.geom)
      or (
        similarity(coalesce(o.name, ''), coalesce(u.name, '')) >= 0.3
        and st_dwithin(o.geom_full, u.geom, 0.01)
      )
    )
)

union all

-- ── CCC-only rows (beaches OSM/UBP haven't catalogued) ──────────────
-- Eligible: inferred_type beach/named_beach, active, no OSM same_beach
-- association, no UBP partner.  CCC's lat/lng is the access point
-- (parking/trailhead) — same semantic as it has elsewhere.
select
  'ccc/' || c.objectid::text                    as origin_key,
  'ccc_only'::text                              as origin_source,
  c.name,
  'ccc_only'::text                              as name_source,
  'beach'::text                                 as feature_type,
  c.geom,
  null::geometry                                as geom_full,
  c.operator_id,
  c.managing_agency_source                      as operator_source,
  c.admin_inactive,
  null::text                                    as address_clean,
  null::text                                    as address_street,
  null::text                                    as address_city,
  null::text                                    as address_state,
  null::text                                    as address_postal
from public.ccc_access_points c
where (c.archived is null or c.archived <> 'Yes')
  and (c.admin_inactive is null or c.admin_inactive = false)
  and c.inferred_type in ('beach', 'named_beach')
  and c.latitude is not null and c.longitude is not null
  and not exists (
    select 1 from public.feature_associations fa
    where fa.a_source = 'ccc'  and fa.a_id = c.objectid::text
      and fa.b_source = 'osm'  and fa.relationship = 'same_beach'
  )
  and not exists (
    select 1 from public.osm_features o
    where o.feature_type in ('beach', 'dog_friendly_beach')
      and o.geom_full is not null
      and (o.admin_inactive is null or o.admin_inactive = false)
      and st_contains(o.geom_full, c.geom)
  )
  and not exists (
    select 1 from public.us_beach_points u
    where u.state = 'CA' and (u.admin_inactive is null or u.admin_inactive = false)
      and st_dwithin(u.geom::geography, c.geom::geography, 500)
      and similarity(coalesce(u.name, ''), coalesce(c.name, '')) >= 0.3
  );

grant select on public.beach_locations to anon, authenticated;

comment on view public.beach_locations is
  'Unified CA beach inventory across three sources: OSM beach polygons (spine), UBP records that have no spatial+name match in OSM, and CCC named_beach/beach access points that have no OSM/UBP partner. Backfills name + address onto OSM rows from contained UBP. Sits between source tables and locations_stage. Anon-readable.';
