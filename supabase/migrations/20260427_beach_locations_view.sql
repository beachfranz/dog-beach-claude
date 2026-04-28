-- Unified beach catalog VIEW. One row per real-world beach location,
-- sourced from osm_features (the spine) plus UBP records that have no
-- spatial+name match in OSM (the gaps). Backfills name + address onto
-- OSM rows when a contained UBP exists.
--
-- Sits BETWEEN source tables and the curated locations_stage layer.
-- locations_stage is the validated/canonical subset for the consumer
-- app; beach_locations is the broader inventory. Promote to MV or
-- table only when we need writes or stable IDs.

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
  -- Pick one contained UBP for backfill, preferring the one with the
  -- highest name similarity (so the operator/address aligns with the
  -- "right" beach inside multi-beach polygons like Newport Beach).
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
        and st_dwithin(o.geom_full, u.geom, 0.01)  -- ~1km proximity guard
      )
    )
);

grant select on public.beach_locations to anon, authenticated;

comment on view public.beach_locations is
  'Unified CA beach inventory. OSM beach polygons + UBP records that have no spatial+name match in OSM. Backfills name and address onto OSM rows from contained UBP. Sits between source tables and locations_stage. Anon-readable. See migration 20260427_beach_locations_view.sql for merge rules.';
