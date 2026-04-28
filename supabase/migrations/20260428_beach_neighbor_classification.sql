-- Reclassify CCC rows tagged inferred_type='beach' but whose names
-- match access-infrastructure patterns (stairways, walkways, RV parks,
-- KOAs, marinas, clubs, vista points, overlooks, trails, underpasses,
-- picnic areas) as 'neighbor' — coastal access infrastructure adjacent
-- to a beach but not the beach itself.
--
-- Effect on 805: 18 rows drop out of beach_locations.ccc_only.
-- Those rows surface in beach_access_features with feature_type='neighbor'
-- so they're not lost — just routed to the access layer where they belong.
--
-- 805 beach count: 1,639 → 1,621.
-- beach_access_features count: 1,120 → 1,138.

-- Helper: pattern-match for "access infrastructure" names. Returns true
-- for clearly non-beach access types even when CCC's classifier said
-- inferred_type='beach' or 'named_beach'.
create or replace function public.is_beach_neighbor_name(p_name text)
returns boolean
language sql immutable as $$
  select coalesce(
    p_name ~* '\m(stairway|walkway|underpass|vista|viewpoint|overlook|RV park|KOA|marina|picnic area)\M'
    or p_name ~* '(\sTrail|\sCampground|\sClub)$',
    false
  );
$$;


-- ── Update beach_locations: exclude name-pattern neighbors from 805 ──
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
-- OSM rows (spine) — unchanged
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
  where o.geom_full is not null and st_contains(o.geom_full, u.geom)
  order by similarity(coalesce(o.name, ''), coalesce(u.name, '')) desc nulls last,
           st_distance(o.geom, u.geom) asc
  limit 1
) backfill on true

union all

-- UBP-only rows — unchanged
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
  select 1 from osm_active o
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

-- CCC-only — now also excludes name-pattern neighbors (routed to
-- beach_access_features instead).
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
  null::text, null::text, null::text, null::text, null::text
from public.ccc_access_points c
where (c.archived is null or c.archived <> 'Yes')
  and (c.admin_inactive is null or c.admin_inactive = false)
  and c.inferred_type in ('beach', 'named_beach')
  and c.latitude is not null and c.longitude is not null
  -- NEW: exclude name-pattern neighbors
  and not public.is_beach_neighbor_name(c.name)
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


-- ── Update beach_access_features: include 'neighbor' rows ──
create or replace view public.beach_access_features
with (security_invoker = true)
as
select
  'ccc/' || c.objectid::text                    as origin_key,
  'ccc'::text                                   as origin_source,
  c.name,
  case
    when c.inferred_type in ('beach','named_beach')
         and public.is_beach_neighbor_name(c.name) then 'neighbor'
    else coalesce(c.inferred_type, 'unknown')
  end                                           as feature_type,
  c.geom,
  c.operator_id,
  c.managing_agency_source                      as operator_source,
  c.admin_inactive,
  c.dogs_verdict,
  c.dogs_verdict_confidence,
  c.description,
  c.phone,
  c.photo_1,
  c.google_maps_location,
  c.county_name_tiger                           as county
from public.ccc_access_points c
where (c.archived is null or c.archived <> 'Yes')
  and (c.admin_inactive is null or c.admin_inactive = false)
  and c.latitude is not null and c.longitude is not null
  and (
    -- Non-beach CCC types (existing behavior)
    coalesce(c.inferred_type, 'unknown') not in ('beach', 'named_beach')
    -- OR beach-class CCC where the name marks it as access infrastructure
    or (
      c.inferred_type in ('beach','named_beach')
      and public.is_beach_neighbor_name(c.name)
    )
  );

grant select on public.beach_access_features to anon, authenticated;

comment on view public.beach_locations is
  'Unified CA beach inventory (a.k.a. 805) across three sources: OSM beach polygons (spine), UBP records that have no spatial+name match in OSM, and CCC named_beach/beach access points that have no OSM/UBP partner. Excludes CCC rows whose names mark them as access infrastructure (stairway/walkway/RV park/KOA/marina/club/vista/overlook/trail/underpass) — those route to beach_access_features as feature_type=neighbor.';
