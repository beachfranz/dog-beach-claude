-- Stop dropping CCC named beaches when they sit inside an OSM polygon
-- with a materially different name. Dog beaches (Huntington, Rosie's,
-- Coronado, Dog Beach OB, Del Mar) are stand-alone parks, NOT
-- sub-features of the larger city/state beach polygons that contain
-- them geometrically. Keep both rows when their names differ.
--
-- Two guards added (`similarity(c.name, candidate.name) < 0.5`):
--   1. The OSM-containment exclusion now only fires when the OSM
--      polygon's name is similar to the CCC name.
--   2. The same_beach feature_association exclusion stays as-is
--      (curated pairing — when we explicitly tagged "same beach", we
--      meant it).
--   3. The UBP-similarity exclusion already had a name-similarity
--      check (>= 0.3); leave it alone.

drop view if exists public.beach_locations cascade;
create view public.beach_locations
with (security_invoker = true) as
with osm_active as (
  select * from public.osm_features
   where feature_type in ('beach','dog_friendly_beach')
     and (admin_inactive is null or admin_inactive = false)
),
ubp_active as (
  select * from public.us_beach_points
   where state = 'CA'
     and (admin_inactive is null or admin_inactive = false)
)
select 'osm/' || o.osm_type || '/' || o.osm_id::text as origin_key,
       'osm'::text as origin_source,
       coalesce(nullif(o.name, ''), backfill.ubp_name) as name,
       case
         when nullif(o.name, '') is not null then 'osm'
         when backfill.ubp_name is not null then 'ubp_borrow'
         else null
       end as name_source,
       o.feature_type, o.geom, o.geom_full, o.operator_id,
       o.managing_agency_source as operator_source, o.admin_inactive,
       backfill.address_clean, backfill.address_street,
       backfill.address_city, backfill.address_state, backfill.address_postal
  from osm_active o
  left join lateral (
    select u.name as ubp_name, u.address_clean, u.address_street,
           u.address_city, u.address_state, u.address_postal
      from ubp_active u
     where o.geom_full is not null and st_contains(o.geom_full, u.geom)
     order by similarity(coalesce(o.name,''), coalesce(u.name,'')) desc nulls last,
              st_distance(o.geom, u.geom)
     limit 1
  ) backfill on true

union all

select 'ubp/' || u.fid::text as origin_key,
       'ubp_only'::text as origin_source,
       u.name, 'ubp_only'::text as name_source,
       'beach'::text as feature_type,
       u.geom, null::geometry as geom_full, u.operator_id,
       u.managing_agency_source as operator_source, u.admin_inactive,
       u.address_clean, u.address_street, u.address_city, u.address_state, u.address_postal
  from ubp_active u
 where not exists (
   select 1 from osm_active o
    where o.geom_full is not null
      and (
        st_contains(o.geom_full, u.geom)
        or (
          similarity(coalesce(o.name,''), coalesce(u.name,'')) >= 0.3
          and st_dwithin(o.geom_full, u.geom, 0.01)
        )
      )
 )

union all

select 'ccc/' || c.objectid::text as origin_key,
       'ccc_only'::text as origin_source,
       c.name, 'ccc_only'::text as name_source,
       'beach'::text as feature_type,
       c.geom, null::geometry as geom_full, c.operator_id,
       c.managing_agency_source as operator_source, c.admin_inactive,
       null::text, null::text, null::text, null::text, null::text
  from public.ccc_access_points c
 where (c.archived is null or c.archived <> 'Yes')
   and (c.admin_inactive is null or c.admin_inactive = false)
   and c.inferred_type in ('beach','named_beach')
   and c.latitude is not null and c.longitude is not null
   and not public.is_beach_neighbor_name(c.name)
   -- Curated same_beach pairings still drop the CCC row
   and not exists (
     select 1 from public.feature_associations fa
      where fa.a_source='ccc' and fa.a_id = c.objectid::text
        and fa.b_source='osm' and fa.relationship='same_beach'
   )
   -- Spatial containment in OSM only drops CCC when:
   --   1. names match closely AND
   --   2. names AGREE on whether this is a dog beach (both have
   --      "Dog Beach" in name, or neither does — otherwise they're
   --      describing functionally distinct parks)
   and not exists (
     select 1 from public.osm_features o
      where o.feature_type in ('beach','dog_friendly_beach')
        and o.geom_full is not null
        and (o.admin_inactive is null or o.admin_inactive = false)
        and st_contains(o.geom_full, c.geom)
        and similarity(coalesce(o.name,''), coalesce(c.name,'')) >= 0.5
        and (c.name ilike '%dog beach%') = (coalesce(o.name,'') ilike '%dog beach%')
   )
   -- UBP proximity drop — same dog-beach agreement guard
   and not exists (
     select 1 from public.us_beach_points u
      where u.state = 'CA'
        and (u.admin_inactive is null or u.admin_inactive = false)
        and st_dwithin(u.geom::geography, c.geom::geography, 500)
        and similarity(coalesce(u.name,''), coalesce(c.name,'')) >= 0.3
        and (c.name ilike '%dog beach%') = (coalesce(u.name,'') ilike '%dog beach%')
   );
