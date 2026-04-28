-- Revert beach_locations from a table back to the UBP-spine view.
-- The view-to-table conversion + OSM-overlay refresh introduced
-- attribution regressions. Restoring the view as it was at commit
-- f157a05 (before the table conversion) so we can recover, verify
-- correctness, then redo any materialization carefully.
--
-- Drops the table (and the refresh_beach_locations function that
-- only made sense for a table). Recreates the view with the same
-- column shape and source rules as the prior commit.

drop function if exists public.refresh_beach_locations() cascade;
drop table if exists public.beach_locations cascade;

create view public.beach_locations
with (security_invoker = true) as

-- UBP spine — every active CA UBP row, no exclusions
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

union all

-- CCC orphan branch — only rows with no UBP equivalent
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
       null::text, null::text, null::text, null::text, null::text
  from public.ccc_access_points c
 where (c.archived is null or c.archived <> 'Yes')
   and (c.admin_inactive is null or c.admin_inactive = false)
   and c.inferred_type in ('beach','named_beach')
   and c.latitude is not null and c.longitude is not null
   and not public.is_beach_neighbor_name(c.name)
   and not exists (
     select 1 from public.us_beach_points u
      where u.state = 'CA'
        and (u.admin_inactive is null or u.admin_inactive = false)
        and st_dwithin(u.geom::geography, c.geom::geography, 500)
        and (c.name ilike '%dog beach%') = (coalesce(u.name,'') ilike '%dog beach%')
        and (
          case
            when c.name ilike '%dog beach%' then
              similarity(coalesce(u.name,''), coalesce(c.name,'')) >= 0.5
            else
              similarity(coalesce(u.name,''), coalesce(c.name,'')) >= 0.3
          end
        )
   );
