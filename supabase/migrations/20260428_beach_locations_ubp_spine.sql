-- 805 = UBP spine + CCC orphans only.
--
-- UBP (us_beach_points) is the canonical CA beach inventory layer.
-- All active CA UBP rows pass through unfiltered.
--
-- CCC (ccc_access_points) is an overlay providing rich attributes
-- (dog_friendly, sandy_beach, parking, photos, etc.) joinable from
-- 805 via origin_key=ccc/<objectid> when the row appears, OR via
-- spatial proximity for UBP-spined rows.
--
-- A CCC row only enters 805 as an orphan when:
--   1. It's an active named beach with lat/lng
--   2. It's not a beach-neighbor name (stairway/walkway/etc.)
--   3. NO active CA UBP exists within 500m with name similarity >= 0.3
--   4. Names agree on whether this is a "Dog Beach" — dog beaches
--      are stand-alone parks, not deduped into nearby non-dog UBP rows.

drop view if exists public.beach_locations cascade;

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
        -- Names must agree on dog-beach status to even be candidates
        and (c.name ilike '%dog beach%') = (coalesce(u.name,'') ilike '%dog beach%')
        -- Tiered similarity: dog-beach pairs need stricter match because
        -- "Rosie's Dog Beach" vs "Monty's Dog Beach & Bar" both pass
        -- the loose 0.3 floor but are distinct parks. Non-dog pairs
        -- can collapse at 0.3.
        and (
          case
            when c.name ilike '%dog beach%' then
              similarity(coalesce(u.name,''), coalesce(c.name,'')) >= 0.5
            else
              similarity(coalesce(u.name,''), coalesce(c.name,'')) >= 0.3
          end
        )
   );
