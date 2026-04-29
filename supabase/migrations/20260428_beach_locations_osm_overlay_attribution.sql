-- Fix attribution regression introduced by the OSM-uncouple.
--
-- When beach_locations was OSM-spined, each row's geom was an OSM
-- polygon centroid — guaranteed on the actual beach — and the
-- operator_id was the carefully-cascaded OSM operator. UBP rows
-- inherit a stored geom (often address-geocoded inland) and a stored
-- operator_id (set by a weaker name-based cascade); both can be
-- wrong.
--
-- This migration uses OSM as a geographic + attribution OVERLAY
-- (not as identity). For each UBP row in 805, if a same-named OSM
-- beach polygon exists within 1km, prefer:
--   - OSM polygon centroid for geom
--   - OSM polygon's operator_id for operator_id
-- Falls back to UBP-stored values when no OSM match.

create or replace function public.refresh_beach_locations()
returns table (kept integer, inserted integer, updated integer, deleted integer)
language plpgsql security definer as $$
declare
  v_kept     int;
  v_inserted int;
  v_updated  int;
  v_deleted  int;
begin
  create temp table _new_beaches on commit drop as
    -- UBP spine, with OSM overlay for geom + operator
    with ubp_with_overlay as (
      select u.fid,
             u.name,
             u.operator_id as ubp_op,
             u.managing_agency_source,
             u.admin_inactive,
             u.address_clean, u.address_street, u.address_city,
             u.address_state, u.address_postal,
             u.geom as ubp_geom,
             ovr.osm_geom,
             ovr.osm_op
        from public.us_beach_points u
        left join lateral (
          select st_centroid(o.geom_full) as osm_geom,
                 o.operator_id as osm_op
            from public.osm_features o
           where o.feature_type in ('beach','dog_friendly_beach')
             and o.geom_full is not null
             and (o.admin_inactive is null or o.admin_inactive = false)
             and st_dwithin(o.geom_full::geography, u.geom::geography, 1000)
             and similarity(coalesce(o.name,''), coalesce(u.name,'')) >= 0.4
           order by similarity(coalesce(o.name,''), coalesce(u.name,'')) desc,
                    st_distance(o.geom, u.geom)
           limit 1
        ) ovr on true
       where u.state = 'CA'
         and (u.admin_inactive is null or u.admin_inactive = false)
    )
    -- UBP rows
    select 'ubp/' || fid::text as origin_key,
           'ubp'::text as origin_source,
           name,
           'ubp'::text as name_source,
           'beach'::text as feature_type,
           coalesce(osm_geom, ubp_geom) as geom,
           null::geometry as geom_full,
           coalesce(osm_op, ubp_op) as operator_id,
           managing_agency_source as operator_source,
           admin_inactive,
           address_clean, address_street, address_city, address_state, address_postal
      from ubp_with_overlay

    union all

    -- CCC orphan branch (unchanged from prior refresh)
    select 'ccc/' || c.objectid::text,
           'ccc'::text,
           c.name,
           'ccc'::text,
           'beach'::text,
           c.geom,
           null::geometry,
           c.operator_id,
           c.managing_agency_source,
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

  with d as (
    delete from public.beach_locations
     where origin_key not in (select origin_key from _new_beaches)
     returning 1
  )
  select count(*) into v_deleted from d;

  with up as (
    insert into public.beach_locations as t
      (origin_key, origin_source, name, name_source, feature_type,
       geom, geom_full, operator_id, operator_source, admin_inactive,
       address_clean, address_street, address_city, address_state, address_postal)
    select origin_key, origin_source, name, name_source, feature_type,
           geom, geom_full, operator_id, operator_source, admin_inactive,
           address_clean, address_street, address_city, address_state, address_postal
      from _new_beaches
    on conflict (origin_key) do update
      set origin_source   = excluded.origin_source,
          name            = excluded.name,
          name_source     = excluded.name_source,
          feature_type    = excluded.feature_type,
          geom            = excluded.geom,
          geom_full       = excluded.geom_full,
          operator_id     = excluded.operator_id,
          operator_source = excluded.operator_source,
          admin_inactive  = excluded.admin_inactive,
          address_clean   = excluded.address_clean,
          address_street  = excluded.address_street,
          address_city    = excluded.address_city,
          address_state   = excluded.address_state,
          address_postal  = excluded.address_postal,
          updated_at      = now()
      where (t.name, t.geom, coalesce(t.operator_id, 0))
          is distinct from
            (excluded.name, excluded.geom, coalesce(excluded.operator_id, 0))
    returning xmax = 0 as is_insert
  )
  select count(*) filter (where is_insert),
         count(*) filter (where not is_insert)
    into v_inserted, v_updated
    from up;

  v_kept := (select count(*) from public.beach_locations);
  return query select v_kept, v_inserted, v_updated, v_deleted;
end $$;

grant execute on function public.refresh_beach_locations() to authenticated;
