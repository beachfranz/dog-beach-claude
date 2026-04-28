-- Materialize beach_locations from a view into a real table.
--
-- Same columns as the prior view, plus created_at/updated_at for
-- tracking. Refresh is explicit via refresh_beach_locations().
-- Cascade impact: zero — readers see the same name + same columns.

drop view if exists public.beach_locations cascade;

create table public.beach_locations (
  origin_key      text primary key,
  origin_source   text not null check (origin_source in ('ubp','ccc')),
  name            text,
  name_source     text,
  feature_type    text,
  geom            geometry(Point, 4326),
  geom_full       geometry,
  operator_id     bigint,
  operator_source text,
  admin_inactive  boolean,
  address_clean   text,
  address_street  text,
  address_city    text,
  address_state   text,
  address_postal  text,
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now()
);

create index beach_locations_geom_gix       on public.beach_locations using gist (geom);
create index beach_locations_name_trgm_gin  on public.beach_locations using gin (name gin_trgm_ops);
create index beach_locations_state_idx      on public.beach_locations (address_state);
create index beach_locations_operator_idx   on public.beach_locations (operator_id);
create index beach_locations_source_idx     on public.beach_locations (origin_source);


-- Refresh function: rebuild from source tables. Idempotent — call any
-- time UBP or CCC source data is reloaded, or whenever you want a
-- fresh snapshot. UPSERT-style so existing override columns (when we
-- add them) survive a rebuild — origin_key is stable across rebuilds.

create or replace function public.refresh_beach_locations()
returns table (kept integer, inserted integer, updated integer, deleted integer)
language plpgsql security definer as $$
declare
  v_kept     int;
  v_inserted int;
  v_updated  int;
  v_deleted  int;
begin
  -- Build the canonical row set into a temp table
  create temp table _new_beaches on commit drop as
    -- UBP spine
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

    -- CCC orphan branch
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

  -- Delete rows no longer in the canonical set
  with d as (
    delete from public.beach_locations
     where origin_key not in (select origin_key from _new_beaches)
     returning 1
  )
  select count(*) into v_deleted from d;

  -- Upsert canonical rows
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
