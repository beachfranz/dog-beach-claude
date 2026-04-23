-- DoD Military Installations (USA Military Bases layer) localized for CA.
-- 89 polygons in CA — active installations, training areas, ranges.
-- Source: https://services.arcgis.com/hRUr1F8lE8Jq2uJo/ArcGIS/rest/
--           services/milbases/FeatureServer/0
--         (Authoritative DoD boundaries via Esri Living Atlas)
-- CRS: EPSG:4326 (GeoJSON output).
--
-- Role: binary "no public access" flag for beaches that fall inside.
-- Major CA installations with coastal footprint: Marine Corps Base
-- Camp Pendleton (125k ac), Naval Base Coronado, Naval Base Point
-- Loma, Naval Base Ventura County (Point Mugu), Vandenberg Space
-- Force Base. CPAD covers none of these fully (CPAD scopes to
-- protected areas, not active military), so this is purely additive.

create table if not exists public.military_bases (
  objectid        int primary key,
  site_name       text,
  component       text,             -- "Army Active", "MC Active", "Navy", "Air Force", etc.
  joint_base      text,
  state_terr      text,
  state_postal    text,
  state_fips      text,
  brac_site       text,             -- "YES" / "NO" — flags closed-base realignment sites
  geom            geometry(MultiPolygon, 4326) not null,
  loaded_at       timestamptz not null default now()
);

create index if not exists military_bases_geom_gix    on public.military_bases using gist(geom);
create index if not exists military_bases_name_idx    on public.military_bases (site_name);
create index if not exists military_bases_comp_idx    on public.military_bases (component);

alter table public.military_bases enable row level security;

create or replace function public.load_military_bases_batch(p_features jsonb)
returns jsonb
language sql
security definer
as $$
  with candidates as (
    select
      (f->'properties'->>'OBJECTID')::int as objectid,
      f->'properties'->>'SITE_NAME'        as site_name,
      f->'properties'->>'COMPONENT'        as component,
      f->'properties'->>'JOINT_BASE'       as joint_base,
      f->'properties'->>'STATE_TERR'       as state_terr,
      f->'properties'->>'STPOSTAL'         as state_postal,
      f->'properties'->>'STFIPS'           as state_fips,
      f->'properties'->>'BRAC_SITE'        as brac_site,
      ST_Multi(
        ST_CollectionExtract(
          ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON((f->'geometry')::text), 4326)),
          3
        )
      ) as geom
    from jsonb_array_elements(p_features) as f
    where (f->'properties'->>'OBJECTID') is not null
      and (f->'geometry') is not null
  ),
  upserted as (
    insert into public.military_bases (
      objectid, site_name, component, joint_base,
      state_terr, state_postal, state_fips, brac_site, geom
    )
    select * from candidates
    on conflict (objectid) do update set
      site_name    = excluded.site_name,
      component    = excluded.component,
      joint_base   = excluded.joint_base,
      state_terr   = excluded.state_terr,
      state_postal = excluded.state_postal,
      state_fips   = excluded.state_fips,
      brac_site    = excluded.brac_site,
      geom         = excluded.geom,
      loaded_at    = now()
    returning 1
  )
  select jsonb_build_object(
    'total',    jsonb_array_length(p_features),
    'affected', (select count(*)::int from upserted),
    'skipped',  jsonb_array_length(p_features) - (select count(*)::int from candidates)
  );
$$;

revoke all on function public.load_military_bases_batch(jsonb) from public, anon, authenticated;
grant  execute on function public.load_military_bases_batch(jsonb) to service_role;
