-- California Marine Protected Areas (CDFW ds582) localized into PostGIS.
-- 155 polygons covering every State Marine Reserve, State Marine
-- Conservation Area, Special Closure, etc. along the CA coast.
--
-- Source: https://services2.arcgis.com/Uq9r85Potqm3MfRV/arcgis/rest/
--           services/biosds582_fpu/FeatureServer/0
--         (CDFW Open Data / CA Open Data Portal, canonical MPA dataset)
-- CRS: EPSG:4326 (GeoJSON output).
--
-- Role: dog-restriction signal. Most MPAs — especially State Marine
-- Reserves (SMRs) — prohibit or severely restrict dogs. Not a
-- jurisdiction classifier (CPAD does that) but an additional
-- overlay CPAD doesn't cleanly capture for marine zones.

create table if not exists public.mpas (
  objectid       int primary key,
  name           text,
  full_name      text,
  short_name     text,
  mpa_type       text,                          -- "State Marine Reserve", "SMCA", "SMRMA", etc.
  ccr_section    text,                          -- California Code of Regulations citation
  ccr_int        int,
  study_region   text,                          -- North Coast / San Francisco / Central Coast / South Coast
  area_sq_mi     double precision,
  acres          double precision,
  hectares       double precision,
  geom           geometry(MultiPolygon, 4326) not null,
  loaded_at      timestamptz not null default now()
);

create index if not exists mpas_geom_gix    on public.mpas using gist(geom);
create index if not exists mpas_type_idx    on public.mpas (mpa_type);
create index if not exists mpas_region_idx  on public.mpas (study_region);

alter table public.mpas enable row level security;

create or replace function public.load_mpas_batch(p_features jsonb)
returns jsonb
language sql
security definer
as $$
  with candidates as (
    select
      (f->'properties'->>'OBJECTID')::int             as objectid,
      f->'properties'->>'NAME'                         as name,
      f->'properties'->>'FULLNAME'                     as full_name,
      f->'properties'->>'SHORTNAME'                    as short_name,
      f->'properties'->>'Type'                         as mpa_type,
      f->'properties'->>'CCR'                          as ccr_section,
      nullif(f->'properties'->>'CCR_Int','')::int      as ccr_int,
      f->'properties'->>'Study_Regi'                   as study_region,
      nullif(f->'properties'->>'Area_sq_mi','')::double precision as area_sq_mi,
      nullif(f->'properties'->>'Acres','')::double precision      as acres,
      nullif(f->'properties'->>'Hectares','')::double precision   as hectares,
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
    insert into public.mpas (
      objectid, name, full_name, short_name, mpa_type, ccr_section, ccr_int,
      study_region, area_sq_mi, acres, hectares, geom
    )
    select * from candidates
    on conflict (objectid) do update set
      name         = excluded.name,
      full_name    = excluded.full_name,
      short_name   = excluded.short_name,
      mpa_type     = excluded.mpa_type,
      ccr_section  = excluded.ccr_section,
      ccr_int      = excluded.ccr_int,
      study_region = excluded.study_region,
      area_sq_mi   = excluded.area_sq_mi,
      acres        = excluded.acres,
      hectares     = excluded.hectares,
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

revoke all on function public.load_mpas_batch(jsonb) from public, anon, authenticated;
grant  execute on function public.load_mpas_batch(jsonb) to service_role;
