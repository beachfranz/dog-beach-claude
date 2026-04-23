-- BIA American Indian & Alaska Native Land Area Representations,
-- filtered to California via bbox (138 polygons).
--
-- Source: https://services.arcgis.com/cJ9YHowT8TU7DUyn/ArcGIS/rest/
--           services/BND___American_Indian___Alaska_Native_Land_Area_
--           Representations__BIA_/FeatureServer/1
-- CRS: EPSG:4326.
--
-- Role: separate-jurisdiction overlay. Tribal lands have their own
-- governance and dog rules that neither CPAD nor PAD-US cleanly
-- capture. Most federal / state jurisdiction analysis doesn't apply
-- — tribe-specific rules do.

create table if not exists public.tribal_lands (
  objectid    int primary key,
  lar_id      text,                                  -- BIA LAR identifier, e.g. "LAR0095"
  lar_name    text,                                  -- tribe/area name, e.g. "Agua Caliente LAR"
  gis_acres   double precision,
  geom        geometry(MultiPolygon, 4326) not null,
  loaded_at   timestamptz not null default now()
);

create index if not exists tribal_lands_geom_gix   on public.tribal_lands using gist(geom);
create index if not exists tribal_lands_name_idx   on public.tribal_lands (lar_name);

alter table public.tribal_lands enable row level security;

create or replace function public.load_tribal_lands_batch(p_features jsonb)
returns jsonb
language sql
security definer
as $$
  with candidates as (
    select
      (f->'properties'->>'OBJECTID')::int                             as objectid,
      f->'properties'->>'LARID'                                        as lar_id,
      f->'properties'->>'LARName'                                      as lar_name,
      nullif(f->'properties'->>'GISAcres','')::double precision        as gis_acres,
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
    insert into public.tribal_lands (objectid, lar_id, lar_name, gis_acres, geom)
    select * from candidates
    on conflict (objectid) do update set
      lar_id    = excluded.lar_id,
      lar_name  = excluded.lar_name,
      gis_acres = excluded.gis_acres,
      geom      = excluded.geom,
      loaded_at = now()
    returning 1
  )
  select jsonb_build_object(
    'total',    jsonb_array_length(p_features),
    'affected', (select count(*)::int from upserted),
    'skipped',  jsonb_array_length(p_features) - (select count(*)::int from candidates)
  );
$$;

revoke all on function public.load_tribal_lands_batch(jsonb) from public, anon, authenticated;
grant  execute on function public.load_tribal_lands_batch(jsonb) to service_role;
