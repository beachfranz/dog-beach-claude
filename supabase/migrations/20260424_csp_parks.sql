-- California State Parks unit boundaries, localized from the CSP
-- ArcGIS FeatureServer (462 polygons). Complements rather than
-- replaces cpad_units — this layer exposes SUBTYPE (State Park vs
-- State Beach vs State Recreation Area vs State Historic Park), which
-- matters for dog policy (State Beaches often restrict dogs, SRAs
-- usually allow on-leash, SHPs often prohibit).
--
-- Source: https://services2.arcgis.com/AhxrK3F6WM8ECvDi/arcgis/rest/
--           services/ParkBoundaries/FeatureServer/0
-- CRS: EPSG:4326 (from GeoJSON output).
--
-- Keeps the existing csp_places point table in place for name-based
-- matching — that's populated by the pipeline separately.

create table if not exists public.csp_parks (
  fid          int primary key,          -- CSP internal FID
  unit_name    text,                     -- e.g. "Will Rogers State Beach"
  unit_nbr     text,                     -- CSP unit number
  subtype      text,                     -- "State Park" / "State Beach" / "State Recreation Area" / etc.
  gis_id       text,
  geom         geometry(MultiPolygon, 4326) not null,
  loaded_at    timestamptz not null default now()
);

create index if not exists csp_parks_geom_gix     on public.csp_parks using gist(geom);
create index if not exists csp_parks_subtype_idx  on public.csp_parks (subtype);
create index if not exists csp_parks_name_idx     on public.csp_parks (unit_name);

alter table public.csp_parks enable row level security;

create or replace function public.load_csp_parks_batch(p_features jsonb)
returns jsonb
language sql
security definer
as $$
  with candidates as (
    select
      (f->'properties'->>'FID')::int      as fid,
      f->'properties'->>'UNITNAME'         as unit_name,
      f->'properties'->>'UNITNBR'          as unit_nbr,
      f->'properties'->>'SUBTYPE'          as subtype,
      f->'properties'->>'GISID'            as gis_id,
      ST_Multi(
        ST_CollectionExtract(
          ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON((f->'geometry')::text), 4326)),
          3
        )
      ) as geom
    from jsonb_array_elements(p_features) as f
    where (f->'properties'->>'FID') is not null
      and (f->'geometry') is not null
  ),
  upserted as (
    insert into public.csp_parks (fid, unit_name, unit_nbr, subtype, gis_id, geom)
    select * from candidates
    on conflict (fid) do update set
      unit_name = excluded.unit_name,
      unit_nbr  = excluded.unit_nbr,
      subtype   = excluded.subtype,
      gis_id    = excluded.gis_id,
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

revoke all on function public.load_csp_parks_batch(jsonb) from public, anon, authenticated;
grant  execute on function public.load_csp_parks_batch(jsonb) to service_role;
