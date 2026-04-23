-- US Census TIGER/Line county boundaries, localized from the national
-- 2024 TIGER shapefile and filtered to California at load time (STATEFP =
-- '06' → 58 counties). Replaces per-row Google Maps reverse geocoding for
-- the county field.
--
-- Source shapefile projection: NAD83 geographic (EPSG:4269). Reprojected
-- to WGS84 (EPSG:4326) at ingest per project_crs_convention.md.
--
-- Populated by scripts/load_counties_shapefile.py via direct SQL
-- (same pattern as load_cpad_shapefile_direct.py). Idempotent — safe to
-- re-run when Census publishes a new vintage.

create table if not exists public.counties (
  geoid       text primary key,                  -- 5-digit state+county FIPS, e.g. "06059" = Orange County
  state_fp    text not null,                     -- 2-digit state FIPS, e.g. "06" = California
  county_fp   text not null,                     -- 3-digit county FIPS, e.g. "059" = Orange
  name        text not null,                     -- short form, e.g. "Orange"
  name_full   text not null,                     -- legal form with suffix, e.g. "Orange County"
  aland       bigint,                            -- land area in m²
  awater      bigint,                            -- water area in m²
  intpt_lat   double precision,                  -- interior point (for label placement)
  intpt_lon   double precision,
  geom        geometry(MultiPolygon, 4326) not null,
  loaded_at   timestamptz not null default now()
);

create index if not exists counties_geom_gix   on public.counties using gist(geom);
create index if not exists counties_state_idx  on public.counties (state_fp);
create index if not exists counties_name_idx   on public.counties (name);

alter table public.counties enable row level security;

-- Batch upsert RPC. Same pattern as load_cpad_batch — takes a GeoJSON
-- FeatureCollection-shaped jsonb array, parses via jsonb_array_elements,
-- and inserts in a single set-based statement. ST_Multi(ST_CollectionExtract(
-- ST_MakeValid(...), 3)) normalises polygons to MultiPolygon and strips
-- stray lines/points ST_MakeValid may emit.
create or replace function public.load_counties_batch(p_features jsonb)
returns jsonb
language sql
security definer
as $$
  with candidates as (
    select
      f->'properties'->>'GEOID'                      as geoid,
      f->'properties'->>'STATEFP'                    as state_fp,
      f->'properties'->>'COUNTYFP'                   as county_fp,
      f->'properties'->>'NAME'                       as name,
      f->'properties'->>'NAMELSAD'                   as name_full,
      nullif(f->'properties'->>'ALAND','')::bigint   as aland,
      nullif(f->'properties'->>'AWATER','')::bigint  as awater,
      nullif(f->'properties'->>'INTPTLAT','')::double precision as intpt_lat,
      nullif(f->'properties'->>'INTPTLON','')::double precision as intpt_lon,
      ST_Multi(
        ST_CollectionExtract(
          ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON((f->'geometry')::text), 4326)),
          3
        )
      ) as geom
    from jsonb_array_elements(p_features) as f
    where (f->'properties'->>'GEOID') is not null
      and (f->'geometry') is not null
  ),
  upserted as (
    insert into public.counties (
      geoid, state_fp, county_fp, name, name_full,
      aland, awater, intpt_lat, intpt_lon, geom
    )
    select * from candidates
    on conflict (geoid) do update set
      state_fp  = excluded.state_fp,
      county_fp = excluded.county_fp,
      name      = excluded.name,
      name_full = excluded.name_full,
      aland     = excluded.aland,
      awater    = excluded.awater,
      intpt_lat = excluded.intpt_lat,
      intpt_lon = excluded.intpt_lon,
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

revoke all on function public.load_counties_batch(jsonb) from public, anon, authenticated;
grant  execute on function public.load_counties_batch(jsonb) to service_role;
