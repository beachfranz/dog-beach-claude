-- USGS National Hydrography Dataset (NHD) Waterbodies — CA lakes and
-- reservoirs larger than 10 hectares (~24 acres). 3,145 polygons.
--
-- Source: https://hydro.nationalmap.gov/arcgis/rest/services/nhd/
--           MapServer/12 (Waterbody - Large Scale)
-- Filter: FTYPE IN (390 Lake/Pond, 436 Reservoir) AND AREASQKM > 0.1.
-- CRS: EPSG:4326.
--
-- Role: identifies WHICH lake a beach point sits on, so we can group
-- inland beaches ("all beaches on Lake Tahoe") and distinguish ocean
-- vs lake beaches for scoring purposes. Per feedback_scope_includes_
-- lakes.md, inland lake beaches are in scope.
--
-- Threshold chosen so every lake that could plausibly host a public
-- beach is included (Tahoe, Clear Lake, Shasta, Berryessa, Don Pedro,
-- Isabella, Perris, Salton Sea, every reservoir with recreation) while
-- excluding farm ponds, stock tanks, irrigation retention basins.

create table if not exists public.waterbodies (
  objectid             int primary key,
  permanent_identifier text,
  gnis_id              text,
  gnis_name            text,                            -- USGS canonical name when available
  reach_code           text,
  ftype                int,                             -- 390 = Lake/Pond, 436 = Reservoir
  fcode                int,                             -- finer classification
  area_sq_km           double precision,
  elevation            double precision,
  geom                 geometry(MultiPolygon, 4326) not null,
  loaded_at            timestamptz not null default now()
);

create index if not exists waterbodies_geom_gix    on public.waterbodies using gist(geom);
create index if not exists waterbodies_gnis_idx    on public.waterbodies (gnis_name);
create index if not exists waterbodies_ftype_idx   on public.waterbodies (ftype);
create index if not exists waterbodies_size_idx    on public.waterbodies (area_sq_km);

alter table public.waterbodies enable row level security;

create or replace function public.load_waterbodies_batch(p_features jsonb)
returns jsonb
language sql
security definer
as $$
  with candidates as (
    select
      (f->'properties'->>'OBJECTID')::int                             as objectid,
      f->'properties'->>'PERMANENT_IDENTIFIER'                         as permanent_identifier,
      f->'properties'->>'GNIS_ID'                                      as gnis_id,
      f->'properties'->>'GNIS_NAME'                                    as gnis_name,
      f->'properties'->>'REACHCODE'                                    as reach_code,
      nullif(f->'properties'->>'FTYPE','')::int                        as ftype,
      nullif(f->'properties'->>'FCODE','')::int                        as fcode,
      nullif(f->'properties'->>'AREASQKM','')::double precision        as area_sq_km,
      nullif(f->'properties'->>'ELEVATION','')::double precision       as elevation,
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
    insert into public.waterbodies (
      objectid, permanent_identifier, gnis_id, gnis_name, reach_code,
      ftype, fcode, area_sq_km, elevation, geom
    )
    select * from candidates
    on conflict (objectid) do update set
      permanent_identifier = excluded.permanent_identifier,
      gnis_id              = excluded.gnis_id,
      gnis_name            = excluded.gnis_name,
      reach_code           = excluded.reach_code,
      ftype                = excluded.ftype,
      fcode                = excluded.fcode,
      area_sq_km           = excluded.area_sq_km,
      elevation            = excluded.elevation,
      geom                 = excluded.geom,
      loaded_at            = now()
    returning 1
  )
  select jsonb_build_object(
    'total',    jsonb_array_length(p_features),
    'affected', (select count(*)::int from upserted),
    'skipped',  jsonb_array_length(p_features) - (select count(*)::int from candidates)
  );
$$;

revoke all on function public.load_waterbodies_batch(jsonb) from public, anon, authenticated;
grant  execute on function public.load_waterbodies_batch(jsonb) to service_role;
