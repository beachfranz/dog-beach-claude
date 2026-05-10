-- 20260509_census_tracts.sql
--
-- US Census tract polygons + ACS 5-year population. Source: TIGER/Line
-- 2024 (geometry) + ACS 2023 5-year estimates (population). ~84K rows
-- nationwide. Backs the upcoming radius-population-density signal for
-- beach tier sub-segmentation (urban/suburban/rural catchment).
--
-- Loaded by:
--   scripts/one_off/bulk_load_census_tracts.py — downloads per-state
--   shapefile, calls load_census_tracts_shapefile.py (geometry batch),
--   then queries Census ACS API for B01003_001E (total population) and
--   updates the population column via update_census_tract_population.

begin;

create table if not exists public.census_tracts (
  geoid       text primary key,             -- 11-char: STATEFP+COUNTYFP+TRACTCE
  state_fp    text not null,                -- 2-char
  county_fp   text not null,                -- 3-char
  tract_ce    text not null,                -- 6-char
  name        text,                         -- short name (e.g. '1.01')
  namelsad    text,                         -- 'Census Tract 1.01'
  funcstat    text,
  aland       bigint,
  awater      bigint,
  intpt_lat   numeric,
  intpt_lon   numeric,
  population  int,                          -- ACS B01003_001E (5yr)
  acs_vintage int,                          -- e.g. 2023
  geom        geometry(MultiPolygon, 4326) not null,
  loaded_at   timestamptz not null default now()
);

create index if not exists census_tracts_state_idx
  on public.census_tracts (state_fp);
create index if not exists census_tracts_county_idx
  on public.census_tracts (state_fp, county_fp);
create index if not exists census_tracts_geom_gix
  on public.census_tracts using gist (geom);

grant select on public.census_tracts to anon, authenticated, service_role;

-- ----------------------------------------------------------------------
-- Batch loader for shapefile geometry. Mirrors load_jurisdictions_batch.
-- Each element of p_batch is {props: {...TIGER fields...}, geom: <GeoJSON>}.
-- ----------------------------------------------------------------------
create or replace function public.load_census_tracts_batch(p_batch jsonb)
returns int
language plpgsql
as $function$
declare
  v_count int := 0;
  rec     jsonb;
  props   jsonb;
  geom_gj jsonb;
  v_geom  geometry;
begin
  for rec in select * from jsonb_array_elements(p_batch) loop
    props   := rec->'props';
    geom_gj := rec->'geom';

    -- Force MultiPolygon to keep the column shape consistent
    v_geom := st_multi(st_setsrid(st_geomfromgeojson(geom_gj::text), 4326));

    insert into public.census_tracts
      (geoid, state_fp, county_fp, tract_ce, name, namelsad,
       funcstat, aland, awater, intpt_lat, intpt_lon, geom)
    values (
      props->>'GEOID',
      props->>'STATEFP',
      props->>'COUNTYFP',
      props->>'TRACTCE',
      props->>'NAME',
      props->>'NAMELSAD',
      props->>'FUNCSTAT',
      nullif(props->>'ALAND','')::bigint,
      nullif(props->>'AWATER','')::bigint,
      nullif(props->>'INTPTLAT','')::numeric,
      nullif(props->>'INTPTLON','')::numeric,
      v_geom
    )
    on conflict (geoid) do update
      set state_fp  = excluded.state_fp,
          county_fp = excluded.county_fp,
          tract_ce  = excluded.tract_ce,
          name      = excluded.name,
          namelsad  = excluded.namelsad,
          funcstat  = excluded.funcstat,
          aland     = excluded.aland,
          awater    = excluded.awater,
          intpt_lat = excluded.intpt_lat,
          intpt_lon = excluded.intpt_lon,
          geom      = excluded.geom,
          loaded_at = now();

    v_count := v_count + 1;
  end loop;
  return v_count;
end;
$function$;

grant execute on function public.load_census_tracts_batch(jsonb) to service_role;

-- ----------------------------------------------------------------------
-- Population updater. Each element of p_batch is {geoid, population}.
-- Used by bulk_load_census_tracts.py after the ACS API pull.
-- ----------------------------------------------------------------------
create or replace function public.update_census_tract_population(
  p_batch jsonb,
  p_vintage int default 2023
)
returns int
language plpgsql
as $function$
declare
  v_count int := 0;
  rec     jsonb;
begin
  for rec in select * from jsonb_array_elements(p_batch) loop
    update public.census_tracts
       set population  = nullif(rec->>'population','')::int,
           acs_vintage = p_vintage
     where geoid = rec->>'geoid';
    if found then v_count := v_count + 1; end if;
  end loop;
  return v_count;
end;
$function$;

grant execute on function public.update_census_tract_population(jsonb, int) to service_role;

commit;
