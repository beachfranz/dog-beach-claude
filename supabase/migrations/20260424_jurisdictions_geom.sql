-- Add geom column + GIST index + loader RPC to jurisdictions (2026-04-24)
-- Existing table has name/fips/county/state/place_type text columns but no
-- geometry. TIGER Places (CLASSFP C1/C2/… = incorporated, U1 = CDP) gives
-- us spatial polygons for all 540-ish CA places.
--
-- `place_type` stores TIGER CLASSFP verbatim (C1/C2/C3/C5/U1/etc.) so
-- consumers can filter to incorporated via `place_type LIKE 'C%'`.

alter table public.jurisdictions
  add column if not exists geom      geometry(MultiPolygon, 4326),
  add column if not exists namelsad  text,
  add column if not exists funcstat  text,
  add column if not exists loaded_at timestamptz not null default now();

create index if not exists jurisdictions_geom_gix
  on public.jurisdictions using gist (geom);

create index if not exists jurisdictions_place_type_idx
  on public.jurisdictions(place_type);

comment on column public.jurisdictions.place_type is
  'TIGER CLASSFP: C1/C2/C3/C5/C6/C7 = incorporated place, U1 = census-designated place (unincorporated). Filter to incorporated via place_type LIKE ''C%''.';

-- Loader RPC: accepts a jsonb batch of features, upserts by fips_place + fips_state
create or replace function public.load_jurisdictions_batch(p_batch jsonb)
returns int
language plpgsql
security definer
as $$
declare
  f jsonb;
  n int := 0;
begin
  for f in select * from jsonb_array_elements(p_batch)
  loop
    insert into public.jurisdictions (
      name, namelsad, place_type, funcstat,
      fips_state, fips_place, fips_county,
      state, county, geom, loaded_at
    ) values (
      f->'props'->>'NAME',
      f->'props'->>'NAMELSAD',
      f->'props'->>'CLASSFP',
      f->'props'->>'FUNCSTAT',
      f->'props'->>'STATEFP',
      f->'props'->>'PLACEFP',
      null,  -- no county FIPS in TIGER Places; derive at query time if needed
      'CA',
      null,
      ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(f->>'geom'), 4326)),
      now()
    )
    on conflict do nothing;  -- no unique constraint yet; upsert logic reworked below
    n := n + 1;
  end loop;
  return n;
end;
$$;

-- Unique index to enable upserts cleanly on re-runs
create unique index if not exists jurisdictions_unique_place
  on public.jurisdictions(fips_state, fips_place);

-- Rewrite loader to use the unique index for true upsert
create or replace function public.load_jurisdictions_batch(p_batch jsonb)
returns int
language plpgsql
security definer
as $$
declare
  f jsonb;
  n int := 0;
begin
  for f in select * from jsonb_array_elements(p_batch)
  loop
    insert into public.jurisdictions (
      name, namelsad, place_type, funcstat,
      fips_state, fips_place, fips_county,
      state, county, geom, loaded_at
    ) values (
      f->'props'->>'NAME',
      f->'props'->>'NAMELSAD',
      f->'props'->>'CLASSFP',
      f->'props'->>'FUNCSTAT',
      f->'props'->>'STATEFP',
      f->'props'->>'PLACEFP',
      null,
      'CA',
      null,
      ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(f->>'geom'), 4326)),
      now()
    )
    on conflict (fips_state, fips_place) do update set
      name      = excluded.name,
      namelsad  = excluded.namelsad,
      place_type = excluded.place_type,
      funcstat  = excluded.funcstat,
      geom      = excluded.geom,
      loaded_at = now();
    n := n + 1;
  end loop;
  return n;
end;
$$;

revoke all on function public.load_jurisdictions_batch(jsonb) from public, anon, authenticated;
grant  execute on function public.load_jurisdictions_batch(jsonb) to service_role;
