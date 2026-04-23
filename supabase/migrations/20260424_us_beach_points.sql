-- Master beach-point inventory. Loads US_beaches_with_state.csv into a
-- permanent table so ingest steps can do set-based SQL joins instead of
-- re-parsing the CSV from Supabase Storage every time.
--
-- Source CSV: share/Dog_Beaches/US_beaches_with_state.csv (8,041 rows).
-- The STATE column was added by scripts/add_state_to_csv.py (commit
-- 6fdfc09) via classify_points_to_state RPC — nearest-state lookup
-- against the states table. Now stored here so downstream consumers
-- can skip that step too.
--
-- CRS: EPSG:4326 per project_crs_convention.md. WKT is parsed to a
-- Point geometry at load time.

create table if not exists public.us_beach_points (
  fid          int primary key,               -- original unique ID from US_beaches.csv
  name         text,
  country      text,
  addr1        text,
  addr2        text,
  addr3        text,
  addr4        text,
  addr5        text,
  cat_mod      text,                           -- category/moderation flag
  state        text,                           -- 2-letter USPS code (from classify_points_to_state)
  raw_wkt      text,                           -- original WKT string from CSV
  geom         geometry(Point, 4326) not null, -- parsed from WKT at ingest
  loaded_at    timestamptz not null default now()
);

create index if not exists us_beach_points_geom_gix   on public.us_beach_points using gist(geom);
create index if not exists us_beach_points_state_idx  on public.us_beach_points (state);
create index if not exists us_beach_points_name_idx   on public.us_beach_points (name);

alter table public.us_beach_points enable row level security;

-- Batch upsert RPC. Caller passes an array of objects with keys matching
-- the CSV columns. WKT parsed via ST_GeomFromText.
create or replace function public.load_us_beach_points_batch(p_rows jsonb)
returns jsonb
language sql
security definer
as $$
  with candidates as (
    select
      (r->>'fid')::int                                    as fid,
      r->>'NAME'                                           as name,
      r->>'COUNTRY'                                        as country,
      r->>'ADDR1'                                          as addr1,
      r->>'ADDR2'                                          as addr2,
      r->>'ADDR3'                                          as addr3,
      r->>'ADDR4'                                          as addr4,
      r->>'ADDR5'                                          as addr5,
      r->>'CAT_MOD'                                        as cat_mod,
      r->>'STATE'                                          as state,
      r->>'WKT'                                            as raw_wkt,
      ST_SetSRID(ST_GeomFromText(r->>'WKT'), 4326)         as geom
    from jsonb_array_elements(p_rows) as r
    where r->>'fid' is not null
      and r->>'WKT' is not null
  ),
  upserted as (
    insert into public.us_beach_points (
      fid, name, country, addr1, addr2, addr3, addr4, addr5,
      cat_mod, state, raw_wkt, geom
    )
    select * from candidates
    on conflict (fid) do update set
      name      = excluded.name,
      country   = excluded.country,
      addr1     = excluded.addr1,
      addr2     = excluded.addr2,
      addr3     = excluded.addr3,
      addr4     = excluded.addr4,
      addr5     = excluded.addr5,
      cat_mod   = excluded.cat_mod,
      state     = excluded.state,
      raw_wkt   = excluded.raw_wkt,
      geom      = excluded.geom,
      loaded_at = now()
    returning 1
  )
  select jsonb_build_object(
    'total',    jsonb_array_length(p_rows),
    'affected', (select count(*)::int from upserted),
    'skipped',  jsonb_array_length(p_rows) - (select count(*)::int from candidates)
  );
$$;

revoke all on function public.load_us_beach_points_batch(jsonb) from public, anon, authenticated;
grant  execute on function public.load_us_beach_points_batch(jsonb) to service_role;
