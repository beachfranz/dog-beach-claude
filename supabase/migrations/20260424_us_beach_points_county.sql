-- Add county_fips + county_name to us_beach_points. Populated at ingest
-- by nearest-county KNN against the public.counties table (now national).
--
-- FIPS is the 5-digit state+county code (e.g. "06037" = Los Angeles
-- County). Gold-standard federal identifier — enables joins with
-- Census, HIFLD, CDC, NWS datasets without fuzzy name matching.

alter table public.us_beach_points
  add column if not exists county_fips text,
  add column if not exists county_name text;

create index if not exists us_beach_points_county_fips_idx
  on public.us_beach_points (county_fips);

-- Update load_us_beach_points_batch to derive county (and state, unchanged)
-- at ingest via nearest-county KNN.
create or replace function public.load_us_beach_points_batch(p_rows jsonb)
returns jsonb
language sql
security definer
as $$
  with raw_rows as (
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
      r->>'WKT'                                            as raw_wkt,
      ST_SetSRID(ST_GeomFromText(r->>'WKT'), 4326)         as geom
    from jsonb_array_elements(p_rows) as r
    where r->>'fid' is not null
      and r->>'WKT' is not null
  ),
  enriched as (
    select r.*,
           (select s.state_code from public.states s
            order by s.geom <-> r.geom limit 1) as state,
           (select c.geoid     from public.counties c
            order by c.geom <-> r.geom limit 1) as county_fips,
           (select c.name_full from public.counties c
            order by c.geom <-> r.geom limit 1) as county_name
    from raw_rows r
  ),
  upserted as (
    insert into public.us_beach_points (
      fid, name, country, addr1, addr2, addr3, addr4, addr5,
      cat_mod, state, county_fips, county_name, raw_wkt, geom
    )
    select fid, name, country, addr1, addr2, addr3, addr4, addr5,
           cat_mod, state, county_fips, county_name, raw_wkt, geom
    from enriched
    on conflict (fid) do update set
      name        = excluded.name,
      country     = excluded.country,
      addr1       = excluded.addr1,
      addr2       = excluded.addr2,
      addr3       = excluded.addr3,
      addr4       = excluded.addr4,
      addr5       = excluded.addr5,
      cat_mod     = excluded.cat_mod,
      state       = excluded.state,
      county_fips = excluded.county_fips,
      county_name = excluded.county_name,
      raw_wkt     = excluded.raw_wkt,
      geom        = excluded.geom,
      loaded_at   = now()
    returning 1
  )
  select jsonb_build_object(
    'total',    jsonb_array_length(p_rows),
    'affected', (select count(*)::int from upserted),
    'skipped',  jsonb_array_length(p_rows) - (select count(*)::int from raw_rows)
  );
$$;

revoke all on function public.load_us_beach_points_batch(jsonb) from public, anon, authenticated;
grant  execute on function public.load_us_beach_points_batch(jsonb) to service_role;
