-- Switch us_beach_points.state from "copied from CSV" to "derived from
-- geom at ingest". Before: the STATE column came directly from the
-- CSV's pre-computed column (set by scripts/add_state_to_csv.py
-- yesterday against the then-simplified state polygons). After: every
-- load — whether ingesting a new CSV row, a backfill from the existing
-- CSV, or any future reload — computes state by nearest-state lookup
-- against the current (hi-res TIGER) public.states table.
--
-- Makes us_beach_points.state an owned value rather than an inherited
-- one. Also means STATE in the CSV is now ignored at load time; if it
-- differs from the nearest-state computation, nearest-state wins.

create or replace function public.load_us_beach_points_batch(p_rows jsonb)
returns jsonb
language sql
security definer
as $$
  with raw_rows as (
    select
      (r->>'fid')::int                                   as fid,
      r->>'NAME'                                          as name,
      r->>'COUNTRY'                                       as country,
      r->>'ADDR1'                                         as addr1,
      r->>'ADDR2'                                         as addr2,
      r->>'ADDR3'                                         as addr3,
      r->>'ADDR4'                                         as addr4,
      r->>'ADDR5'                                         as addr5,
      r->>'CAT_MOD'                                       as cat_mod,
      r->>'WKT'                                           as raw_wkt,
      ST_SetSRID(ST_GeomFromText(r->>'WKT'), 4326)        as geom
    from jsonb_array_elements(p_rows) as r
    where r->>'fid' is not null
      and r->>'WKT' is not null
  ),
  with_state as (
    select r.*,
           (select s.state_code
              from public.states s
              order by s.geom <-> r.geom
              limit 1) as state
    from raw_rows r
  ),
  upserted as (
    insert into public.us_beach_points (
      fid, name, country, addr1, addr2, addr3, addr4, addr5,
      cat_mod, state, raw_wkt, geom
    )
    select fid, name, country, addr1, addr2, addr3, addr4, addr5,
           cat_mod, state, raw_wkt, geom
    from with_state
    on conflict (fid) do update set
      name      = excluded.name,
      country   = excluded.country,
      addr1     = excluded.addr1,
      addr2     = excluded.addr2,
      addr3     = excluded.addr3,
      addr4     = excluded.addr4,
      addr5     = excluded.addr5,
      cat_mod   = excluded.cat_mod,
      state     = excluded.state,     -- derived from current states polygon
      raw_wkt   = excluded.raw_wkt,
      geom      = excluded.geom,
      loaded_at = now()
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
