-- NOAA CO-OPS tide-prediction station list, filtered to CA (192 stations).
--
-- Source: https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/
--           stations.json?type=tidepredictions
-- Same endpoint registered in pipeline_sources as noaa_tide_stations
-- and used by v2-noaa-station-match per-call; this table caches it so
-- nearest-station lookups become a PostGIS ORDER BY <-> instead of an
-- HTTP fetch + haversine loop.
--
-- Two station types (reference vs subordinate) differ in rigor:
--   type 'R' (Reference): direct tide predictions
--   type 'S' (Subordinate): offsets from a reference station
-- The existing pipeline filters to Reference stations (reference_id
-- IS NULL) for beach assignment. Stored as-is so consumers can choose.

create table if not exists public.noaa_stations (
  station_id      text primary key,               -- e.g. "9410580"
  name            text,
  state           text,
  station_type    text,                            -- "R" = Reference, "S" = Subordinate
  reference_id    text,                            -- set on subordinate stations
  time_meridian   double precision,
  time_zone_corr  double precision,
  latitude        double precision,
  longitude       double precision,
  geom            geometry(Point, 4326) not null,
  loaded_at       timestamptz not null default now()
);

create index if not exists noaa_stations_geom_gix    on public.noaa_stations using gist(geom);
create index if not exists noaa_stations_state_idx   on public.noaa_stations (state);
create index if not exists noaa_stations_type_idx    on public.noaa_stations (station_type);

alter table public.noaa_stations enable row level security;

-- Batch-insert RPC. NOAA's stations.json endpoint returns plain JSON
-- (not GeoJSON), so the caller passes raw station objects and we
-- construct the geometry from lat/lng.
create or replace function public.load_noaa_stations_batch(p_stations jsonb)
returns jsonb
language sql
security definer
as $$
  with candidates as (
    select
      s->>'id'                                           as station_id,
      s->>'name'                                         as name,
      s->>'state'                                        as state,
      s->>'type'                                         as station_type,
      case when s->>'reference_id' = '' then null
           else s->>'reference_id' end                   as reference_id,
      nullif(s->>'timemeridian','')::double precision    as time_meridian,
      nullif(s->>'timezonecorr','')::double precision    as time_zone_corr,
      (s->>'lat')::double precision                      as latitude,
      (s->>'lng')::double precision                      as longitude,
      ST_SetSRID(ST_MakePoint(
        (s->>'lng')::double precision,
        (s->>'lat')::double precision
      ), 4326)                                            as geom
    from jsonb_array_elements(p_stations) as s
    where s->>'id' is not null
      and s->>'lat' is not null
      and s->>'lng' is not null
  ),
  upserted as (
    insert into public.noaa_stations (
      station_id, name, state, station_type, reference_id,
      time_meridian, time_zone_corr, latitude, longitude, geom
    )
    select * from candidates
    on conflict (station_id) do update set
      name           = excluded.name,
      state          = excluded.state,
      station_type   = excluded.station_type,
      reference_id   = excluded.reference_id,
      time_meridian  = excluded.time_meridian,
      time_zone_corr = excluded.time_zone_corr,
      latitude       = excluded.latitude,
      longitude      = excluded.longitude,
      geom           = excluded.geom,
      loaded_at      = now()
    returning 1
  )
  select jsonb_build_object(
    'total',    jsonb_array_length(p_stations),
    'affected', (select count(*)::int from upserted),
    'skipped',  jsonb_array_length(p_stations) - (select count(*)::int from candidates)
  );
$$;

revoke all on function public.load_noaa_stations_batch(jsonb) from public, anon, authenticated;
grant  execute on function public.load_noaa_stations_batch(jsonb) to service_role;
