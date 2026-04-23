-- US state boundary polygons, populated from Census TIGERweb ArcGIS REST.
-- Used by load-beaches-staging to assign a state to each ingested beach
-- via ST_Contains on lat/lon — replaces the brittle ADDR2 regex filter
-- that was silently dropping ~30% of the old staging set (including most
-- iconic state and city beaches) because of multi-line quoted addresses
-- in the source CSV.

create extension if not exists postgis;

create table if not exists public.states (
  state_code text primary key,              -- two-letter, e.g. "CA"
  state_name text not null,                 -- full, e.g. "California"
  geom       geometry(MultiPolygon, 4326) not null,
  loaded_at  timestamptz not null default now()
);

create index if not exists states_geom_gix on public.states using gist(geom);

alter table public.states enable row level security;

-- RPC used by admin-load-us-states edge function. Takes geometry as
-- a GeoJSON text blob so the edge function doesn't need to stringify
-- PostGIS WKT itself. Returns true when the row was newly inserted,
-- false when an existing row was updated.
create or replace function public.load_state_feature(
  p_state_code text,
  p_state_name text,
  p_geojson    text
) returns boolean
language plpgsql
security definer
as $$
declare
  existed boolean;
begin
  select true into existed from public.states where state_code = p_state_code;
  insert into public.states (state_code, state_name, geom)
  values (
    p_state_code,
    p_state_name,
    ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(p_geojson), 4326))
  )
  on conflict (state_code) do update
    set state_name = excluded.state_name,
        geom      = excluded.geom,
        loaded_at = now();
  return not coalesce(existed, false);
end;
$$;

revoke all on function public.load_state_feature(text, text, text) from public, anon, authenticated;
grant  execute on function public.load_state_feature(text, text, text) to service_role;
