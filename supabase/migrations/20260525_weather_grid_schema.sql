-- 20260525_weather_grid_schema.sql
--
-- W1.1 of weather-grid-reference-layer (see project pin
-- [[weather-grid-reference-layer]]). Two new tables:
--
--   weather_grid           — materialized inventory of active grid cells
--                            (cells with ≥1 consumer entity). Auto-populated
--                            by triggers on beaches_gold + dog_parks_gold.
--   weather_grid_hourly    — 168-hour rolling forecast + past-3-days
--                            observed weather per cell. The reference
--                            layer consumers JOIN against.
--
-- Grid resolution: 0.025° (~3km at CA lat) — matches HRRR-NA native
-- resolution; captures coastal/inland weather differential. Floor-bin
-- semantics: cell (X, Y) covers [X, X+0.025) × [Y, Y+0.025).
--
-- Consumers (daily-beach-refresh / daily-dog-park-refresh / NOW fns)
-- stop fetching Open-Meteo and read from weather_grid_hourly via
-- weather_for_point() helper (added in W1.2).
--
-- Loader: Python on Dagster (NOT edge function — Dagster has no compute
-- budget limit and supports concurrency natively). See W1.5.

begin;

-- ───────────────────────────────────────────────────────────────────────────
-- 1. weather_grid — materialized active-cell inventory
-- ───────────────────────────────────────────────────────────────────────────

create table public.weather_grid (
  grid_lat   numeric(6,3) not null,
  grid_lon   numeric(7,3) not null,
  geom       geography(point, 4326),
  state      text,                            -- audit / partitioning hint
  first_seen timestamptz not null default now(),
  primary key (grid_lat, grid_lon)
);

create index weather_grid_geom_gix on public.weather_grid using gist (geom);
create index weather_grid_state_idx on public.weather_grid (state);

comment on table public.weather_grid is
  'Materialized inventory of active weather grid cells. Auto-populated by triggers on beaches_gold + dog_parks_gold. One row per (grid_lat, grid_lon) tuple where ≥1 consumer entity exists. Loader reads this to know what cells to refresh. See [[weather-grid-reference-layer]].';
comment on column public.weather_grid.grid_lat is
  '0.025° floor-bin latitude (e.g. 32.725 for any lat in [32.725, 32.750)). NOT Open-Meteo''s native model grid — our dedup key.';
comment on column public.weather_grid.grid_lon is
  '0.025° floor-bin longitude. NOT Open-Meteo''s native model grid — our dedup key.';
comment on column public.weather_grid.geom is
  'Geography POINT at the cell''s SW corner. For PIP / KNN spatial queries.';

-- ───────────────────────────────────────────────────────────────────────────
-- 2. weather_grid_hourly — the actual time-series reference layer
-- ───────────────────────────────────────────────────────────────────────────

create table public.weather_grid_hourly (
  grid_lat       numeric(6,3) not null,
  grid_lon       numeric(7,3) not null,
  forecast_ts    timestamptz  not null,
  is_observed    boolean      not null default false,
  -- Weather fields (mirror Open-Meteo hourly schema)
  temp_air       numeric,     -- °F
  feels_like     numeric,     -- °F (apparent_temperature)
  wind_speed     numeric,     -- mph
  precip_chance  numeric,     -- 0-100%
  precip_mm      numeric,     -- mm (observed past or forecast future)
  weather_code   integer,     -- WMO code
  uv_index       numeric,
  cloud_cover    numeric,     -- 0-100%
  is_day         boolean,
  -- Provenance
  fetched_at     timestamptz  not null default now(),
  source         text         not null default 'open_meteo',
  primary key (grid_lat, grid_lon, forecast_ts),
  foreign key (grid_lat, grid_lon) references public.weather_grid (grid_lat, grid_lon) on delete cascade
);

create index weather_grid_hourly_ts_idx on public.weather_grid_hourly (forecast_ts desc);
create index weather_grid_hourly_fetched_idx on public.weather_grid_hourly (fetched_at desc);

comment on table public.weather_grid_hourly is
  '168-hour rolling forecast + past-3-days observed weather per grid cell. is_observed=true for past hours (bacteria-grade), false for forecast. Consumers join via weather_for_point(). See [[weather-grid-reference-layer]].';
comment on column public.weather_grid_hourly.is_observed is
  'true for past hours from latest fetch (use for bacteria-risk, dog-park mud); false for forecast (use for scoring).';

-- ───────────────────────────────────────────────────────────────────────────
-- 3. RLS — anon read access (matches scoring tables)
-- ───────────────────────────────────────────────────────────────────────────

alter table public.weather_grid         enable row level security;
alter table public.weather_grid_hourly  enable row level security;

create policy weather_grid_anon_select         on public.weather_grid         for select to anon, authenticated using (true);
create policy weather_grid_hourly_anon_select  on public.weather_grid_hourly  for select to anon, authenticated using (true);

commit;
