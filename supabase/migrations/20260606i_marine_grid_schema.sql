-- 20260606i_marine_grid_schema.sql
--
-- M1.1 of marine-grid-reference-layer (sibling of weather-grid-reference-layer
-- [[weather-grid-reference-layer]]; pin to be created [[marine-grid-reference-layer]]).
-- Two new tables mirroring the weather_grid pattern at coarser resolution:
--
--   marine_grid           — materialized inventory of active marine cells
--                            (cells with ≥1 coastal beach). Auto-populated
--                            by trigger on beaches_gold.
--   marine_grid_hourly    — 168-hour rolling marine forecast per cell.
--                            Consumers (refresh_marine_advisories) JOIN here.
--
-- Grid resolution: 0.1° (~11km) — matches ECMWF marine model's native
-- resolution. Fetching at finer than 0.1° gets the same interpolated value
-- from Open-Meteo (wasted API calls). 64% fewer fetches than per-beach.
--
-- Floor-bin semantics: cell (X, Y) covers [X, X+0.1) × [Y, Y+0.1).
--
-- Scope: ONLY coastal beaches need marine data (no dog parks). So unlike
-- weather_grid this table only gets a trigger on beaches_gold.
--
-- Loader: edge function `refresh-marine-grid` (M1.5). Fired by orch_jobs
-- same way `refresh-weather-grid` is fired by weather_grid_t1/t2/t3.

begin;

-- ───────────────────────────────────────────────────────────────────────────
-- 1. marine_grid — materialized active-cell inventory
-- ───────────────────────────────────────────────────────────────────────────

create table public.marine_grid (
  grid_lat   numeric(5,2) not null,    -- 0.1° resolution
  grid_lon   numeric(6,2) not null,
  geom       geography(point, 4326),
  state      text,                     -- audit / partitioning hint
  first_seen timestamptz not null default now(),
  primary key (grid_lat, grid_lon)
);

create index marine_grid_geom_gix on public.marine_grid using gist (geom);
create index marine_grid_state_idx on public.marine_grid (state);

comment on table public.marine_grid is
  'Materialized inventory of active marine grid cells. Auto-populated by trigger on beaches_gold. One row per (grid_lat, grid_lon) tuple where ≥1 coastal beach exists. Loader reads this to know what cells to refresh. See [[marine-grid-reference-layer]].';
comment on column public.marine_grid.grid_lat is
  '0.1° floor-bin latitude. Matches ECMWF marine model native resolution; fetching finer is wasted API calls.';
comment on column public.marine_grid.grid_lon is
  '0.1° floor-bin longitude.';
comment on column public.marine_grid.geom is
  'Geography POINT at the cell''s SW corner. For PIP / KNN spatial queries.';

-- ───────────────────────────────────────────────────────────────────────────
-- 2. marine_grid_hourly — the actual time-series reference layer
-- ───────────────────────────────────────────────────────────────────────────

create table public.marine_grid_hourly (
  grid_lat                    numeric(5,2) not null,
  grid_lon                    numeric(6,2) not null,
  forecast_ts                 timestamptz  not null,
  -- Wave fields (Open-Meteo Marine variables)
  wave_height_m               numeric,
  wave_period_s               numeric,
  wave_direction_deg          numeric,
  swell_wave_height_m         numeric,
  swell_wave_period_s         numeric,
  swell_wave_direction_deg    numeric,
  wind_wave_height_m          numeric,
  wind_wave_period_s          numeric,
  wind_wave_direction_deg     numeric,
  -- Sea surface
  sst_c                       numeric,
  ocean_current_velocity_ms   numeric,
  ocean_current_direction_deg numeric,
  -- Derived field (computed at load time):
  -- swell_power = 0.49 * height² * period (kJ/m of wavefront)
  -- Better safety predictor than wave_height alone.
  swell_power_kj_m            numeric,
  -- Provenance
  fetched_at                  timestamptz  not null default now(),
  source                      text         not null default 'open_meteo_marine',
  primary key (grid_lat, grid_lon, forecast_ts),
  foreign key (grid_lat, grid_lon) references public.marine_grid (grid_lat, grid_lon) on delete cascade
);

create index marine_grid_hourly_ts_idx       on public.marine_grid_hourly (forecast_ts desc);
create index marine_grid_hourly_fetched_idx  on public.marine_grid_hourly (fetched_at desc);

comment on table public.marine_grid_hourly is
  '168-hour rolling marine forecast per grid cell. PK (grid_lat, grid_lon, forecast_ts). Marine data has no observed past from Open-Meteo (unlike weather_grid_hourly which has is_observed). Consumers join via marine_for_point(). See [[marine-grid-reference-layer]].';
comment on column public.marine_grid_hourly.swell_power_kj_m is
  'Computed at load: 0.49 * swell_wave_height² * swell_period (kJ/m of wavefront). Better predictor of "is this safe for dog play" than wave_height alone — captures the punch of long-period swells at any height.';

-- ───────────────────────────────────────────────────────────────────────────
-- 3. RLS — anon read access (matches scoring + weather_grid tables)
-- ───────────────────────────────────────────────────────────────────────────

alter table public.marine_grid         enable row level security;
alter table public.marine_grid_hourly  enable row level security;

create policy marine_grid_anon_select         on public.marine_grid         for select to anon, authenticated using (true);
create policy marine_grid_hourly_anon_select  on public.marine_grid_hourly  for select to anon, authenticated using (true);

commit;
