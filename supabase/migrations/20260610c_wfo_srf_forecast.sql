-- 20260610c_wfo_srf_forecast.sql
--
-- Reference table for the NWS Surf Zone Forecast (SRF) data path.
--
-- One row per (zone_id, forecast_date, period). The SRF is published per
-- coastal Weather Forecast Office (WFO); each contains forecast blocks for
-- 1..N coastal forecast zones. Each zone block carries a rip current risk
-- (Low / Moderate / High) for the "current" period (today/tomorrow) and the
-- "next" period (day after).
--
-- Consumers (find_beaches RPC) JOIN at read time: beach.coastal_zone_id →
-- wfo_srf_forecast WHERE forecast_date = p_date AND period = 'current'.
--
-- We do NOT extend public.beach_advisory because that table is keyed per
-- beach. SRF zones fan out to ~50 beaches each; storing per-beach would 50x
-- the row count for no read-side benefit. Reference table + JOIN is the
-- same shape as marine_grid_hourly + beaches_gold.marine_grid_lat/lon.

begin;

create table if not exists public.wfo_srf_forecast (
  zone_id           text        not null,
  wfo               text        not null,
  forecast_date     date        not null,
  period            text        not null check (period in ('current', 'next')),
  rip_current_risk  text                 check (rip_current_risk in ('Low', 'Moderate', 'High')),
  conditions        text,
  valid_from        timestamptz,
  valid_to          timestamptz,
  fetched_at        timestamptz not null default now(),
  raw               text,
  primary key (zone_id, forecast_date, period)
);

create index if not exists wfo_srf_forecast_date_idx
  on public.wfo_srf_forecast (forecast_date, zone_id);
create index if not exists wfo_srf_forecast_wfo_idx
  on public.wfo_srf_forecast (wfo, forecast_date);

comment on table public.wfo_srf_forecast is
  'Parsed NWS Surf Zone Forecast (SRF) per coastal forecast zone × date × period. '
  'Source: https://forecast.weather.gov/product.php?site=NWS&issuedby=<WFO>&product=SRF&format=txt. '
  'Refreshed daily by edge fn refresh-nws-srf. Consumers JOIN via beaches_gold.coastal_zone_id.';
comment on column public.wfo_srf_forecast.rip_current_risk is
  'NWS-published rip current risk: Low / Moderate / High. NULL when the period block omitted a "Rip Current Risk*" line (some WFOs only render risk on elevated days).';
comment on column public.wfo_srf_forecast.period is
  '"current" = today/tomorrow forecast block (first .PERIOD... section). "next" = day-after (second .PERIOD... section). Most SRFs publish two periods.';

-- ─── RLS ─────────────────────────────────────────────────────────────────
-- Anon read so find_beaches RPC (SECURITY DEFINER) doesn't even need it,
-- but mirroring marine_grid_hourly + weather_grid_hourly RLS for consistency.
alter table public.wfo_srf_forecast enable row level security;

drop policy if exists wfo_srf_forecast_read on public.wfo_srf_forecast;
create policy wfo_srf_forecast_read on public.wfo_srf_forecast
  for select to anon, authenticated
  using (true);

commit;
