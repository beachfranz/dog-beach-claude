-- 20260525_weather_grid_helpers.sql
--
-- W1.2 of weather-grid-reference-layer. Helper functions + derived view:
--   weather_grid_bin_lat(double) / weather_grid_bin_lon(double)
--     — compute the SW corner of the 0.025° cell containing a point.
--       Used by triggers (W1.3) and consumers.
--   weather_for_point(lat, lng, start_ts, end_ts)
--     — returns hourly weather rows for the cell containing a point,
--       within a time window. Consumers' single read entry point.
--   precipitation_history view
--     — daily SUM(precip_mm) grouped by cell × date, from observed past.
--       Replaces the inline 72h computation in daily-beach-refresh.

begin;

-- ───────────────────────────────────────────────────────────────────────────
-- Bin functions — floor-to-0.025° (used by triggers + lookups)
-- ───────────────────────────────────────────────────────────────────────────

create or replace function public.weather_grid_bin_lat(p_lat double precision)
returns numeric
language sql immutable
as $$
  select (floor(p_lat / 0.025) * 0.025)::numeric(6,3)
$$;

create or replace function public.weather_grid_bin_lon(p_lng double precision)
returns numeric
language sql immutable
as $$
  select (floor(p_lng / 0.025) * 0.025)::numeric(7,3)
$$;

comment on function public.weather_grid_bin_lat is
  'Floor-bin a latitude to its 0.025° cell SW corner. Per [[weather-grid-reference-layer]].';
comment on function public.weather_grid_bin_lon is
  'Floor-bin a longitude to its 0.025° cell SW corner.';

-- ───────────────────────────────────────────────────────────────────────────
-- weather_for_point — primary consumer read entry point
-- ───────────────────────────────────────────────────────────────────────────

create or replace function public.weather_for_point(
  p_lat       double precision,
  p_lng       double precision,
  p_start_ts  timestamptz,
  p_end_ts    timestamptz
)
returns setof public.weather_grid_hourly
language sql stable
as $$
  select *
  from public.weather_grid_hourly
  where grid_lat = public.weather_grid_bin_lat(p_lat)
    and grid_lon = public.weather_grid_bin_lon(p_lng)
    and forecast_ts between p_start_ts and p_end_ts
  order by forecast_ts;
$$;

comment on function public.weather_for_point is
  'Returns hourly weather rows for the 0.025° cell containing (p_lat, p_lng) within a time window. Primary consumer read entry point. See [[weather-grid-reference-layer]].';

grant execute on function public.weather_grid_bin_lat   to anon, authenticated;
grant execute on function public.weather_grid_bin_lon   to anon, authenticated;
grant execute on function public.weather_for_point      to anon, authenticated;

-- ───────────────────────────────────────────────────────────────────────────
-- precipitation_history — derived view (no separate table needed)
-- ───────────────────────────────────────────────────────────────────────────
--
-- Replaces the per-beach inline 72h computation in daily-beach-refresh
-- (computePrecipForDay). Bacteria-risk reads this; dog-park mud caution
-- (Phase W2) reads this.

create or replace view public.precipitation_history as
select
  grid_lat,
  grid_lon,
  (forecast_ts at time zone 'America/Los_Angeles')::date  as observation_date,
  sum(precip_mm)                                          as precip_mm
from public.weather_grid_hourly
where is_observed = true
  and precip_mm is not null
group by grid_lat, grid_lon, (forecast_ts at time zone 'America/Los_Angeles')::date;

comment on view public.precipitation_history is
  'Daily precipitation per grid cell, summed from observed past hours in weather_grid_hourly. Replaces inline 72h precip computation in daily-beach-refresh. Drives bacteria-risk + dog-park mud caution.';

-- ───────────────────────────────────────────────────────────────────────────
-- precip_72h_for_point — convenience for bacteria-risk + mud caution
-- ───────────────────────────────────────────────────────────────────────────

create or replace function public.precip_72h_for_point(
  p_lat         double precision,
  p_lng         double precision,
  p_anchor_date date
)
returns numeric
language sql stable
as $$
  select coalesce(sum(precip_mm), 0)::numeric
  from public.precipitation_history
  where grid_lat = public.weather_grid_bin_lat(p_lat)
    and grid_lon = public.weather_grid_bin_lon(p_lng)
    and observation_date between p_anchor_date - 3 and p_anchor_date - 1;
$$;

comment on function public.precip_72h_for_point is
  'Returns 72h precipitation total ending at start of p_anchor_date for the cell containing (p_lat, p_lng). Used by bacteria-risk + dog-park mud caution. See [[weather-grid-reference-layer]].';

grant execute on function public.precip_72h_for_point to anon, authenticated;

commit;
