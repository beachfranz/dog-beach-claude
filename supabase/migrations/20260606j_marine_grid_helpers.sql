-- 20260606j_marine_grid_helpers.sql
--
-- M1.2 of marine-grid-reference-layer. Helper functions:
--   marine_grid_bin_lat(double) / marine_grid_bin_lon(double)
--     — compute SW corner of 0.1° cell containing a point.
--       Used by trigger (M1.3) and consumers.
--   marine_for_point(lat, lng, start_ts, end_ts)
--     — returns hourly marine rows for cell containing a point in window.
--       Single read entry point for refresh_marine_advisories + future
--       consumers (per-beach detail, find page surf chip, etc.).

begin;

-- ───────────────────────────────────────────────────────────────────────────
-- Bin functions — floor-to-0.1° (used by triggers + lookups)
-- ───────────────────────────────────────────────────────────────────────────

create or replace function public.marine_grid_bin_lat(p_lat double precision)
returns numeric
language sql immutable
as $$
  select (floor(p_lat / 0.1) * 0.1)::numeric(5,2)
$$;

create or replace function public.marine_grid_bin_lon(p_lng double precision)
returns numeric
language sql immutable
as $$
  select (floor(p_lng / 0.1) * 0.1)::numeric(6,2)
$$;

comment on function public.marine_grid_bin_lat is
  'Floor-bin a latitude to its 0.1° marine cell SW corner. Matches ECMWF marine model native resolution. Per [[marine-grid-reference-layer]].';
comment on function public.marine_grid_bin_lon is
  'Floor-bin a longitude to its 0.1° marine cell SW corner.';

-- ───────────────────────────────────────────────────────────────────────────
-- marine_for_point — primary consumer read entry point
-- ───────────────────────────────────────────────────────────────────────────

create or replace function public.marine_for_point(
  p_lat       double precision,
  p_lng       double precision,
  p_start_ts  timestamptz,
  p_end_ts    timestamptz
)
returns setof public.marine_grid_hourly
language sql stable
as $$
  select *
  from public.marine_grid_hourly
  where grid_lat = public.marine_grid_bin_lat(p_lat)
    and grid_lon = public.marine_grid_bin_lon(p_lng)
    and forecast_ts between p_start_ts and p_end_ts
  order by forecast_ts;
$$;

comment on function public.marine_for_point is
  'Returns hourly marine rows for the 0.1° cell containing (p_lat, p_lng) within a time window. Primary consumer read entry point. See [[marine-grid-reference-layer]].';

grant execute on function public.marine_grid_bin_lat   to anon, authenticated;
grant execute on function public.marine_grid_bin_lon   to anon, authenticated;
grant execute on function public.marine_for_point      to anon, authenticated;

commit;
