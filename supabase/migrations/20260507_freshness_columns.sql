-- 20260507_freshness_columns.sql
--
-- Per-row freshness tracking for daily-beach-refresh skip logic.
--
-- Two timestamps on beach_day_hourly_scores:
--   * tide_refreshed_at    — when NOAA tide for this row was last fetched
--   * weather_refreshed_at — when Open-Meteo for this row was last fetched
--
-- The edge function uses these to skip API calls when the data is fresh:
--   * Tide: skip NOAA fetch if all 7 forecast days are < 7 days fresh
--   * Weather days 5-7: skip Open-Meteo for those days if < 48h fresh
--
-- Pre-existing rows get NULL — the edge function treats NULL as
-- "needs refresh", so the next run does the full fetch and starts the
-- freshness clock.

begin;

alter table public.beach_day_hourly_scores
  add column if not exists tide_refreshed_at    timestamptz,
  add column if not exists weather_refreshed_at timestamptz;

create index if not exists hourly_tide_refreshed_idx
  on public.beach_day_hourly_scores (arena_group_id, tide_refreshed_at)
  where tide_refreshed_at is not null;

create index if not exists hourly_weather_refreshed_idx
  on public.beach_day_hourly_scores (arena_group_id, weather_refreshed_at)
  where weather_refreshed_at is not null;

commit;
