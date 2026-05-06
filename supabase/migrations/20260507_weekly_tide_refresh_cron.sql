-- 20260507_weekly_tide_refresh_cron.sql
--
-- Weekly bulk tide refresh: forces a 14-day NOAA fetch for every
-- scoreable beach. Sunday 08:00 UTC = Sunday 01:00 PT (one hour before
-- the daily refresh). After this fires, the daily cron's tide step
-- skips NOAA all week because stored tide_height covers the next 7
-- days at <=7d age.
--
-- Net effect: tide NOAA calls drop from ~4,400/week to ~627/week (86%
-- reduction). Zero accuracy cost — NOAA tide predictions are harmonic
-- functions of station constants; day-14 prediction is as accurate as
-- day-1.

begin;

create or replace function public._fire_weekly_tide_refresh()
returns bigint
language plpgsql
security definer
set search_path = public
as $$
declare
  request_id bigint;
begin
  select net.http_post(
    url     := 'https://ehlzbwtrsxaaukurekau.supabase.co/functions/v1/daily-beach-refresh',
    headers := jsonb_build_object(
      'Content-Type',   'application/json',
      'Authorization',  'Bearer sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'apikey',         'sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'x-admin-secret', '8JMgk1BEGN0FiEJiivP9_34w-dTJzKTu'
    ),
    body    := '{"tide_window_days": 14, "force_tide_refresh": true}'::jsonb,
    timeout_milliseconds := 1800000
  ) into request_id;
  return request_id;
end $$;

-- Schedule for Sunday 08:00 UTC (01:00 PT) — 1 hour before the daily
-- refresh at 09:00 UTC, so the buffer is fresh by the time the daily
-- run looks for it.
do $cron$
begin
  if exists (select 1 from cron.job where jobname = 'weekly_tide_refresh') then
    perform cron.unschedule('weekly_tide_refresh');
  end if;
  perform cron.schedule(
    'weekly_tide_refresh',
    '0 8 * * 0',
    $$ select public._fire_weekly_tide_refresh(); $$
  );
end $cron$;

commit;
