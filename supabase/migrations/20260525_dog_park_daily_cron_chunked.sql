-- 20260525_dog_park_daily_cron_chunked.sql
--
-- Fix the daily dog-park refresh cron to handle WORKER_RESOURCE_LIMIT.
--
-- Discovered 2026-05-25 LATE: daily-dog-park-refresh edge function dies
-- ~92 parks into a 351-park run (Supabase compute budget). Per
-- [[chunked-subprocess]], chunking should've been baked into the edge
-- function at design time (task #13 tracks the proper fix).
--
-- Workaround until task #13 ships: fire the cron every 10 min with
-- skip_recent_hours=22 (slightly less than 24h to avoid race). Each
-- firing processes parks not refreshed in the last 22h. ~90 parks per
-- firing × 4 firings = full 351-park coverage in ~40 min, then it
-- no-ops for the rest of the day.
--
-- Cost trade-off: 144 firings/day × ~92 parks of processing × ~$0 each.
-- Open-Meteo is free; only DB I/O costs (negligible).

begin;

-- Update the wrapper to pass skip_recent_hours
create or replace function public._fire_daily_dog_park_refresh()
returns bigint
language plpgsql
security definer
as $$
declare
  request_id bigint;
begin
  select net.http_post(
    url     := 'https://ehlzbwtrsxaaukurekau.supabase.co/functions/v1/daily-dog-park-refresh',
    headers := jsonb_build_object(
      'Content-Type',  'application/json',
      'Authorization', 'Bearer sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'apikey',        'sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'x-admin-secret', '8JMgk1BEGN0FiEJiivP9_34w-dTJzKTu'
    ),
    body    := '{"skip_recent_hours": 22}'::jsonb,
    timeout_milliseconds := 600000
  ) into request_id;
  return request_id;
end $$;

-- Reschedule: every 10 min, no fixed daily slot
do $cron$
begin
  if exists (select 1 from cron.job where jobname = 'daily_dog_park_refresh_nightly') then
    perform cron.unschedule('daily_dog_park_refresh_nightly');
  end if;
  if exists (select 1 from cron.job where jobname = 'dog_park_refresh_chunked') then
    perform cron.unschedule('dog_park_refresh_chunked');
  end if;
  perform cron.schedule(
    'dog_park_refresh_chunked',
    '*/10 * * * *',
    $$ select public._fire_daily_dog_park_refresh(); $$
  );
end $cron$;

commit;
