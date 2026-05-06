-- 20260506_daily_beach_refresh_cron.sql
--
-- Schedules daily-beach-refresh edge function via pg_cron. CLAUDE.md
-- claimed it was wired via pg_cron but cron.job had no entry — it's
-- been running manually only. Wiring properly here.
--
-- Schedule: 09:00 UTC = 02:00 PT = post-midnight PST. Runs after
-- verdict_cascade_nightly (03:00 UTC). Open-Meteo + NOAA fetches +
-- BestTime crowd data for ~627 active scoreable beaches; takes ~5-10
-- minutes wall-clock total via the edge function's internal fan-out.
--
-- Secret handling: the edge function requires x-admin-secret header.
-- Storing the secret inside a SECURITY DEFINER wrapper function so it
-- doesn't leak via cron.job listings (which authenticated read access
-- via cron extension). Function granted only to service_role.
--
-- ⚠ The secret is in the migration FILE. Repo is private as of
-- 2026-05-04. If/when the repo flips back public, rotate ADMIN_SECRET
-- and re-deploy this migration with the new value.

begin;

create or replace function public._fire_daily_beach_refresh()
returns bigint
language plpgsql
security definer
as $$
declare
  request_id bigint;
begin
  select net.http_post(
    url     := 'https://ehlzbwtrsxaaukurekau.supabase.co/functions/v1/daily-beach-refresh',
    headers := jsonb_build_object(
      'Content-Type',  'application/json',
      'Authorization', 'Bearer sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'apikey',        'sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'x-admin-secret', '8JMgk1BEGN0FiEJiivP9_34w-dTJzKTu'
    ),
    body    := '{}'::jsonb,
    timeout_milliseconds := 600000
  ) into request_id;
  return request_id;
end $$;

revoke all on function public._fire_daily_beach_refresh() from public, anon, authenticated;
grant  execute on function public._fire_daily_beach_refresh() to service_role;

-- Schedule: 09:00 UTC daily. Idempotent.
do $cron$
begin
  if exists (select 1 from cron.job where jobname = 'daily_beach_refresh_nightly') then
    perform cron.unschedule('daily_beach_refresh_nightly');
  end if;
  perform cron.schedule(
    'daily_beach_refresh_nightly',
    '0 9 * * *',
    $$ select public._fire_daily_beach_refresh(); $$
  );
end $cron$;

commit;
