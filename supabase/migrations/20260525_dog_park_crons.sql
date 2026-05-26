-- 20260525_dog_park_crons.sql
--
-- Two pg_cron entries for the dog-park sub-pipeline:
--   1. daily_dog_park_refresh_nightly — 09:05 UTC daily (5 min after beach
--      daily). POSTs to daily-dog-park-refresh. Admin-gated (x-admin-secret).
--   2. hourly_dog_park_now_refresh   — every hour at :05. POSTs to
--      get-dog-park-now with no body → all CA active+scoreable parks.
--
-- 5-min offset from beach crons (09:00 daily, hourly :00) to spread load.
--
-- Per [[dog-park-subpipeline-scope]] is_scoreable decision: fan all 351 CA
-- active dog parks. Cost is trivial (~351 × Open-Meteo calls/day).
--
-- Secret handling: same pattern as beach daily refresh. Stored inside a
-- SECURITY DEFINER wrapper so the secret doesn't leak via cron.job listings.
-- Per [[besttime-soft-removed]], hourly NOW does NOT need a separate flag
-- — get-dog-park-now never calls BestTime.
--
-- IMPORTANT: rotate ADMIN_SECRET + re-deploy this migration if the repo
-- flips public (currently public per CLAUDE.md; this was true when the
-- beach cron migration was written too — both expose the same secret).

begin;

-- ── 1. Daily refresh wrapper + schedule ─────────────────────────────────────

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
    body    := '{}'::jsonb,
    timeout_milliseconds := 600000
  ) into request_id;
  return request_id;
end $$;

revoke all on function public._fire_daily_dog_park_refresh() from public, anon, authenticated;
grant  execute on function public._fire_daily_dog_park_refresh() to service_role;

do $cron$
begin
  if exists (select 1 from cron.job where jobname = 'daily_dog_park_refresh_nightly') then
    perform cron.unschedule('daily_dog_park_refresh_nightly');
  end if;
  perform cron.schedule(
    'daily_dog_park_refresh_nightly',
    '5 9 * * *',
    $$ select public._fire_daily_dog_park_refresh(); $$
  );
end $cron$;

-- ── 2. Hourly NOW refresh schedule ──────────────────────────────────────────
-- get-dog-park-now is anon-key only (no admin gate) — same pattern as
-- get-beach-now. Inline net.http_post is fine; no wrapper needed.

do $cron$
begin
  if exists (select 1 from cron.job where jobname = 'hourly_dog_park_now_refresh') then
    perform cron.unschedule('hourly_dog_park_now_refresh');
  end if;
  perform cron.schedule(
    'hourly_dog_park_now_refresh',
    '5 * * * *',
    $$
    select net.http_post(
      url     := 'https://ehlzbwtrsxaaukurekau.supabase.co/functions/v1/get-dog-park-now',
      headers := jsonb_build_object(
        'Content-Type',  'application/json',
        'Authorization', 'Bearer sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk'
      ),
      body    := '{}'::jsonb
    ) as request_id
    $$
  );
end $cron$;

commit;
