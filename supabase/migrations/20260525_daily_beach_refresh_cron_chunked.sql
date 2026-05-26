-- 20260525_daily_beach_refresh_cron_chunked.sql
--
-- W2.1 production-stability mitigation. Per Franz directive 2026-05-25 LATE.
--
-- Replaces the once-nightly daily_beach_refresh_nightly cron with a
-- chunked-every-10-min cron pattern (mirrors what was applied to dog
-- parks earlier today). Each firing processes only beaches whose
-- updated_at is older than 22h. Resource budget naturally lands in
-- the 50-beach-per-fire window; coverage achieved over ~10-12 fires
-- (~100-120 min wall clock from cold start), then no-ops the rest
-- of the day until 22h elapse.
--
-- Why this is needed: daily-beach-refresh edge fn hits
-- WORKER_RESOURCE_LIMIT at ~57-90 beaches per call due to per-beach
-- DB I/O cost (DELETE 168 + UPSERT 168 + UPSERT 7). Even after W1
-- weather-grid cutover and tide-cache-only changes, per-beach cost
-- stayed at ~800ms. 100-beach single fire = ~80s = over the ~50s
-- budget.
--
-- True architectural fix is Phase W4 (port to Dagster Python — see
-- [[weather-grid-reference-layer]] task #29). This migration is the
-- bridge until that lands.
--
-- IMPORTANT: weekly_tide_refresh cron stays untouched. That fires
-- with force_tide_refresh=true on Sundays at 08:00 UTC, which is
-- the ONLY path that actually calls NOAA per the cache-only daily
-- refresh logic (see W2.1).

begin;

-- Wrapper passes skip_recent_hours so each fire skips beaches that
-- already refreshed within the cutoff window.
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
    body    := '{"skip_recent_hours": 22}'::jsonb,
    timeout_milliseconds := 600000
  ) into request_id;
  return request_id;
end $$;

-- Reschedule from 09:00 UTC daily → every 10 min
do $cron$
begin
  if exists (select 1 from cron.job where jobname = 'daily_beach_refresh_nightly') then
    perform cron.unschedule('daily_beach_refresh_nightly');
  end if;
  if exists (select 1 from cron.job where jobname = 'beach_refresh_chunked') then
    perform cron.unschedule('beach_refresh_chunked');
  end if;
  perform cron.schedule(
    'beach_refresh_chunked',
    '*/10 * * * *',
    $$ select public._fire_daily_beach_refresh(); $$
  );
end $cron$;

commit;
