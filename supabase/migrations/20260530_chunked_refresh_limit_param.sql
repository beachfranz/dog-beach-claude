-- 20260530_chunked_refresh_limit_param.sql
--
-- Adds `limit:40` to both chunked-refresh cron driver bodies
-- (_fire_daily_beach_refresh + _fire_daily_dog_park_refresh) so
-- each 2-min fire processes only the 40 oldest-stale entities
-- instead of attempting all eligible ones.
--
-- Why: daily-beach-refresh and daily-dog-park-refresh were both
-- hitting WORKER_RESOURCE_LIMIT (HTTP 546) on EVERY chunked fire.
-- The 22h skip_recent_hours filter was supposed to make the cron
-- self-chunking, but it only kicks in AFTER a successful refresh
-- marks beaches/parks recent. Since every invocation crashed
-- mid-load (2,922 stale beaches + ~600 stale parks loaded into
-- memory), nothing got marked recent — the next fire saw the same
-- stale pool — death loop.
--
-- The fix: edge functions now respect a body.limit param. After
-- the skip-recent filter, they sort by recommendation.updated_at
-- NULLS FIRST and slice to limit. At 40 entities/2min = 1,200/hr,
-- the beach backlog drains in ~2.5h; steady-state load after drain
-- is ~5/fire on average (well under the cap).
--
-- Companion edge-fn changes:
--   supabase/functions/daily-beach-refresh/index.ts
--   supabase/functions/daily-dog-park-refresh/index.ts

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
    body    := '{"skip_recent_hours": 22, "limit": 40}'::jsonb,
    timeout_milliseconds := 600000
  ) into request_id;
  return request_id;
end $$;

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
    body    := '{"skip_recent_hours": 22, "limit": 40}'::jsonb,
    timeout_milliseconds := 600000
  ) into request_id;
  return request_id;
end $$;

commit;
