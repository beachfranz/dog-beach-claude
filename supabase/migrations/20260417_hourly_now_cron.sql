-- ============================================================
-- Hourly cron job: refresh NOW conditions for all beaches.
-- Fires every hour on the hour via pg_cron + pg_net.
-- Calls get-beach-now (POST, no body = all active beaches).
-- ============================================================

SELECT cron.schedule(
  'hourly-beach-now-refresh',
  '0 * * * *',
  $$
  SELECT net.http_post(
    url     := 'https://ehlzbwtrsxaaukurekau.supabase.co/functions/v1/get-beach-now',
    headers := jsonb_build_object(
      'Content-Type',  'application/json',
      'Authorization', 'Bearer sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk'
    ),
    body    := '{}'::jsonb
  ) AS request_id
  $$
);
