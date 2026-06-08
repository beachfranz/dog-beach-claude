-- 20260607k_weekly_tide_refresh_uses_grid.sql
--
-- Rewire the weekly_tide_refresh orch job to fire the new
-- refresh-tide-stations edge fn (per 20260607j) instead of
-- daily-beach-refresh with force_tide_refresh=true.
--
-- BEFORE: _fire_weekly_tide_refresh POSTed daily-beach-refresh, which
-- looped 3,899 active scoring beaches sequentially in one edge-fn
-- invocation and died at 150s after ~11% coverage.
--
-- AFTER: fires refresh-tide-stations which loops 296 distinct stations
-- with bounded concurrency. ~28s for the full set in smoke test.
-- Consumers (daily-beach-refresh, now grid-backed) read from the cache.
--
-- The scope_states column on the orch job is now decorative — the
-- loader picker uses tide_station_inventory (auto-populated from
-- beaches_gold.noaa_station_id via trigger), so coverage automatically
-- tracks every state in the catalog including HI.

CREATE OR REPLACE FUNCTION public._fire_weekly_tide_refresh()
RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public'
AS $function$
DECLARE
  request_id bigint;
BEGIN
  SELECT net.http_post(
    url     := 'https://ehlzbwtrsxaaukurekau.supabase.co/functions/v1/refresh-tide-stations',
    headers := jsonb_build_object(
      'Content-Type',   'application/json',
      'Authorization',  'Bearer sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'apikey',         'sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'x-admin-secret', '8JMgk1BEGN0FiEJiivP9_34w-dTJzKTu'
    ),
    body    := '{"force_full": true, "limit": 500}'::jsonb,
    timeout_milliseconds := 300000   -- 5 min cap (was 30 min for the legacy path)
  ) INTO request_id;
  RETURN request_id;
END;
$function$;

-- Drop the now-meaningless scope_states constraint — loader scope
-- comes from tide_station_inventory (auto-tracks catalog). Update the
-- description to reflect new behavior.
UPDATE public.orch_jobs
   SET description = 'Refresh NOAA CO-OPS tide cache via refresh-tide-stations (per-station, all catalog states incl. HI). Weekly.',
       updated_at  = now()
 WHERE job_name = 'weekly_tide_refresh';
