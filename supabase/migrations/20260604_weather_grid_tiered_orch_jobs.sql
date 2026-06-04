-- Switch weather-grid orchestration to the tiered model.
--
-- Old: one orch_jobs row `weather_grid_hourly` firing refresh-weather-grid
--      hourly with picker tier params (hot/warm/cold). Picker has been
--      timing out at ~14s on the LATERAL aggregate over 1.9M rows.
--
-- New: three orch_jobs rows, one per forecast-horizon tier. Each fires the
-- same refresh-weather-grid edge function with {tier: ...}, picker now scans
-- weather_grid directly on the tier's last_fetched_tX column (sub-100ms).
--
--   weather_grid_t1   every 5 min   forecast 0-48h, hourly target
--   weather_grid_t2   every 30 min  forecast 48-96h, 6h target
--   weather_grid_t3   hourly         forecast 96-168h + past_days=3, 12h target
--
-- Each invocation processes limit=600 cells (fits Open-Meteo's 600/min cell
-- budget per minute window). At those cadences and that batch size, each
-- tier completes one full sweep of all 4,350 cells well within its target
-- refresh cadence.

BEGIN;

-- ── 1. Worker functions: fire the edge function via net.http_post ──
-- Same shape as _fire_daily_beach_refresh (publishable + admin-secret).
-- Body sets tier + limit; cron handles cadence + slice progression
-- (picker draws stalest 600 cells per fire, bumper marks them fresh).

CREATE OR REPLACE FUNCTION public._orch_w_weather_grid_t1(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE request_id bigint;
BEGIN
  SELECT net.http_post(
    url := 'https://ehlzbwtrsxaaukurekau.supabase.co/functions/v1/refresh-weather-grid',
    headers := jsonb_build_object(
      'Content-Type',   'application/json',
      'Authorization',  'Bearer sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'apikey',         'sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'x-admin-secret', '8JMgk1BEGN0FiEJiivP9_34w-dTJzKTu'
    ),
    body := jsonb_build_object('tier', 't1', 'limit', 600) || COALESCE(p, '{}'::jsonb),
    timeout_milliseconds := 150000
  ) INTO request_id;
  RETURN request_id;
END;
$$;

CREATE OR REPLACE FUNCTION public._orch_w_weather_grid_t2(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE request_id bigint;
BEGIN
  SELECT net.http_post(
    url := 'https://ehlzbwtrsxaaukurekau.supabase.co/functions/v1/refresh-weather-grid',
    headers := jsonb_build_object(
      'Content-Type',   'application/json',
      'Authorization',  'Bearer sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'apikey',         'sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'x-admin-secret', '8JMgk1BEGN0FiEJiivP9_34w-dTJzKTu'
    ),
    body := jsonb_build_object('tier', 't2', 'limit', 600) || COALESCE(p, '{}'::jsonb),
    timeout_milliseconds := 150000
  ) INTO request_id;
  RETURN request_id;
END;
$$;

CREATE OR REPLACE FUNCTION public._orch_w_weather_grid_t3(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE request_id bigint;
BEGIN
  SELECT net.http_post(
    url := 'https://ehlzbwtrsxaaukurekau.supabase.co/functions/v1/refresh-weather-grid',
    headers := jsonb_build_object(
      'Content-Type',   'application/json',
      'Authorization',  'Bearer sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'apikey',         'sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'x-admin-secret', '8JMgk1BEGN0FiEJiivP9_34w-dTJzKTu'
    ),
    body := jsonb_build_object('tier', 't3', 'limit', 600) || COALESCE(p, '{}'::jsonb),
    timeout_milliseconds := 150000
  ) INTO request_id;
  RETURN request_id;
END;
$$;

-- ── 2. Disable the old cron ───────────────────────────────────────
UPDATE public.orch_jobs
   SET enabled    = FALSE,
       updated_at = now()
 WHERE job_name = 'weather_grid_hourly';

-- ── 3. Register the three tier jobs ───────────────────────────────
INSERT INTO public.orch_jobs (
  job_name, description, cadence_kind, cadence_param,
  worker_kind, worker_target, worker_payload,
  enabled, shadow_mode
) VALUES
  ('weather_grid_t1',
   'Tiered weather-grid refresh — next 48h forecast. Every 5 min, limit 600 cells. Picker reads weather_grid.last_fetched_t1.',
   'every_n_minutes', '5',
   'edge_function', '_orch_w_weather_grid_t1', '{}'::jsonb,
   TRUE, FALSE),
  ('weather_grid_t2',
   'Tiered weather-grid refresh — hours 48-96 of forecast. Every 30 min, limit 600 cells. Picker reads weather_grid.last_fetched_t2.',
   'every_n_minutes', '30',
   'edge_function', '_orch_w_weather_grid_t2', '{}'::jsonb,
   TRUE, FALSE),
  ('weather_grid_t3',
   'Tiered weather-grid refresh — hours 96-168 of forecast + past_days=3 observations. Hourly, limit 600 cells. Sole owner of past-observed hours in weather_grid_hourly. Picker reads weather_grid.last_fetched_t3.',
   'hourly_at', '00',
   'edge_function', '_orch_w_weather_grid_t3', '{}'::jsonb,
   TRUE, FALSE)
ON CONFLICT (job_name) DO UPDATE
  SET description    = EXCLUDED.description,
      cadence_kind   = EXCLUDED.cadence_kind,
      cadence_param  = EXCLUDED.cadence_param,
      worker_kind    = EXCLUDED.worker_kind,
      worker_target  = EXCLUDED.worker_target,
      worker_payload = EXCLUDED.worker_payload,
      enabled        = EXCLUDED.enabled,
      shadow_mode    = EXCLUDED.shadow_mode,
      updated_at     = now();

COMMIT;
