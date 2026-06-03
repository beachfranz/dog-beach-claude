-- Orch engine: fleet-wide NOAA tide station health audit.
--
-- beaches_gold.noaa_station_id is silently load-bearing — daily-beach-refresh
-- joins it to NOAA's CO-OPS API to fetch tide forecasts. When NOAA retires
-- or renumbers a station, the API 404s and the function writes neutral tide
-- scores (status='go') with no alarm. The beach scoring just quietly drops
-- the tide signal.
--
-- This migration:
--   1. _orch_w_noaa_station_health_audit(jsonb) — SQL worker that fires the
--      audit-noaa-stations edge function via net.http_post. Pattern mirrors
--      _fire_daily_beach_refresh (publishable + admin-secret headers).
--   2. Registers as orch_jobs row 'noaa_station_health_audit' running once
--      daily at 03:30 UTC (low traffic). NOT shadow_mode — we want it firing.
--
-- The response_status_code from the edge function lands in net._http_response;
-- the existing _orch_reconcile_responses pass (20260603_orch_reconcile_http_responses.sql)
-- will demote last_succeeded_at on non-2xx, and the JSON body (broken station
-- list) is preserved via response_body for inspection on orch_runs.
--
-- We deliberately do NOT run the audit at migration apply time — letting
-- orch_tick fire it naturally is the right thing here so the response lands
-- in net._http_response with a worker_request_id the reconciler can find.

BEGIN;

-- 1. Worker function. Same headers/URL shape as _fire_daily_beach_refresh.
CREATE OR REPLACE FUNCTION public._orch_w_noaa_station_health_audit(p jsonb)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  request_id bigint;
BEGIN
  SELECT net.http_post(
    url := 'https://ehlzbwtrsxaaukurekau.supabase.co/functions/v1/audit-noaa-stations',
    headers := jsonb_build_object(
      'Content-Type',   'application/json',
      'Authorization',  'Bearer sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'apikey',         'sb_publishable_lAg7YdZ3w7S5fN8jgiExKQ_3-KtW3xk',
      'x-admin-secret', '8JMgk1BEGN0FiEJiivP9_34w-dTJzKTu'
    ),
    body := '{}'::jsonb,
    timeout_milliseconds := 150000
  ) INTO request_id;
  RETURN request_id;
END;
$$;

-- 2. Register as orch job (once daily at 03:30 UTC, low-traffic window).
INSERT INTO public.orch_jobs (
  job_name, description, cadence_kind, cadence_param,
  worker_kind, worker_target, worker_payload,
  enabled, shadow_mode
) VALUES (
  'noaa_station_health_audit',
  'Probes every distinct beaches_gold.noaa_station_id against NOAA mdapi and reports broken stations + affected beaches. Surfaces silently-load-bearing NOAA station id rot — when a station retires/renumbers, daily-beach-refresh writes neutral tide scores with no alarm. Audit findings land in orch_runs.response_body via the reconciler.',
  'daily_at', '03:30',
  'sql_function', '_orch_w_noaa_station_health_audit', '{}'::jsonb,
  TRUE, FALSE
) ON CONFLICT (job_name) DO UPDATE
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
