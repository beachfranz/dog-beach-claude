-- Schedule the precipitation_history rollup every 6 hours.
--
-- Worker is the SQL function from 20260604_precipitation_history_rollup_maintenance.sql:
--   _refresh_precipitation_history_from_grid()
-- which aggregates weather_grid_hourly observed precip into precipitation_history
-- for the trailing 3 PT-local calendar days + today and drops anything older.
--
-- Cadence: every 6 hours. Matches refresh-weather-grid's ~hourly cadence with
-- enough lag to ride out transient gaps but well within the staleness budget
-- for bacteria_risk (computed at most once daily per beach via daily-beach-refresh)
-- and mud_caution (daily per dog park).

BEGIN;

CREATE OR REPLACE FUNCTION public._orch_w_precipitation_history_rollup(p jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  PERFORM public._refresh_precipitation_history_from_grid();
END;
$$;

INSERT INTO public.orch_jobs (
  job_name, description, cadence_kind, cadence_param,
  worker_kind, worker_target, worker_payload,
  enabled, shadow_mode
) VALUES (
  'precipitation_history_rollup',
  'Aggregates weather_grid_hourly observed precip into precipitation_history (3-day rolling window, PT-local daily buckets). Drops rows outside the window. Backs precip_72h_for_point() RPC for bacteria_risk (beaches) + mud_caution (dirt-surface dog parks).',
  'every_n_minutes', '360',
  'sql_function', '_orch_w_precipitation_history_rollup', '{}'::jsonb,
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
