-- 20260610e_srf_orch_job.sql
--
-- Schedule the daily NWS SRF refresh via orch_jobs.
--
-- Cadence: daily_at 14:00 UTC. Covers all coastal WFOs once each:
--   - East coast (KOKX, KPHI, ...): morning issuance ~6-10am ET → 10-14 UTC
--   - West coast (KLOX, KMTR, ...): morning issuance ~6-10am PT → 13-17 UTC
--   - Hawaii (PHFO):                morning issuance ~6-10am HT → 16-20 UTC
-- 14:00 UTC catches mainland CONUS reliably; HI may see day-old data until
-- next cycle, acceptable for v1.
--
-- Edge function refresh-nws-srf: fetches ~30 SRF text URLs, parses zone
-- blocks, lazily warms nws_zones, UPSERTs into wfo_srf_forecast.
--
-- Mirrors 20260606n_marine_grid_orch_job.sql shape.

insert into public.orch_jobs (
  job_name, description, cadence_kind, cadence_param, cron_expr,
  depends_on, depends_max_age, cost_check_fn, skip_if_succeeded_within,
  worker_kind, worker_target, worker_payload, scope_states,
  enabled, shadow_mode
) values (
  'refresh_nws_srf',
  'Daily 14:00 UTC: fetch ~30 NWS coastal WFO Surf Zone Forecast text products, parse Rip Current Risk per coastal forecast zone, UPSERT into wfo_srf_forecast. Lazily warms nws_zones for newly-encountered zone IDs.',
  'daily_at',
  '14:00',
  null,
  array[]::text[],
  null,
  null,
  null,
  'edge_function',
  'refresh-nws-srf',
  '{}'::jsonb,
  null,
  true,
  false
)
on conflict (job_name) do update set
  description    = excluded.description,
  cadence_kind   = excluded.cadence_kind,
  cadence_param  = excluded.cadence_param,
  worker_kind    = excluded.worker_kind,
  worker_target  = excluded.worker_target,
  worker_payload = excluded.worker_payload,
  enabled        = excluded.enabled;
