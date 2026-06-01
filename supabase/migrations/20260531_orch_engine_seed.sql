-- 20260531_orch_engine_seed.sql
--
-- Seed orch_jobs with all 18 existing schedules (pg_cron + Dagster).
-- ALL jobs are seeded with shadow_mode=TRUE by default so the catalog
-- doesn't accidentally double-fire alongside the existing pg_cron + local
-- Dagster setup. Franz toggles shadow_mode=FALSE per job after verifying
-- the orchestrator decisions look right in orch_runs.
--
-- Worker conventions:
--   edge_function : worker_target is the edge fn name; net.http_post directly
--   sql_function  : worker_target is a qualified SQL fn taking ONE jsonb arg
--   noop          : placeholder for Dagster jobs that need Python ports
--
-- Wrappers below adapt the existing no-arg / typed-arg SQL functions to
-- the (jsonb) signature so they can serve as worker_target.

BEGIN;

-- ─── SQL function wrappers (legacy fn → jsonb signature) ──────────────

CREATE OR REPLACE FUNCTION public._orch_w_beach_refresh_chunked(p jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN PERFORM public._fire_daily_beach_refresh(); END $$;

CREATE OR REPLACE FUNCTION public._orch_w_dog_park_refresh_chunked(p jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN PERFORM public._fire_daily_dog_park_refresh(); END $$;

CREATE OR REPLACE FUNCTION public._orch_w_refresh_scoring_tier(p jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN PERFORM public.refresh_scoring_tier(); END $$;

CREATE OR REPLACE FUNCTION public._orch_w_run_pipeline_maintenance(p jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE v_state TEXT;
BEGIN
  v_state := COALESCE(p->>'state', 'CA');
  PERFORM public.run_full_pipeline_maintenance(v_state);
END $$;

CREATE OR REPLACE FUNCTION public._orch_w_process_geom_change_queue(p jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE v_batch INT;
BEGIN
  v_batch := COALESCE((p->>'batch')::INT, 50);
  PERFORM public.process_geom_change_queue(v_batch);
END $$;

CREATE OR REPLACE FUNCTION public._orch_w_verdict_cascade_nightly(p jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN PERFORM public.recompute_all_dogs_verdicts_by_origin(); END $$;

CREATE OR REPLACE FUNCTION public._orch_w_weekly_tide_refresh(p jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN PERFORM public._fire_weekly_tide_refresh(); END $$;

-- ─── Seed catalog ─────────────────────────────────────────────────────

INSERT INTO public.orch_jobs (
  job_name, description,
  cadence_kind, cadence_param, cron_expr,
  depends_on, depends_max_age,
  cost_check_fn, skip_if_succeeded_within,
  worker_kind, worker_target, worker_payload,
  scope_states, enabled, shadow_mode
) VALUES

-- ════════ Tier 1: Reference data freshness ════════

('weather_grid_inventory',
  'Rebuild weather_grid cell inventory from active entity tables (daily backstop).',
  'daily_at', '04:00', NULL,
  ARRAY[]::TEXT[], '36 hours',
  NULL, NULL,
  'noop', 'dagster:weather_grid_inventory', '{}'::jsonb,
  ARRAY['CA','OR','WA','MD','UT'], TRUE, TRUE),

('weather_grid_hourly',
  'Refresh weather_grid_hourly for cells with active consumers (hourly).',
  'hourly_at', '0', NULL,
  ARRAY['weather_grid_inventory']::TEXT[], '36 hours',
  NULL, NULL,
  'noop', 'dagster:weather_grid_hourly', '{}'::jsonb,
  ARRAY['CA','OR','WA','MD','UT'], TRUE, TRUE),

-- ════════ Tier 2: Per-entity scoring refresh ════════

('beach_refresh_chunked',
  'Chunked daily beach forecast refresh. Fires every 2 min, edge fn does 22h stale filter + 40-beach cap to fit Edge worker limits.',
  'every_n_minutes', '2', NULL,
  ARRAY['weather_grid_hourly']::TEXT[], '36 hours',
  NULL, NULL,
  'sql_function', 'public._orch_w_beach_refresh_chunked', '{}'::jsonb,
  ARRAY['CA','OR','WA','MD','UT'], TRUE, TRUE),

('dog_park_refresh_chunked',
  'Chunked daily dog-park forecast refresh. Same pattern as beach.',
  'every_n_minutes', '2', NULL,
  ARRAY['weather_grid_hourly']::TEXT[], '36 hours',
  NULL, NULL,
  'sql_function', 'public._orch_w_dog_park_refresh_chunked', '{}'::jsonb,
  ARRAY['CA','OR','WA','MD','UT'], TRUE, TRUE),

('hourly_beach_now_refresh',
  'Hourly NOW update for beaches (overwrites the is_now=true row with live actuals).',
  'hourly_at', '0', NULL,
  ARRAY[]::TEXT[], '36 hours',
  NULL, NULL,
  'edge_function', 'get-beach-now', '{"skip_recent_hours": 0.9}'::jsonb,
  ARRAY['CA','OR','WA','MD','UT'], TRUE, TRUE),

('hourly_dog_park_now_refresh',
  'Hourly NOW update for dog parks (offset by 5 min to spread load).',
  'hourly_at', '5', NULL,
  ARRAY[]::TEXT[], '36 hours',
  NULL, NULL,
  'edge_function', 'get-dog-park-now', '{}'::jsonb,
  ARRAY['CA','OR','WA','MD','UT'], TRUE, TRUE),

-- ════════ Tier 3: Per-entity advisories (depend on scoring) ════════

('daily_weather_advisories',
  'Compute beach_advisory rows from precip-derived bacteria_risk + heat/cold etc.',
  'daily_at', '12:00', NULL,
  ARRAY['beach_refresh_chunked']::TEXT[], '36 hours',
  NULL, NULL,
  'noop', 'dagster:weather_advisories_refresh', '{}'::jsonb,
  ARRAY['CA','OR','WA','MD','UT'], TRUE, TRUE),

('daily_dog_park_advisories',
  'Compute dog_park_advisory rows (mud caution from precip, surface-temp etc.).',
  'daily_at', '12:00', NULL,
  ARRAY['dog_park_refresh_chunked']::TEXT[], '36 hours',
  NULL, NULL,
  'noop', 'dagster:dog_park_advisories', '{}'::jsonb,
  ARRAY['CA','OR','WA','MD','UT'], TRUE, TRUE),

-- ════════ Tier 4: Daily housekeeping ════════

('daily_refresh_scoring_tier',
  'Recompute beaches_gold.scoring_tier from beach_dog_policy state (daily).',
  'daily_at', '07:00', NULL,
  ARRAY[]::TEXT[], '36 hours',
  NULL, NULL,
  'sql_function', 'public._orch_w_refresh_scoring_tier', '{}'::jsonb,
  ARRAY['CA','OR','WA','MD','UT'], TRUE, TRUE),

('verdict_cascade_nightly',
  'Recompute beach_verdicts from beach_locations + osm_features + CCC points.',
  'daily_at', '03:00', NULL,
  ARRAY[]::TEXT[], '36 hours',
  NULL, NULL,
  'sql_function', 'public._orch_w_verdict_cascade_nightly', '{}'::jsonb,
  ARRAY['CA','OR','WA','MD','UT'], TRUE, TRUE),

('nightly_pipeline_maintenance',
  'State-scoped pipeline maintenance (was CA-only; orch makes the state explicit).',
  'daily_at', '04:00', NULL,
  ARRAY[]::TEXT[], '36 hours',
  NULL, NULL,
  'sql_function', 'public._orch_w_run_pipeline_maintenance', '{"state":"CA"}'::jsonb,
  ARRAY['CA'], TRUE, TRUE),

('process_geom_change_queue',
  'Process pending geometry change events (PIP recomputes etc.).',
  'every_n_minutes', '5', NULL,
  ARRAY[]::TEXT[], '36 hours',
  NULL, NULL,
  'sql_function', 'public._orch_w_process_geom_change_queue', '{"batch":50}'::jsonb,
  ARRAY['CA','OR','WA','MD','UT'], TRUE, TRUE),

('weekly_tide_refresh',
  'Refresh NOAA tide cache for all stations (weekly).',
  'weekly_on_at', 'sun:08:00', NULL,
  ARRAY[]::TEXT[], '8 days',
  NULL, NULL,
  'sql_function', 'public._orch_w_weekly_tide_refresh', '{}'::jsonb,
  ARRAY['CA','OR','WA','MD','UT'], TRUE, TRUE),

-- ════════ Tier 5: Codify health (Dagster originals) ════════

('daily_codify_coverage_audit',
  'Per-state sand-bps coverage audit. Raises on regression below 80%.',
  'daily_at', '12:30', NULL,
  ARRAY['beach_refresh_chunked']::TEXT[], '36 hours',
  NULL, NULL,
  'noop', 'dagster:codify_coverage_audit', '{}'::jsonb,
  ARRAY['CA','OR','WA','MD','UT'], TRUE, TRUE),

('weekly_codify_gap_clone',
  'Clone operative sand-bps to gap beaches that share a city or CDPR unit.',
  'weekly_on_at', 'mon:13:00', NULL,
  ARRAY['daily_codify_coverage_audit']::TEXT[], '8 days',
  NULL, NULL,
  'noop', 'dagster:codify_gap_clone', '{}'::jsonb,
  ARRAY['CA','OR','WA','MD','UT'], TRUE, TRUE),

('weekly_dog_park_data_quality_audit',
  'DP catalog data quality + drift detection (weekly).',
  'weekly_on_at', 'sun:14:00', NULL,
  ARRAY[]::TEXT[], '8 days',
  NULL, NULL,
  'noop', 'dagster:dog_park_data_quality_audit', '{}'::jsonb,
  ARRAY['CA','OR','WA','MD','UT'], TRUE, TRUE),

('weekly_pipeline_health',
  'Broad pipeline drift + freshness checks across all assets.',
  'weekly_on_at', 'sun:15:00', NULL,
  ARRAY[]::TEXT[], '8 days',
  NULL, NULL,
  'noop', 'dagster:pipeline_health_audit', '{}'::jsonb,
  ARRAY['CA','OR','WA','MD','UT'], TRUE, TRUE),

('monthly_dog_park_coverage',
  'Full DP coverage refresh (operator-posted policy walker, slow).',
  'monthly_on_at', '1:08:00', NULL,
  ARRAY[]::TEXT[], '32 days',
  NULL, NULL,
  'noop', 'dagster:dog_park_coverage', '{}'::jsonb,
  ARRAY['CA','OR','WA','MD','UT'], TRUE, TRUE);

COMMIT;
