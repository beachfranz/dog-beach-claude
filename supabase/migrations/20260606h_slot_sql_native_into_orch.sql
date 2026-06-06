-- 2026-06-06 — Slot the three SQL-native refresh shapes into orch_jobs.
--
-- Replaces existing per-fid-loop wrappers with thin wrappers that call
-- the bulk SQL functions, flips advisory cadence daily→hourly (now that
-- SQL is 30-60s instead of 1h+), and adds new orch_jobs for marine
-- advisories + bulk best-window apply.
--
-- Three phase revisions slotted:
--   1. refresh_beach_advisories     ← _orch_w_compute_weather_advisories
--      refresh_dog_park_advisories  ← _orch_w_compute_dp_advisories
--   2. apply_v2_best_window_to_*_bulk ← new wrappers + new jobs
--   3. refresh_marine_advisories      ← new wrapper + new job
--
-- After this migration:
--   - daily_weather_advisories       → hourly_weather_advisories (cadence flip)
--   - daily_dog_park_advisories      → hourly_dog_park_advisories (cadence flip)
--   - hourly_marine_advisories       (NEW)
--   - apply_v2_best_window_beach     (NEW, every 5 min after beach_refresh_chunked)
--   - apply_v2_best_window_dog_park  (NEW, every 5 min after dog_park_refresh_chunked)
--
-- NOT IN THIS PR (open for future):
--   - daily-beach-refresh + daily-dog-park-refresh edge functions still
--     do per-fid apply_v2_best_window_to_*_recommendations as part of their
--     per-beach work. That's now redundant with the new orch job that does
--     it set-based. Removing the per-fid call requires an edge fn change +
--     redeploy. Safe to leave for now (the bulk call just overwrites with
--     identical values), but should be cleaned up.
--   - marine_forecast LOADER scheduling. fetch_marine_forecast.py is still
--     a Python script with no schedule. Marine advisory job will silently
--     produce no rows on stale upstream — orch_deps_health_audit catches
--     dead deps, but we don't have a staleness audit on
--     beach_marine_forecast.fetched_at yet.

-- ─────────────────────────────────────────────────────────────────────────
-- 1. Replace existing wrapper bodies with thin SQL-fn callers
-- ─────────────────────────────────────────────────────────────────────────

-- Beach weather advisories — was per-fid loop calling _orch_compute_weather_advisories_one
CREATE OR REPLACE FUNCTION public._orch_w_compute_weather_advisories(p jsonb)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  v_state    text;
  v_only_fid bigint;
  v_upserted bigint;
  v_retired  bigint;
BEGIN
  v_state    := p->>'state';
  v_only_fid := nullif(p->>'only_fid', '')::bigint;

  SELECT upserted, retired INTO v_upserted, v_retired
    FROM public.refresh_beach_advisories(v_state, v_only_fid);

  RAISE NOTICE 'beach_advisories: state=% fid=% upserted=% retired=%',
    coalesce(v_state, 'ALL'), coalesce(v_only_fid::text, 'ALL'),
    v_upserted, v_retired;
END;
$$;

-- Dog-park advisories — was per-fid loop calling _orch_compute_dp_advisories_one
CREATE OR REPLACE FUNCTION public._orch_w_compute_dp_advisories(p jsonb)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  v_state    text;
  v_only_fid bigint;
  v_upserted bigint;
  v_retired  bigint;
BEGIN
  v_state    := p->>'state';
  v_only_fid := nullif(p->>'only_fid', '')::bigint;

  SELECT upserted, retired INTO v_upserted, v_retired
    FROM public.refresh_dog_park_advisories(v_state, v_only_fid);

  RAISE NOTICE 'dp_advisories: state=% fid=% upserted=% retired=%',
    coalesce(v_state, 'ALL'), coalesce(v_only_fid::text, 'ALL'),
    v_upserted, v_retired;
END;
$$;

-- ─────────────────────────────────────────────────────────────────────────
-- 2. New wrappers for marine + bulk apply
-- ─────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public._orch_w_refresh_marine_advisories(p jsonb)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  v_state       text;
  v_only_fid    bigint;
  v_horizon_h   int;
  v_upserted    bigint;
  v_retired     bigint;
  v_forecast_max timestamptz;
BEGIN
  v_state     := p->>'state';
  v_only_fid  := nullif(p->>'only_fid', '')::bigint;
  v_horizon_h := coalesce(nullif(p->>'horizon_hours', '')::int, 48);

  -- Pre-flight staleness check on upstream beach_marine_forecast.
  -- Marine fn will silently produce zero rows if upstream isn't being
  -- refreshed. Raise a warning so the orch run shows up in audit.
  SELECT max(fetched_at) INTO v_forecast_max FROM public.beach_marine_forecast;
  IF v_forecast_max IS NULL OR v_forecast_max < now() - interval '6 hours' THEN
    RAISE WARNING 'marine_advisories: upstream beach_marine_forecast stale '
                  '(last fetched %); function will produce 0 rows. '
                  'Schedule the marine loader (fetch_marine_forecast.py).',
                  v_forecast_max;
  END IF;

  SELECT upserted, retired INTO v_upserted, v_retired
    FROM public.refresh_marine_advisories(v_state, v_only_fid, v_horizon_h);

  RAISE NOTICE 'marine_advisories: state=% fid=% horizon=%h upserted=% retired=%',
    coalesce(v_state, 'ALL'), coalesce(v_only_fid::text, 'ALL'),
    v_horizon_h, v_upserted, v_retired;
END;
$$;

CREATE OR REPLACE FUNCTION public._orch_w_apply_v2_best_window_beach(p jsonb)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  v_state     text;
  v_days_ahead int;
  v_beaches_processed bigint;
  v_pairs     bigint;
  v_hour_writes bigint;
  v_recs      bigint;
  v_windows   bigint;
BEGIN
  v_state      := p->>'state';
  v_days_ahead := coalesce(nullif(p->>'days_ahead', '')::int, 6);

  SELECT beaches_processed, fid_date_pairs, hour_scores_written, recs_written, windows_picked
    INTO v_beaches_processed, v_pairs, v_hour_writes, v_recs, v_windows
    FROM public.apply_v2_best_window_to_beach_recommendations_bulk(
      v_state, NULL, NULL, v_days_ahead);

  RAISE NOTICE 'bulk_apply_beach: state=% days=% beaches=% pairs=% hours=% recs=% windows=%',
    coalesce(v_state, 'ALL'), v_days_ahead,
    v_beaches_processed, v_pairs, v_hour_writes, v_recs, v_windows;
END;
$$;

CREATE OR REPLACE FUNCTION public._orch_w_apply_v2_best_window_dog_park(p jsonb)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  v_state     text;
  v_days_ahead int;
  v_parks_processed bigint;
  v_pairs     bigint;
  v_hour_writes bigint;
  v_recs      bigint;
  v_windows   bigint;
BEGIN
  v_state      := p->>'state';
  v_days_ahead := coalesce(nullif(p->>'days_ahead', '')::int, 6);

  SELECT parks_processed, fid_date_pairs, hour_scores_written, recs_written, windows_picked
    INTO v_parks_processed, v_pairs, v_hour_writes, v_recs, v_windows
    FROM public.apply_v2_best_window_to_recommendations_bulk(
      v_state, NULL, NULL, v_days_ahead);

  RAISE NOTICE 'bulk_apply_dp: state=% days=% parks=% pairs=% hours=% recs=% windows=%',
    coalesce(v_state, 'ALL'), v_days_ahead,
    v_parks_processed, v_pairs, v_hour_writes, v_recs, v_windows;
END;
$$;

-- ─────────────────────────────────────────────────────────────────────────
-- 3. Update existing orch_jobs cadences + insert new jobs
-- ─────────────────────────────────────────────────────────────────────────

-- Flip advisory jobs daily → hourly (SQL fns are 30-60s, can run every hour).
-- Schedule them 10 min past the hour so they land after the every-2-min
-- chunked refresh has settled. depends_on still chains to chunked refresh.
UPDATE public.orch_jobs
   SET cadence_kind  = 'hourly_at',
       cadence_param = '10',
       description   = 'Hourly: refresh beach_advisory deterministic_weather + deterministic_bacteria rows via refresh_beach_advisories() SQL fn'
 WHERE job_name = 'daily_weather_advisories';

UPDATE public.orch_jobs
   SET cadence_kind  = 'hourly_at',
       cadence_param = '12',
       description   = 'Hourly: refresh dog_park_advisory rows via refresh_dog_park_advisories() SQL fn'
 WHERE job_name = 'daily_dog_park_advisories';

-- Insert new marine advisory job. Same payload shape; daily → hourly aware.
INSERT INTO public.orch_jobs (
  job_name, description, cadence_kind, cadence_param, cron_expr,
  depends_on, depends_max_age, cost_check_fn, skip_if_succeeded_within,
  worker_kind, worker_target, worker_payload, scope_states,
  enabled, shadow_mode
) VALUES (
  'hourly_marine_advisories',
  'Hourly: refresh beach_advisory marine_threshold rows via refresh_marine_advisories() SQL fn. NOTE upstream beach_marine_forecast loader (fetch_marine_forecast.py) not yet scheduled.',
  'hourly_at',
  '14',  -- 14 minutes past, after beach_refresh and weather/dp advisory jobs
  NULL,
  ARRAY[]::text[],   -- intentionally no deps; marine_forecast scheduling is a separate workstream
  NULL,
  NULL,
  NULL,
  'sql_function',
  'public._orch_w_refresh_marine_advisories',
  '{}'::jsonb,
  NULL,
  TRUE,
  FALSE
)
ON CONFLICT (job_name) DO UPDATE SET
  description   = EXCLUDED.description,
  cadence_kind  = EXCLUDED.cadence_kind,
  cadence_param = EXCLUDED.cadence_param,
  worker_kind   = EXCLUDED.worker_kind,
  worker_target = EXCLUDED.worker_target,
  enabled       = EXCLUDED.enabled;

-- Bulk best-window apply for beaches. Run every 5 min so the daily refresh
-- chunked path is followed up quickly with set-based rollup. Per-state-fan-out
-- not used here — the bulk fn handles full scope in ~234s sequentially OR
-- ~61s for the slowest state when called per-state.
-- Cadence of 5 min matches beach_refresh_chunked which runs every 2 min;
-- gives the chunked path ~3 cycles between rollups.
INSERT INTO public.orch_jobs (
  job_name, description, cadence_kind, cadence_param, cron_expr,
  depends_on, depends_max_age, cost_check_fn, skip_if_succeeded_within,
  worker_kind, worker_target, worker_payload, scope_states,
  enabled, shadow_mode
) VALUES (
  'apply_v2_best_window_beach',
  'Every 5 min: rollup hour_score_v2 + composite_score_v2 + best_window for all scoreable beaches via apply_v2_best_window_to_beach_recommendations_bulk(). Replaces per-fid×date loop inside daily-beach-refresh edge fn (per-fid call still fires from that edge fn; safe — bulk overwrites with identical values; clean up later).',
  'every_n_minutes',
  '5',
  NULL,
  ARRAY['beach_refresh_chunked']::text[],
  NULL,
  NULL,
  NULL,
  'sql_function',
  'public._orch_w_apply_v2_best_window_beach',
  '{}'::jsonb,
  NULL,
  TRUE,
  FALSE
)
ON CONFLICT (job_name) DO UPDATE SET
  description   = EXCLUDED.description,
  cadence_kind  = EXCLUDED.cadence_kind,
  cadence_param = EXCLUDED.cadence_param,
  depends_on    = EXCLUDED.depends_on,
  worker_kind   = EXCLUDED.worker_kind,
  worker_target = EXCLUDED.worker_target,
  enabled       = EXCLUDED.enabled;

-- Bulk best-window apply for dog parks. Sibling. Depends on
-- dog_park_refresh_chunked.
INSERT INTO public.orch_jobs (
  job_name, description, cadence_kind, cadence_param, cron_expr,
  depends_on, depends_max_age, cost_check_fn, skip_if_succeeded_within,
  worker_kind, worker_target, worker_payload, scope_states,
  enabled, shadow_mode
) VALUES (
  'apply_v2_best_window_dog_park',
  'Every 5 min: rollup hour_score_v2 + composite_score_v2 + best_window for all scoreable dog parks via apply_v2_best_window_to_recommendations_bulk(). Sibling of apply_v2_best_window_beach.',
  'every_n_minutes',
  '5',
  NULL,
  ARRAY['dog_park_refresh_chunked']::text[],
  NULL,
  NULL,
  NULL,
  'sql_function',
  'public._orch_w_apply_v2_best_window_dog_park',
  '{}'::jsonb,
  NULL,
  TRUE,
  FALSE
)
ON CONFLICT (job_name) DO UPDATE SET
  description   = EXCLUDED.description,
  cadence_kind  = EXCLUDED.cadence_kind,
  cadence_param = EXCLUDED.cadence_param,
  depends_on    = EXCLUDED.depends_on,
  worker_kind   = EXCLUDED.worker_kind,
  worker_target = EXCLUDED.worker_target,
  enabled       = EXCLUDED.enabled;
