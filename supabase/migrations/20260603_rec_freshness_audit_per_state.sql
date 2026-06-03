-- Per-state generated_at freshness audit.
--
-- The earlier _orch_w_chunked_refresh_freshness_audit (shipped today)
-- checks max(generated_at) GLOBALLY across all active scoreable beaches /
-- dog parks. That catches "the cron is totally stuck" but not "the cron
-- is firing fine but state X is silently drained slower than the skip
-- window" — which is exactly the HB Dog Beach / 600-stale-beaches shape
-- found today.
--
-- This audit goes per-state:
--   - For each state with ≥1 active scoreable entity, compute count of
--     entities with generated_at fresher than the threshold (default 26h).
--   - If any state has <100% fresh, fail with a per-state breakdown so
--     cron-audit shows the affected states explicitly.
--
-- Top 8 stale states surfaced in last_error; full breakdown via the
-- existing _orch_w_rec_freshness_audit_per_state(jsonb) function call.
-- Cadence every 30 minutes.

BEGIN;

CREATE OR REPLACE FUNCTION public._orch_w_rec_freshness_audit_per_state(p jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_threshold       interval := COALESCE((p->>'stale_after')::interval, interval '26 hours');
  v_beach_findings  text;
  v_dp_findings     text;
  v_beach_stale_n   int;
  v_dp_stale_n      int;
  v_err             text;
BEGIN
  -- Beach side: per-state generated_at coverage.
  WITH per_state AS (
    SELECT b.state,
           count(DISTINCT b.fid) AS total_scoreable,
           count(DISTINCT r.arena_group_id) FILTER (
             WHERE r.generated_at > now() - v_threshold
           ) AS fresh_count,
           max(r.generated_at) AS latest_gen
      FROM public.beaches_gold b
      LEFT JOIN public.beach_day_recommendations r ON r.arena_group_id = b.fid
     WHERE b.is_active AND b.scoring_tier IN ('hourly','daily')
     GROUP BY b.state
  ),
  stale AS (
    SELECT * FROM per_state WHERE fresh_count < total_scoreable
  )
  SELECT count(*),
         string_agg(
           format('%s: %s/%s fresh (max %s)',
                  state,
                  fresh_count, total_scoreable,
                  COALESCE(to_char(latest_gen at time zone 'UTC','YYYY-MM-DD HH24:MI'), 'never')),
           '; '
           ORDER BY (total_scoreable - fresh_count) DESC)
    INTO v_beach_stale_n, v_beach_findings
    FROM (SELECT * FROM stale ORDER BY (total_scoreable - fresh_count) DESC LIMIT 8) s;

  -- DP side: same shape against dog_park_day_recommendations.
  WITH per_state AS (
    SELECT p.state,
           count(DISTINCT p.fid) AS total_scoreable,
           count(DISTINCT r.dog_park_fid) FILTER (
             WHERE r.generated_at > now() - v_threshold
           ) AS fresh_count,
           max(r.generated_at) AS latest_gen
      FROM public.dog_parks_gold p
      LEFT JOIN public.dog_park_day_recommendations r ON r.dog_park_fid = p.fid
     WHERE p.is_active AND p.is_scoreable
     GROUP BY p.state
  ),
  stale AS (
    SELECT * FROM per_state WHERE fresh_count < total_scoreable
  )
  SELECT count(*),
         string_agg(
           format('%s: %s/%s fresh (max %s)',
                  state,
                  fresh_count, total_scoreable,
                  COALESCE(to_char(latest_gen at time zone 'UTC','YYYY-MM-DD HH24:MI'), 'never')),
           '; '
           ORDER BY (total_scoreable - fresh_count) DESC)
    INTO v_dp_stale_n, v_dp_findings
    FROM (SELECT * FROM stale ORDER BY (total_scoreable - fresh_count) DESC LIMIT 8) s;

  -- Build error string + update the audit job's own state.
  IF v_beach_stale_n > 0 OR v_dp_stale_n > 0 THEN
    v_err := format(
      'rec generated_at staleness (threshold %s): beach states=%s [%s]; DP states=%s [%s]',
      v_threshold,
      COALESCE(v_beach_stale_n, 0), COALESCE(v_beach_findings, '—'),
      COALESCE(v_dp_stale_n, 0),    COALESCE(v_dp_findings, '—')
    );
    UPDATE public.orch_jobs
       SET last_failed_at = now(),
           last_error = v_err,
           updated_at = now()
     WHERE job_name = 'rec_freshness_audit_per_state';
  ELSE
    UPDATE public.orch_jobs
       SET last_error = NULL,
           updated_at = now()
     WHERE job_name = 'rec_freshness_audit_per_state'
       AND last_error LIKE 'rec generated_at staleness%';
  END IF;
END;
$$;

-- Register as orch job (every 30 min).
INSERT INTO public.orch_jobs (
  job_name, description, cadence_kind, cadence_param,
  worker_kind, worker_target, worker_payload,
  enabled, shadow_mode
) VALUES (
  'rec_freshness_audit_per_state',
  'Per-state max(generated_at) audit on beach_day_recommendations + dog_park_day_recommendations. Catches drainage gaps that the global chunked_refresh_freshness_audit can''t see — e.g. one state drained slower than the skip window so rec rows stay stale forever.',
  'every_n_minutes', '30',
  'sql_function', '_orch_w_rec_freshness_audit_per_state', '{}'::jsonb,
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

-- Run once now to populate state immediately.
SELECT public._orch_w_rec_freshness_audit_per_state('{}'::jsonb);

COMMIT;
