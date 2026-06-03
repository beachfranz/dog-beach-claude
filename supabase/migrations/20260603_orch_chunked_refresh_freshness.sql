-- Orch engine: content-freshness audit for chunked refresh jobs.
--
-- beach_refresh_chunked and dog_park_refresh_chunked are sql_function
-- workers that fire daily-beach-refresh / daily-dog-park-refresh edge
-- functions via net.http_post inside _fire_daily_*_refresh(). The
-- nested request_id isn't stored on orch_runs, so the response
-- reconciler (20260603_orch_reconcile_http_responses) can't see them
-- — a persistent 401 / 5xx from the edge worker stays silent at the
-- orch layer for these jobs specifically.
--
-- For chunked refreshes the right health signal is content freshness:
-- the chunked cron fires every 2 minutes with skip_recent_hours=22, so
-- max(beach_day_recommendations.generated_at) across scoreable beaches
-- should always be very recent (well within the last hour). If max
-- is more than 4 hours old, the chunked refresh is stuck — same
-- failure shape that took out daily-beach-refresh 2026-05-30 → 2026-06-03.
--
-- This migration:
--   1. _orch_w_chunked_refresh_freshness_audit(jsonb) — SQL worker that
--      checks both rec tables' max(generated_at) and demotes the parent
--      chunked job (last_succeeded_at + last_failed_at + last_error)
--      if stale. Otherwise no-op.
--   2. Registers it as orch_jobs row 'chunked_refresh_freshness_audit'
--      firing every 15 minutes.

BEGIN;

-- 1. Audit worker.
CREATE OR REPLACE FUNCTION public._orch_w_chunked_refresh_freshness_audit(p jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_threshold     interval := COALESCE((p->>'stale_after')::interval, interval '4 hours');
  v_beach_latest  timestamptz;
  v_dp_latest     timestamptz;
  v_beach_stale   boolean;
  v_dp_stale      boolean;
BEGIN
  -- Beach side.
  SELECT max(r.generated_at) INTO v_beach_latest
    FROM public.beach_day_recommendations r
    JOIN public.beaches_gold b ON b.fid = r.arena_group_id
   WHERE b.is_active AND b.scoring_tier IN ('hourly','daily');

  v_beach_stale := (v_beach_latest IS NULL)
                   OR (v_beach_latest < now() - v_threshold);

  IF v_beach_stale THEN
    UPDATE public.orch_jobs
       SET last_failed_at = now(),
           last_error = format(
             'content-freshness: beach_day_recommendations.generated_at max=%s (%s ago), threshold=%s — daily-beach-refresh edge fn is stuck. Likely deploy-without-no-verify-jwt or persistent 5xx. Check supabase logs.',
             COALESCE(v_beach_latest::text, 'NULL'),
             COALESCE((now() - v_beach_latest)::text, 'never'),
             v_threshold
           ),
           last_succeeded_at = (
             SELECT max(rr.started_at)
               FROM public.orch_runs rr
              WHERE rr.job_name = 'beach_refresh_chunked'
                AND rr.status = 'succeeded'
           ),
           updated_at = now()
     WHERE job_name = 'beach_refresh_chunked';
  ELSE
    -- Fresh content → ensure last_error is cleared if it was set by a
    -- prior stale audit; don't touch last_succeeded_at (orch_tick owns it).
    UPDATE public.orch_jobs
       SET last_error = NULL, updated_at = now()
     WHERE job_name = 'beach_refresh_chunked'
       AND last_error LIKE 'content-freshness:%';
  END IF;

  -- Dog park side.
  SELECT max(r.generated_at) INTO v_dp_latest
    FROM public.dog_park_day_recommendations r
    JOIN public.dog_parks_gold p ON p.fid = r.dog_park_fid
   WHERE p.is_active AND p.is_scoreable;

  v_dp_stale := (v_dp_latest IS NULL)
                OR (v_dp_latest < now() - v_threshold);

  IF v_dp_stale THEN
    UPDATE public.orch_jobs
       SET last_failed_at = now(),
           last_error = format(
             'content-freshness: dog_park_day_recommendations.generated_at max=%s (%s ago), threshold=%s — daily-dog-park-refresh edge fn is stuck.',
             COALESCE(v_dp_latest::text, 'NULL'),
             COALESCE((now() - v_dp_latest)::text, 'never'),
             v_threshold
           ),
           last_succeeded_at = (
             SELECT max(rr.started_at)
               FROM public.orch_runs rr
              WHERE rr.job_name = 'dog_park_refresh_chunked'
                AND rr.status = 'succeeded'
           ),
           updated_at = now()
     WHERE job_name = 'dog_park_refresh_chunked';
  ELSE
    UPDATE public.orch_jobs
       SET last_error = NULL, updated_at = now()
     WHERE job_name = 'dog_park_refresh_chunked'
       AND last_error LIKE 'content-freshness:%';
  END IF;
END;
$$;

-- 2. Register as orch job (every 15 min).
INSERT INTO public.orch_jobs (
  job_name, description, cadence_kind, cadence_param,
  worker_kind, worker_target, worker_payload,
  enabled, shadow_mode
) VALUES (
  'chunked_refresh_freshness_audit',
  'Demotes beach/dog-park chunked refresh jobs if their rec tables stop receiving fresh generated_at writes (covers the gap the http-response reconciler can''t see for sql_function workers that fan out to edge fns).',
  'every_n_minutes', '15',
  'sql_function', '_orch_w_chunked_refresh_freshness_audit', '{}'::jsonb,
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

-- 3. Run it once now to populate state immediately.
SELECT public._orch_w_chunked_refresh_freshness_audit('{}'::jsonb);

COMMIT;
