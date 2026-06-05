-- Enhanced content-freshness audit: catch BOTH global stuck (existing)
-- AND long-tail stuck (new).
--
-- Today's blackout receipts:
--   - 12h dog-park stall: CAUGHT by existing max(gen) check
--   - 14d Luhr Jensen + 1,889-beach backlog: NOT CAUGHT — max(gen) was
--     current because the cron was processing 40 beaches per fire, just
--     never reaching the high-fid beaches outside PostgREST's 1000-row
--     window. The audit needs per-fid visibility, not just pool-max.
--
-- Enhancement: in addition to max(gen) < threshold (the "everything stuck"
-- signal), add per-fid staleness count and oldest-fid report (the "some
-- stuck for too long" signal). With cron firing every 2 min × limit=40 +
-- skip_recent_hours=22, every active beach/park should be touched within
-- ~25h in steady state. Per-fid threshold default = 30h (generous buffer).
--
-- Sink unchanged: orch_jobs.last_error string. Dashboard surface unchanged.
-- Per [[paired-functions-port-fixes-both-sides]]: both beach + dog-park
-- legs of the audit enhanced in lockstep.

CREATE OR REPLACE FUNCTION public._orch_w_chunked_refresh_freshness_audit(p jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
  v_pool_threshold     interval := COALESCE((p->>'stale_after')::interval, interval '4 hours');
  v_per_fid_threshold  interval := COALESCE((p->>'per_fid_stale_after')::interval, interval '30 hours');

  v_beach_max_gen      timestamptz;
  v_beach_oldest_gen   timestamptz;
  v_beach_total        int;
  v_beach_stale_count  int;
  v_beach_msg          text;

  v_dp_max_gen         timestamptz;
  v_dp_oldest_gen      timestamptz;
  v_dp_total           int;
  v_dp_stale_count     int;
  v_dp_msg             text;
BEGIN
  -- ── Beach side ────────────────────────────────────────────────────────
  WITH per_fid AS (
    SELECT b.fid, max(r.generated_at) AS last_gen
    FROM public.beaches_gold b
    LEFT JOIN public.beach_day_recommendations r ON r.arena_group_id = b.fid
    WHERE b.is_active AND b.scoring_tier IN ('hourly','daily')
    GROUP BY b.fid
  )
  SELECT
    max(last_gen),
    -- min() ignores NULLs; coalesce never-written to epoch so they show as oldest
    min(coalesce(last_gen, 'epoch'::timestamptz)),
    count(*),
    count(*) FILTER (
      WHERE last_gen IS NULL OR last_gen < now() - v_per_fid_threshold
    )
  INTO v_beach_max_gen, v_beach_oldest_gen, v_beach_total, v_beach_stale_count
  FROM per_fid;

  IF v_beach_max_gen IS NULL OR v_beach_max_gen < now() - v_pool_threshold THEN
    v_beach_msg := format(
      'content-freshness: beach_day_recommendations.generated_at max=%s (%s ago), threshold=%s — daily-beach-refresh edge fn is stuck. Likely deploy-without-no-verify-jwt or persistent 5xx. Check supabase logs.',
      COALESCE(v_beach_max_gen::text, 'NULL'),
      COALESCE((now() - v_beach_max_gen)::text, 'never'),
      v_pool_threshold
    );
  ELSIF v_beach_stale_count > 0 THEN
    v_beach_msg := format(
      'per-fid staleness: %s/%s beaches have generated_at > %s old; oldest=%s (%s ago). Cron is firing but draining incompletely — likely PostgREST cap, broken filter, or processBeach error on specific fids. Run: SELECT * FROM beaches_due_for_refresh(NULL,NULL,NULL,5) to see the 5 oldest.',
      v_beach_stale_count, v_beach_total, v_per_fid_threshold,
      v_beach_oldest_gen::text,
      (now() - v_beach_oldest_gen)::text
    );
  ELSE
    v_beach_msg := NULL;
  END IF;

  IF v_beach_msg IS NOT NULL THEN
    UPDATE public.orch_jobs
       SET last_failed_at = now(),
           last_error = v_beach_msg,
           last_succeeded_at = (
             SELECT max(rr.started_at) FROM public.orch_runs rr
              WHERE rr.job_name = 'beach_refresh_chunked' AND rr.status = 'succeeded'
           ),
           updated_at = now()
     WHERE job_name = 'beach_refresh_chunked';
  ELSE
    UPDATE public.orch_jobs
       SET last_error = NULL, updated_at = now()
     WHERE job_name = 'beach_refresh_chunked'
       AND (last_error LIKE 'content-freshness:%' OR last_error LIKE 'per-fid staleness:%');
  END IF;

  -- ── Dog park side (mirror of beach side) ──────────────────────────────
  -- Audit scope MUST match refresh function scope (per [[audit-for-ca-only-defaults]]
  -- + [[paired-functions-port-fixes-both-sides]]): daily-dog-park-refresh
  -- hardcodes state IN ('CA','OR','WA','MD','UT'); audit mirrors that.
  -- Real fix is lifting the hardcode — until then, scope must align or
  -- the audit reports phantom staleness on non-MVP+ states that the cron
  -- has no intent to process.
  WITH per_fid AS (
    SELECT dpg.fid, max(r.generated_at) AS last_gen
    FROM public.dog_parks_gold dpg
    LEFT JOIN public.dog_park_day_recommendations r ON r.dog_park_fid = dpg.fid
    WHERE dpg.is_active AND dpg.is_scoreable
      AND dpg.state IN ('CA','OR','WA','MD','UT')
    GROUP BY dpg.fid
  )
  SELECT
    max(last_gen),
    min(coalesce(last_gen, 'epoch'::timestamptz)),
    count(*),
    count(*) FILTER (
      WHERE last_gen IS NULL OR last_gen < now() - v_per_fid_threshold
    )
  INTO v_dp_max_gen, v_dp_oldest_gen, v_dp_total, v_dp_stale_count
  FROM per_fid;

  IF v_dp_max_gen IS NULL OR v_dp_max_gen < now() - v_pool_threshold THEN
    v_dp_msg := format(
      'content-freshness: dog_park_day_recommendations.generated_at max=%s (%s ago), threshold=%s — daily-dog-park-refresh edge fn is stuck.',
      COALESCE(v_dp_max_gen::text, 'NULL'),
      COALESCE((now() - v_dp_max_gen)::text, 'never'),
      v_pool_threshold
    );
  ELSIF v_dp_stale_count > 0 THEN
    v_dp_msg := format(
      'per-fid staleness: %s/%s dog parks have generated_at > %s old; oldest=%s (%s ago). Cron is firing but draining incompletely. Run: SELECT * FROM dog_parks_due_for_refresh(NULL,NULL,NULL,5) to see the 5 oldest.',
      v_dp_stale_count, v_dp_total, v_per_fid_threshold,
      v_dp_oldest_gen::text,
      (now() - v_dp_oldest_gen)::text
    );
  ELSE
    v_dp_msg := NULL;
  END IF;

  IF v_dp_msg IS NOT NULL THEN
    UPDATE public.orch_jobs
       SET last_failed_at = now(),
           last_error = v_dp_msg,
           last_succeeded_at = (
             SELECT max(rr.started_at) FROM public.orch_runs rr
              WHERE rr.job_name = 'dog_park_refresh_chunked' AND rr.status = 'succeeded'
           ),
           updated_at = now()
     WHERE job_name = 'dog_park_refresh_chunked';
  ELSE
    UPDATE public.orch_jobs
       SET last_error = NULL, updated_at = now()
     WHERE job_name = 'dog_park_refresh_chunked'
       AND (last_error LIKE 'content-freshness:%' OR last_error LIKE 'per-fid staleness:%');
  END IF;
END;
$function$;
