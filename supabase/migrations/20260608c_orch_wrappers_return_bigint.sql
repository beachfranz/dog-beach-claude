-- 20260608c — Option A part 1: convert all SQL-fn orch wrappers to RETURNS bigint
--
-- WHY: `_orch_dispatch`'s sql_function branch currently does
--   EXECUTE 'SELECT ' || worker_target || '($1)' USING worker_payload;
-- — discarding the return value. Wrappers that fire `net.http_post` (the
-- chunked refreshers, the now-retired weekly tide wrapper) lose their
-- request_id, so `_orch_reconcile_responses` can never join to
-- `net._http_response` and surface 4xx/5xx failures.
--
-- This migration converts every void-returning wrapper to RETURNS bigint:
--   * Pure-SQL wrappers return NULL (no HTTP request to surface)
--   * HTTP-firing wrappers (`_orch_w_beach_refresh_chunked`,
--     `_orch_w_dog_park_refresh_chunked`) replace `PERFORM _fire_X()`
--     with `RETURN _fire_X()` — the helpers already return bigint
--
-- `_orch_w_noaa_station_health_audit` already returns bigint (untouched).
-- `_orch_w_weekly_tide_refresh` was dropped by 20260608b.
-- `_orch_work_pending(boolean)` is not a wrapper (different family).
--
-- The dispatcher patch lives in the next migration (20260608d). This one
-- must land FIRST so the dispatcher always sees bigint when it flips.
--
-- SECURITY DEFINER preserved per-wrapper. Wrapper bodies preserved
-- exactly; only signature change + final RETURN NULL added.
--
-- Companion follow-up tracked at [[orch-sql-fn-workers-blind-to-http-responses]].

-- ─── HTTP-firing wrappers (Tier D) ───────────────────────────────────

DROP FUNCTION IF EXISTS public._orch_w_beach_refresh_chunked(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_beach_refresh_chunked(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN RETURN public._fire_daily_beach_refresh(); END $$;

DROP FUNCTION IF EXISTS public._orch_w_dog_park_refresh_chunked(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_dog_park_refresh_chunked(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN RETURN public._fire_daily_dog_park_refresh(); END $$;

-- ─── Simple pure-SQL wrappers (Tier B — one-liner PERFORMs) ──────────

DROP FUNCTION IF EXISTS public._orch_w_refresh_scoring_tier(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_refresh_scoring_tier(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN PERFORM public.refresh_scoring_tier(); RETURN NULL; END $$;

DROP FUNCTION IF EXISTS public._orch_w_verdict_cascade_nightly(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_verdict_cascade_nightly(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN PERFORM public.recompute_all_dogs_verdicts_by_origin(); RETURN NULL; END $$;

DROP FUNCTION IF EXISTS public._orch_w_precipitation_history_rollup(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_precipitation_history_rollup(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN PERFORM public._refresh_precipitation_history_from_grid(); RETURN NULL; END $$;

DROP FUNCTION IF EXISTS public._orch_w_process_geom_change_queue(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_process_geom_change_queue(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE v_batch INT;
BEGIN
  v_batch := COALESCE((p->>'batch')::INT, 50);
  PERFORM public.process_geom_change_queue(v_batch);
  RETURN NULL;
END $$;

DROP FUNCTION IF EXISTS public._orch_w_dog_park_data_quality_audit(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_dog_park_data_quality_audit(p jsonb)
RETURNS bigint LANGUAGE plpgsql AS $$
BEGIN
  RAISE NOTICE 'dog_park_data_quality_audit: placeholder (no active checks). '
               'Original lighting+dawn-dusk check removed 2026-06-07 — '
               'legitimate combo (lights extend short winter dawn-dusk play).';
  RETURN NULL;
END $$;

-- ─── Tier B/C pure-SQL wrappers — compute / refresh callers ──────────

DROP FUNCTION IF EXISTS public._orch_w_compute_weather_advisories(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_compute_weather_advisories(p jsonb)
RETURNS bigint LANGUAGE plpgsql AS $$
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
  RETURN NULL;
END $$;

DROP FUNCTION IF EXISTS public._orch_w_compute_dp_advisories(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_compute_dp_advisories(p jsonb)
RETURNS bigint LANGUAGE plpgsql AS $$
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
  RETURN NULL;
END $$;

DROP FUNCTION IF EXISTS public._orch_w_refresh_marine_advisories(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_refresh_marine_advisories(p jsonb)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
  v_state       text;
  v_only_fid    bigint;
  v_horizon_h   int;
  v_upserted    bigint;
  v_retired     bigint;
  v_grid_max    timestamptz;
BEGIN
  v_state     := p->>'state';
  v_only_fid  := nullif(p->>'only_fid', '')::bigint;
  v_horizon_h := coalesce(nullif(p->>'horizon_hours', '')::int, 48);

  SELECT max(fetched_at) INTO v_grid_max FROM public.marine_grid_hourly;
  IF v_grid_max IS NULL OR v_grid_max < now() - interval '6 hours' THEN
    RAISE WARNING 'marine_advisories: upstream marine_grid_hourly stale '
                  '(last fetched %); function will produce 0 rows. '
                  'Schedule the marine_grid loader (refresh-marine-grid edge fn).',
                  v_grid_max;
  END IF;

  SELECT upserted, retired INTO v_upserted, v_retired
    FROM public.refresh_marine_advisories(v_state, v_only_fid, v_horizon_h);

  RAISE NOTICE 'marine_advisories: state=% fid=% horizon=%h upserted=% retired=%',
    coalesce(v_state, 'ALL'), coalesce(v_only_fid::text, 'ALL'),
    v_horizon_h, v_upserted, v_retired;
  RETURN NULL;
END $$;

DROP FUNCTION IF EXISTS public._orch_w_refresh_beach_hourly_scores_bulk(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_refresh_beach_hourly_scores_bulk(p jsonb)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
  v_state             text;
  v_days_ahead        int;
  v_beaches           bigint;
  v_hours             bigint;
  v_with_tide         bigint;
  v_with_weather      bigint;
BEGIN
  v_state      := p->>'state';
  v_days_ahead := COALESCE(NULLIF(p->>'days_ahead', '')::int, 6);

  SELECT beaches_processed, hours_written, hours_with_tide, hours_with_weather
    INTO v_beaches, v_hours, v_with_tide, v_with_weather
    FROM public.refresh_beach_day_hourly_scores_bulk(v_state, NULL, v_days_ahead);

  RAISE NOTICE 'refresh_beach_hourly_bulk: state=% days=% beaches=% hours=% tide=% weather=%',
    COALESCE(v_state, 'ALL'), v_days_ahead,
    v_beaches, v_hours, v_with_tide, v_with_weather;
  RETURN NULL;
END $$;

-- ─── Tier C wrappers with internal RETURN; early exits ───────────────
-- Both wrappers had `RETURN;` (void) early-exits; these become `RETURN NULL;`.

DROP FUNCTION IF EXISTS public._orch_w_apply_v2_best_window_beach(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_apply_v2_best_window_beach(p jsonb)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
  v_state_override   text;
  v_days_ahead       int;
  v_state_to_process text;
  v_last_state       text;
  v_b bigint; v_p bigint; v_h bigint; v_r bigint; v_w bigint;
BEGIN
  v_state_override := p->>'state';
  v_days_ahead     := coalesce(nullif(p->>'days_ahead', '')::int, 6);

  IF v_state_override IS NOT NULL THEN
    v_state_to_process := upper(v_state_override);
  ELSE
    SELECT last_state INTO v_last_state
      FROM public._apply_v2_state_cursor
     WHERE entity = 'beach';

    SELECT MIN(state) INTO v_state_to_process
      FROM public.beaches_gold
     WHERE is_active
       AND scoring_tier IN ('daily','hourly')
       AND state IS NOT NULL
       AND (v_last_state IS NULL OR state > v_last_state);

    IF v_state_to_process IS NULL THEN
      SELECT MIN(state) INTO v_state_to_process
        FROM public.beaches_gold
       WHERE is_active
         AND scoring_tier IN ('daily','hourly')
         AND state IS NOT NULL;
    END IF;
  END IF;

  IF v_state_to_process IS NULL THEN
    RAISE NOTICE 'bulk_apply_beach: no states with scoreable beaches; skipping';
    RETURN NULL;
  END IF;

  SELECT beaches_processed, fid_date_pairs, hour_scores_written, recs_written, windows_picked
    INTO v_b, v_p, v_h, v_r, v_w
    FROM public.apply_v2_best_window_to_beach_recommendations_bulk(
      v_state_to_process, NULL, NULL, v_days_ahead);

  IF v_state_override IS NULL THEN
    UPDATE public._apply_v2_state_cursor
       SET last_state = v_state_to_process, updated_at = now()
     WHERE entity = 'beach';
  END IF;

  RAISE NOTICE 'bulk_apply_beach: state=% days=% beaches=% pairs=% hours=% recs=% windows=%',
    v_state_to_process, v_days_ahead, v_b, v_p, v_h, v_r, v_w;
  RETURN NULL;
END $$;

DROP FUNCTION IF EXISTS public._orch_w_apply_v2_best_window_dog_park(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_apply_v2_best_window_dog_park(p jsonb)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
  v_state_override   text;
  v_days_ahead       int;
  v_state_to_process text;
  v_last_state       text;
  v_pp bigint; v_p bigint; v_h bigint; v_r bigint; v_w bigint;
BEGIN
  v_state_override := p->>'state';
  v_days_ahead     := coalesce(nullif(p->>'days_ahead', '')::int, 6);

  IF v_state_override IS NOT NULL THEN
    v_state_to_process := upper(v_state_override);
  ELSE
    SELECT last_state INTO v_last_state
      FROM public._apply_v2_state_cursor
     WHERE entity = 'dog_park';

    SELECT MIN(state) INTO v_state_to_process
      FROM public.dog_parks_gold
     WHERE is_active AND is_scoreable = true
       AND state IS NOT NULL
       AND (v_last_state IS NULL OR state > v_last_state);

    IF v_state_to_process IS NULL THEN
      SELECT MIN(state) INTO v_state_to_process
        FROM public.dog_parks_gold
       WHERE is_active AND is_scoreable = true
         AND state IS NOT NULL;
    END IF;
  END IF;

  IF v_state_to_process IS NULL THEN
    RAISE NOTICE 'bulk_apply_dp: no states with scoreable parks; skipping';
    RETURN NULL;
  END IF;

  SELECT parks_processed, fid_date_pairs, hour_scores_written, recs_written, windows_picked
    INTO v_pp, v_p, v_h, v_r, v_w
    FROM public.apply_v2_best_window_to_recommendations_bulk(
      v_state_to_process, NULL, NULL, v_days_ahead);

  IF v_state_override IS NULL THEN
    UPDATE public._apply_v2_state_cursor
       SET last_state = v_state_to_process, updated_at = now()
     WHERE entity = 'dog_park';
  END IF;

  RAISE NOTICE 'bulk_apply_dp: state=% days=% parks=% pairs=% hours=% recs=% windows=%',
    v_state_to_process, v_days_ahead, v_pp, v_p, v_h, v_r, v_w;
  RETURN NULL;
END $$;

-- ─── Tier B audits (RAISE EXCEPTION exits, no in-body RETURN) ────────

DROP FUNCTION IF EXISTS public._orch_w_audit_pg_cron_failures(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_audit_pg_cron_failures(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_window_days INT     := COALESCE((p->>'window_days')::INT, 7);
  v_exclude     TEXT[]  := COALESCE(
    ARRAY(SELECT jsonb_array_elements_text(p->'exclude')),
    ARRAY[]::TEXT[]
  ) || ARRAY['orch_tick']::TEXT[];
  v_failures    TEXT[]  := ARRAY[]::TEXT[];
  v_rec         RECORD;
BEGIN
  FOR v_rec IN
    WITH window_data AS (
      SELECT d.jobid, d.status, d.start_time, d.return_message
        FROM cron.job_run_details d
       WHERE d.start_time > now() - (v_window_days || ' days')::interval
    ),
    per_job AS (
      SELECT j.jobname,
             COUNT(*) FILTER (WHERE wd.status='failed')    AS fails,
             COUNT(*) FILTER (WHERE wd.status='succeeded') AS succs,
             MAX(wd.start_time) FILTER (WHERE wd.status='failed')    AS last_fail,
             MAX(wd.start_time) FILTER (WHERE wd.status='succeeded') AS last_succ,
             MAX(wd.return_message) FILTER (WHERE wd.status='failed') AS last_err
        FROM cron.job j
        LEFT JOIN window_data wd ON wd.jobid = j.jobid
       WHERE j.active = TRUE
         AND NOT (j.jobname = ANY(v_exclude))
       GROUP BY j.jobname
    )
    SELECT *
      FROM per_job
     WHERE fails > 0
       AND (last_succ IS NULL OR last_succ < last_fail)
     ORDER BY last_fail DESC NULLS LAST
  LOOP
    v_failures := v_failures || format(
      '%s — %s fail / %s ok in %s days; last fail %s; err: %s',
      v_rec.jobname,
      v_rec.fails,
      v_rec.succs,
      v_window_days,
      v_rec.last_fail,
      LEFT(COALESCE(v_rec.last_err, '(no message)'), 200)
    );
  END LOOP;

  IF array_length(v_failures, 1) > 0 THEN
    RAISE EXCEPTION
      E'pg_cron audit: % rotting job(s) in past % days:\n  %',
      array_length(v_failures, 1),
      v_window_days,
      array_to_string(v_failures, E'\n  ');
  END IF;

  RAISE NOTICE 'pg_cron audit: all clear (% days, % jobs excluded)',
    v_window_days, array_length(v_exclude, 1);
  RETURN NULL;
END $$;

DROP FUNCTION IF EXISTS public._orch_w_codify_coverage_audit(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_codify_coverage_audit(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_threshold_pct NUMERIC := COALESCE((p->>'threshold_pct')::NUMERIC, 80);
  v_states        TEXT[]  := ARRAY['CA','OR','WA','MD','UT'];
  v_worst_state   TEXT;
  v_worst_pct     NUMERIC := 101;
  v_rec           RECORD;
  v_pct           NUMERIC;
BEGIN
  FOR v_rec IN
    WITH scope AS (
      SELECT bg.fid, bg.state
        FROM public.beaches_gold bg
       WHERE bg.is_active
         AND bg.scoring_tier IN ('daily','hourly')
         AND bg.state = ANY(v_states)
    )
    SELECT s.state,
           COUNT(*) AS total_scored,
           COUNT(*) FILTER (WHERE EXISTS (
             SELECT 1 FROM public.beach_policy_source bps
              WHERE bps.beach_fid = s.fid
                AND bps.operative_status = 'operative'
                AND bps.section = 'sand'
           )) AS with_sand_bps
      FROM scope s
     GROUP BY s.state
     ORDER BY s.state
  LOOP
    v_pct := round(100.0 * v_rec.with_sand_bps / GREATEST(v_rec.total_scored, 1), 1);
    RAISE NOTICE 'codify_coverage: state=% % / % (% pct)',
      v_rec.state, v_rec.with_sand_bps, v_rec.total_scored, v_pct;
    IF v_pct < v_worst_pct THEN
      v_worst_pct := v_pct;
      v_worst_state := v_rec.state;
    END IF;
  END LOOP;

  IF v_worst_state IS NOT NULL AND v_worst_pct < v_threshold_pct THEN
    RAISE EXCEPTION 'codify_coverage_audit: % at % pct < threshold % pct',
      v_worst_state, v_worst_pct, v_threshold_pct;
  END IF;
  RETURN NULL;
END $$;

-- ─── Tier B audits — UPDATE orch_jobs to record findings ─────────────

DROP FUNCTION IF EXISTS public._orch_w_chunked_refresh_freshness_audit(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_chunked_refresh_freshness_audit(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
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
  WITH per_fid AS (
    SELECT b.fid, max(r.generated_at) AS last_gen
    FROM public.beaches_gold b
    LEFT JOIN public.beach_day_recommendations r ON r.arena_group_id = b.fid
    WHERE b.is_active AND b.scoring_tier IN ('hourly','daily')
    GROUP BY b.fid
  )
  SELECT
    max(last_gen),
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

  WITH per_fid AS (
    SELECT dpg.fid, max(r.generated_at) AS last_gen
    FROM public.dog_parks_gold dpg
    LEFT JOIN public.dog_park_day_recommendations r ON r.dog_park_fid = dpg.fid
    WHERE dpg.is_active AND dpg.is_scoreable
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
  RETURN NULL;
END $$;

DROP FUNCTION IF EXISTS public._orch_w_dogs_policy_parity_audit(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_dogs_policy_parity_audit(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_radius_m            int := COALESCE((p->>'radius_m')::int, 500);
  v_disagree_threshold  int := COALESCE((p->>'disagree_threshold')::int, 0);
  v_disagree_n          int;
  v_curated_yes_no_n    int;
  v_curated_no_yes_n    int;
  v_findings            text;
  v_err                 text;
BEGIN
  WITH verdict_geom AS (
    SELECT 'ubp/' || ubp.fid::text AS origin_key, ubp.geom
      FROM public.us_beach_points ubp
    UNION ALL
    SELECT 'osm/' || osm.osm_type || '/' || osm.osm_id::text AS origin_key, osm.geom
      FROM public.osm_features osm
     WHERE osm.osm_type IS NOT NULL
    UNION ALL
    SELECT 'ccc/' || ccc.objectid::text AS origin_key, ccc.geom
      FROM public.ccc_access_points ccc
  ),
  fid_to_verdict AS (
    SELECT DISTINCT ON (b.fid)
           b.fid,
           bv.dogs_verdict
      FROM public.beaches_gold b
      JOIN verdict_geom vg
        ON ST_DWithin(b.geom::geography, vg.geom::geography, v_radius_m)
      JOIN public.beach_verdicts bv ON bv.origin_key = vg.origin_key
     WHERE b.is_active
       AND b.scoring_tier IN ('hourly','daily')
       AND bv.dogs_verdict IS NOT NULL
     ORDER BY b.fid,
              bv.dogs_verdict_confidence DESC NULLS LAST,
              ST_Distance(b.geom::geography, vg.geom::geography) ASC
  ),
  parity AS (
    SELECT
      b.fid,
      b.state,
      CASE
        WHEN bdp.dogs_allowed IS NULL AND f.dogs_verdict IS NULL                                THEN 'both_null'
        WHEN f.dogs_verdict IS NULL                                                             THEN 'cascade_missing'
        WHEN bdp.dogs_allowed IS NULL                                                           THEN 'curated_missing'
        WHEN bdp.dogs_allowed IN ('yes','mixed','restricted','seasonal') AND f.dogs_verdict = 'yes' THEN 'agree_yes'
        WHEN bdp.dogs_allowed = 'no'  AND f.dogs_verdict = 'no'                                 THEN 'agree_no'
        WHEN bdp.dogs_allowed = 'no'  AND f.dogs_verdict = 'yes'                                THEN 'disagree_curated_no_cascade_yes'
        WHEN bdp.dogs_allowed IN ('yes','mixed','restricted','seasonal') AND f.dogs_verdict = 'no'  THEN 'disagree_curated_yes_cascade_no'
        ELSE 'other'
      END AS parity
      FROM public.beaches_gold b
      LEFT JOIN public.beach_dog_policy bdp ON bdp.arena_group_id = b.fid
      LEFT JOIN fid_to_verdict     f   ON f.fid = b.fid
     WHERE b.is_active AND b.scoring_tier IN ('hourly','daily')
       AND (bdp.zone_rules IS NULL AND COALESCE(bdp.source, '') <> 'manual_curator')
  ),
  per_state AS (
    SELECT state,
           count(*) FILTER (WHERE parity = 'disagree_curated_yes_cascade_no') AS curated_yes_cascade_no,
           count(*) FILTER (WHERE parity = 'disagree_curated_no_cascade_yes') AS curated_no_cascade_yes
      FROM parity
     GROUP BY state
  )
  SELECT
    (SELECT count(*) FILTER (WHERE parity LIKE 'disagree_%') FROM parity),
    (SELECT count(*) FILTER (WHERE parity = 'disagree_curated_yes_cascade_no') FROM parity),
    (SELECT count(*) FILTER (WHERE parity = 'disagree_curated_no_cascade_yes') FROM parity),
    string_agg(
      format('%s: %s curated_yes_cascade_no, %s curated_no_cascade_yes',
             state, curated_yes_cascade_no, curated_no_cascade_yes),
      '; '
      ORDER BY curated_yes_cascade_no DESC, curated_no_cascade_yes DESC)
    INTO v_disagree_n, v_curated_yes_no_n, v_curated_no_yes_n, v_findings
    FROM (
      SELECT *
        FROM per_state
       WHERE curated_yes_cascade_no > 0 OR curated_no_cascade_yes > 0
       ORDER BY curated_yes_cascade_no DESC, curated_no_cascade_yes DESC
       LIMIT 8
    ) s;

  IF v_disagree_n > v_disagree_threshold THEN
    v_err := format(
      'cascade vs curated dogs_allowed disagreement (uncurated beaches only): %s fids total (%s curated_yes_cascade_no; %s curated_no_cascade_yes). Top states: [%s]',
      v_disagree_n,
      COALESCE(v_curated_yes_no_n, 0),
      COALESCE(v_curated_no_yes_n, 0),
      COALESCE(v_findings, '—')
    );
    UPDATE public.orch_jobs
       SET last_failed_at = now(),
           last_error = v_err,
           updated_at = now()
     WHERE job_name = 'dogs_policy_parity_audit';
  ELSE
    UPDATE public.orch_jobs
       SET last_error = NULL,
           updated_at = now()
     WHERE job_name = 'dogs_policy_parity_audit'
       AND last_error LIKE 'cascade vs curated dogs_allowed disagreement%';
  END IF;
  RETURN NULL;
END $$;

DROP FUNCTION IF EXISTS public._orch_w_orch_deps_health_audit(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_orch_deps_health_audit(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_findings text;
  v_count    int;
BEGIN
  WITH dep_check AS (
    SELECT
      j.job_name AS depender,
      unnest(j.depends_on) AS dep
    FROM public.orch_jobs j
    WHERE j.enabled = true
      AND j.depends_on IS NOT NULL
      AND array_length(j.depends_on, 1) > 0
  ),
  broken AS (
    SELECT
      d.depender,
      d.dep,
      CASE
        WHEN target.job_name IS NULL THEN 'missing'
        WHEN target.enabled = false  THEN 'disabled'
      END AS reason
    FROM dep_check d
    LEFT JOIN public.orch_jobs target ON target.job_name = d.dep
    WHERE target.job_name IS NULL OR target.enabled = false
  )
  SELECT
    count(*),
    string_agg(
      format('%s→%s (%s)', depender, dep, reason),
      '; ' ORDER BY depender, dep
    )
  INTO v_count, v_findings
  FROM broken;

  IF v_count > 0 THEN
    UPDATE public.orch_jobs
       SET last_failed_at = now(),
           last_error = format(
             'orch_deps_health: %s broken depends_on entries — %s. These jobs are silently skipped by orch_tick. Fix via UPDATE orch_jobs SET depends_on = ARRAY[''<correct_dep>''] WHERE job_name = ''<depender>''.',
             v_count, v_findings
           ),
           updated_at = now()
     WHERE job_name = 'orch_deps_health_audit';
  ELSE
    UPDATE public.orch_jobs
       SET last_error = NULL,
           updated_at = now()
     WHERE job_name = 'orch_deps_health_audit'
       AND last_error LIKE 'orch_deps_health:%';
  END IF;
  RETURN NULL;
END $$;

DROP FUNCTION IF EXISTS public._orch_w_rec_freshness_audit_per_state(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_rec_freshness_audit_per_state(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_threshold       interval := COALESCE((p->>'stale_after')::interval, interval '26 hours');
  v_beach_findings  text;
  v_dp_findings     text;
  v_beach_stale_n   int;
  v_dp_stale_n      int;
  v_err             text;
BEGIN
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
  RETURN NULL;
END $$;

-- ─── Tier B/C wrappers — loops + helpers ─────────────────────────────

DROP FUNCTION IF EXISTS public._orch_w_codify_gap_clone(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_codify_gap_clone(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_states     TEXT[];
  v_limit      INT;
  v_only_fid   BIGINT;
  v_n_cloned   INT := 0;
  v_n_unhandled INT := 0;
  v_n_failed   INT := 0;
  v_gap        RECORD;
  v_jurisdiction TEXT;
  v_cdpr_unit  TEXT;
  v_template   BIGINT;
  v_match_kind TEXT;
  v_match_label TEXT;
  v_counts     JSONB;
BEGIN
  v_states := COALESCE(
    ARRAY(SELECT jsonb_array_elements_text(p->'states')),
    ARRAY['CA','OR','WA','MD','UT']::TEXT[]
  );
  v_limit := COALESCE((p->>'limit')::INT, 99999);
  v_only_fid := (p->>'only_fid')::BIGINT;

  FOR v_gap IN
    SELECT bg.fid, COALESCE(bg.display_name_override, bg.name) AS name
      FROM public.beaches_gold bg
     WHERE bg.is_active
       AND (v_only_fid IS NOT NULL AND bg.fid = v_only_fid
            OR v_only_fid IS NULL
               AND bg.state = ANY(v_states)
               AND bg.scoring_tier IN ('daily','hourly')
               AND NOT EXISTS (
                 SELECT 1 FROM public.beach_policy_source bps
                  WHERE bps.beach_fid = bg.fid
                    AND bps.operative_status = 'operative'
                    AND bps.section = 'sand'
               ))
     ORDER BY bg.fid
     LIMIT v_limit
  LOOP
    v_template := NULL;
    v_match_kind := NULL;
    v_match_label := NULL;

    v_jurisdiction := public._orch_codify_clone_smallest_jurisdiction(v_gap.fid);
    IF v_jurisdiction IS NOT NULL THEN
      v_template := public._orch_codify_clone_template_sibling(v_jurisdiction, v_gap.fid);
      IF v_template IS NOT NULL THEN
        v_match_kind := 'juri';
        v_match_label := v_jurisdiction;
      END IF;
    END IF;

    IF v_template IS NULL THEN
      v_cdpr_unit := public._orch_codify_clone_nearest_cdpr_unit(v_gap.fid);
      IF v_cdpr_unit IS NOT NULL THEN
        v_template := public._orch_codify_clone_template_sibling_cdpr(v_cdpr_unit, v_gap.fid);
        IF v_template IS NOT NULL THEN
          v_match_kind := 'cdpr';
          v_match_label := v_cdpr_unit;
        END IF;
      END IF;
    END IF;

    IF v_template IS NULL THEN
      v_n_unhandled := v_n_unhandled + 1;
      CONTINUE;
    END IF;

    BEGIN
      v_counts := public._orch_codify_clone_one(v_template, v_gap.fid);
      PERFORM public._promote_zone_rules_for_fid(v_gap.fid);
      v_n_cloned := v_n_cloned + 1;
      RAISE NOTICE 'codify_gap_clone: OK fid=% % match=%=% bps+=% temp+=%',
        v_gap.fid, v_gap.name, v_match_kind, v_match_label,
        v_counts->>'n_bps', v_counts->>'n_temporal';
    EXCEPTION WHEN OTHERS THEN
      v_n_failed := v_n_failed + 1;
      RAISE NOTICE 'codify_gap_clone: FAIL fid=% % %', v_gap.fid, v_gap.name, SQLERRM;
    END;
  END LOOP;

  RAISE NOTICE 'codify_gap_clone done: cloned=% unhandled=% failed=%',
    v_n_cloned, v_n_unhandled, v_n_failed;
  RETURN NULL;
END $$;

DROP FUNCTION IF EXISTS public._orch_w_pipeline_health(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_pipeline_health(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_states         TEXT[];
  v_launch_mode    TEXT;
  v_threshold      NUMERIC;
  v_state          TEXT;
  v_failures       TEXT[] := ARRAY[]::TEXT[];
  v_beach_cnt      INT;
  v_dp_cnt         INT;
  v_inland         BOOLEAN;
  v_ext_cnt        INT;
  v_tier12_cnt     INT;
  v_total_t12      INT;
  v_with_cfips     INT;
  v_with_nsrc      INT;
  v_scoreable      INT;
  v_with_today     INT;
  v_pct            NUMERIC;
  v_bep_has_seed   BOOLEAN;
  v_seasonal_pend  INT;
BEGIN
  v_states := COALESCE(
    NULLIF(ARRAY(SELECT jsonb_array_elements_text(p->'states')), ARRAY[]::TEXT[]),
    ARRAY['CA','OR','WA','MD','UT']::TEXT[]
  );
  v_launch_mode := COALESCE(p->>'launch_mode', 'steady-state');
  v_threshold   := CASE WHEN v_launch_mode = 'state-launch' THEN 0.85 ELSE 0.95 END;

  RAISE NOTICE 'pipeline_health: states=% launch_mode=% threshold=%',
    v_states, v_launch_mode, v_threshold;

  FOREACH v_state IN ARRAY v_states LOOP
    SELECT count(*) INTO v_beach_cnt FROM public.beaches_gold
     WHERE state = v_state AND is_active;
    SELECT count(*) INTO v_dp_cnt FROM public.dog_parks_gold
     WHERE state = v_state AND is_active;
    v_inland := (v_beach_cnt = 0 AND v_dp_cnt > 0);

    SELECT count(*) INTO v_ext_cnt FROM public.external_source_status
     WHERE state = v_state
       AND source IN ('pad_us','osm_landing','osm_amenities','tiger_places')
       AND status IN ('ok','skipped');
    IF v_ext_cnt < 4 THEN
      v_failures := v_failures || format(
        '%s: external sources missing (%s of 4 required)', v_state, v_ext_cnt);
    END IF;

    IF v_beach_cnt = 0 AND NOT v_inland THEN
      v_failures := v_failures || format('%s: 0 active beaches in beaches_gold', v_state);
    END IF;

    SELECT count(*),
           count(*) FILTER (WHERE g.county_fips IS NOT NULL),
           count(*) FILTER (WHERE g.name_source IS NOT NULL)
      INTO v_total_t12, v_with_cfips, v_with_nsrc
      FROM public.beaches_gold g
      JOIN public.beach_dog_policy bdp ON bdp.arena_group_id = g.fid
     WHERE g.state = v_state AND g.is_active
       AND public.beach_location_tier(
             bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash,
             bdp.dogs_prohibited_start::text
           ) IN ('1_off-leash','2_on-leash');
    v_tier12_cnt := v_total_t12;

    IF v_total_t12 = 0 AND NOT v_inland THEN
      v_failures := v_failures || format(
        '%s: 0 beaches in scoring scope (Tier 1+2). Missing state_dogs_policy seed or BEP drift.',
        v_state);
    END IF;
    IF v_total_t12 > 0 AND v_with_cfips < v_total_t12 THEN
      v_failures := v_failures || format(
        '%s: county_fips coverage %s/%s (must be 100%%)', v_state, v_with_cfips, v_total_t12);
    END IF;
    IF v_total_t12 > 0 AND v_with_nsrc < v_total_t12 THEN
      v_failures := v_failures || format(
        '%s: name_source coverage %s/%s (must be 100%%)', v_state, v_with_nsrc, v_total_t12);
    END IF;

    SELECT count(DISTINCT g.fid),
           count(DISTINCT r.location_id) FILTER (WHERE r.local_date = current_date)
      INTO v_scoreable, v_with_today
      FROM public.beaches_gold g
      LEFT JOIN public.beach_day_recommendations r
        ON r.location_id = g.location_id
     WHERE g.scoring_tier IN ('daily','hourly')
       AND g.is_active AND g.state = v_state;
    IF v_scoreable > 0 THEN
      v_pct := v_with_today::NUMERIC / v_scoreable;
      IF v_pct < v_threshold THEN
        v_failures := v_failures || format(
          '%s: today rec coverage %s/%s (%s pct) below threshold %s pct',
          v_state, v_with_today, v_scoreable,
          round(v_pct * 100, 0), round(v_threshold * 100, 0));
      END IF;
    END IF;

    IF v_tier12_cnt > 0 THEN
      SELECT EXISTS (
        SELECT 1 FROM public.beach_enrichment_provenance bep
          JOIN public.beaches_gold g ON g.fid = bep.gold_fid
          JOIN public.beach_dog_policy bdp ON bdp.arena_group_id = g.fid
         WHERE g.state = v_state AND g.is_active
           AND bep.source = 'state_dogs_policy_v1'
           AND public.beach_location_tier(
                 bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash,
                 bdp.dogs_prohibited_start::text
               ) IN ('1_off-leash','2_on-leash')
      ) INTO v_bep_has_seed;
      IF NOT v_bep_has_seed THEN
        v_failures := v_failures || format(
          '%s: state_dogs_policy_v1 missing from BEP for tier-1+2 (populator drift).', v_state);
      END IF;
    END IF;

    SELECT count(*) INTO v_seasonal_pend FROM public.seasonal_closure_seed
     WHERE state_code = v_state AND status = 'pending';
    IF v_seasonal_pend > 0 THEN
      v_failures := v_failures || format(
        '%s: %s seasonal_closure_seed rows still pending', v_state, v_seasonal_pend);
    END IF;

    RAISE NOTICE 'pipeline_health %: t12=% scoreable=% today=% inland=%',
      v_state, v_tier12_cnt, v_scoreable, v_with_today, v_inland;
  END LOOP;

  IF array_length(v_failures, 1) > 0 THEN
    RAISE NOTICE 'pipeline_health: % failures', array_length(v_failures, 1);
    RAISE EXCEPTION 'pipeline_health FAIL: %', array_to_string(v_failures, ' | ');
  END IF;
  RAISE NOTICE 'pipeline_health: all states PASS';
  RETURN NULL;
END $$;

DROP FUNCTION IF EXISTS public._orch_w_run_pipeline_maintenance(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_run_pipeline_maintenance(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_states TEXT[];
  v_state  TEXT;
BEGIN
  PERFORM set_config('statement_timeout', '1800000', true);

  IF p ? 'states' THEN
    v_states := ARRAY(SELECT jsonb_array_elements_text(p->'states'));
  ELSIF p ? 'state' THEN
    v_states := ARRAY[p->>'state']::TEXT[];
  ELSE
    v_states := ARRAY['CA','OR','WA','MD','UT']::TEXT[];
  END IF;

  FOREACH v_state IN ARRAY v_states LOOP
    RAISE NOTICE 'nightly_pipeline_maintenance: running for %', v_state;
    PERFORM public.run_full_pipeline_maintenance(v_state);
  END LOOP;
  RETURN NULL;
END $$;

DROP FUNCTION IF EXISTS public._orch_w_weather_grid_inventory(jsonb);
CREATE OR REPLACE FUNCTION public._orch_w_weather_grid_inventory(p jsonb)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_inserted INT;
  v_total INT;
BEGIN
  WITH ins AS (
    INSERT INTO public.weather_grid (grid_lat, grid_lon, geom, state)
    SELECT DISTINCT
        weather_grid_lat,
        weather_grid_lon,
        ST_SetSRID(ST_MakePoint(weather_grid_lon::float, weather_grid_lat::float), 4326)::geography,
        state
      FROM (
        SELECT weather_grid_lat, weather_grid_lon, state
          FROM public.beaches_gold
         WHERE weather_grid_lat IS NOT NULL AND is_active
           AND scoring_tier IN ('daily','hourly')
        UNION
        SELECT weather_grid_lat, weather_grid_lon, state
          FROM public.dog_parks_gold
         WHERE weather_grid_lat IS NOT NULL AND is_active AND is_scoreable
      ) u
     ON CONFLICT (grid_lat, grid_lon) DO NOTHING
    RETURNING 1
  )
  SELECT count(*) INTO v_inserted FROM ins;

  SELECT count(*) INTO v_total FROM public.weather_grid;
  RAISE NOTICE 'weather_grid_inventory: % new cells, % total', v_inserted, v_total;
  RETURN NULL;
END $$;
