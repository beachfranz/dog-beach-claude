-- PostgREST cap detector — surveillance for the silent-truncation class
-- of bug that took out daily-beach-refresh today (1,889 invisible beaches).
--
-- Background: PostgREST applies db-max-rows=1000 (Supabase default) to all
-- table SELECTs and RPC results. When an edge function queries a table
-- with >1000 matching rows without `.range()`, the result is silently
-- truncated. The function thinks it has the full set; the unaccounted
-- rows are invisible. Today's symptoms:
--   - daily-beach-refresh: 1,889 of 2,889 scoreable beaches stuck stale
--   - Cannot be detected by the function itself (no overflow signal)
--   - Audit must run OUTSIDE PostgREST (SQL/PL-pgSQL bypasses the cap)
--
-- This audit counts rows in tables likely to be queried by edge functions
-- without explicit pagination, and flags any whose unfiltered count
-- exceeds the cap. Each finding is a "this table will silently truncate
-- if read without .range()" warning — not a guarantee of a bug, but a
-- canary for developers reviewing new edge function queries.

CREATE OR REPLACE FUNCTION public._orch_w_postgrest_cap_audit(p jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
  v_cap        int    := COALESCE((p->>'db_max_rows')::int, 1000);
  v_findings   text[] := ARRAY[]::text[];
  v_count      int;
  v_msg        text;
BEGIN
  -- Each check: SELECT count(*); if > cap, append "scope=count" to findings.

  -- ── Spine tables (edge functions query these for SELECTs) ───────────
  SELECT count(*) INTO v_count
    FROM public.beaches_gold
    WHERE is_active AND scoring_tier IN ('daily','hourly');
  IF v_count > v_cap THEN
    v_findings := v_findings || format('beaches_gold(scoreable)=%s', v_count);
  END IF;

  SELECT count(*) INTO v_count
    FROM public.dog_parks_gold
    WHERE is_active AND is_scoreable;
  IF v_count > v_cap THEN
    v_findings := v_findings || format('dog_parks_gold(scoreable)=%s', v_count);
  END IF;

  -- ── Recommendation tables (multi-day per fid → very large joins) ────
  SELECT count(*) INTO v_count
    FROM public.beach_day_recommendations r
    JOIN public.beaches_gold g ON g.fid = r.arena_group_id
   WHERE g.is_active AND g.scoring_tier IN ('daily','hourly');
  IF v_count > v_cap THEN
    v_findings := v_findings || format('beach_day_recommendations(scoreable_join)=%s', v_count);
  END IF;

  SELECT count(*) INTO v_count
    FROM public.dog_park_day_recommendations r
    JOIN public.dog_parks_gold p ON p.fid = r.dog_park_fid
   WHERE p.is_active AND p.is_scoreable;
  IF v_count > v_cap THEN
    v_findings := v_findings || format('dog_park_day_recommendations(scoreable_join)=%s', v_count);
  END IF;

  -- ── Hourly scores (~120 hours × pool → very large) ──────────────────
  SELECT count(*) INTO v_count FROM public.beach_day_hourly_scores;
  IF v_count > v_cap THEN
    v_findings := v_findings || format('beach_day_hourly_scores(total)=%s', v_count);
  END IF;

  SELECT count(*) INTO v_count FROM public.dog_park_day_hourly_scores;
  IF v_count > v_cap THEN
    v_findings := v_findings || format('dog_park_day_hourly_scores(total)=%s', v_count);
  END IF;

  -- ── Dog-policy spines (full pool with all fields) ───────────────────
  SELECT count(*) INTO v_count FROM public.beach_dog_policy;
  IF v_count > v_cap THEN
    v_findings := v_findings || format('beach_dog_policy(total)=%s', v_count);
  END IF;

  SELECT count(*) INTO v_count FROM public.dog_park_dog_policy;
  IF v_count > v_cap THEN
    v_findings := v_findings || format('dog_park_dog_policy(total)=%s', v_count);
  END IF;

  -- ── Weather grid (consumer reads via weather_for_point RPC, but the
  --    grid hourly table itself is large enough to bite raw queries) ──
  SELECT count(*) INTO v_count FROM public.weather_grid_hourly;
  IF v_count > v_cap THEN
    v_findings := v_findings || format('weather_grid_hourly(total)=%s', v_count);
  END IF;

  IF array_length(v_findings, 1) > 0 THEN
    v_msg := format(
      'postgrest-cap risk: %s tables exceed db-max-rows=%s — any edge function .select() on these without .range() will silently truncate. Findings: %s. Fix pattern: switch to an aggregating SQL function called via supabase.rpc(...) that returns ≤ cap rows (see beaches_due_for_refresh for the canonical example).',
      array_length(v_findings, 1),
      v_cap,
      array_to_string(v_findings, '; ')
    );
    UPDATE public.orch_jobs
       SET last_failed_at = now(),
           last_error = v_msg,
           updated_at = now()
     WHERE job_name = 'postgrest_cap_audit';
  ELSE
    UPDATE public.orch_jobs
       SET last_error = NULL,
           updated_at = now()
     WHERE job_name = 'postgrest_cap_audit'
       AND last_error LIKE 'postgrest-cap risk:%';
  END IF;
END;
$function$;

-- Register the audit. Daily cadence — table sizes change slowly; hourly
-- would be noise. Plenty of time to fix once flagged.

INSERT INTO public.orch_jobs (
  job_name,
  cadence_kind,
  cadence_param,
  worker_kind,
  worker_target,
  enabled,
  depends_max_age,
  scope_states,
  shadow_mode
) VALUES (
  'postgrest_cap_audit',
  'daily_at',
  '05:00',
  'sql_function',
  '_orch_w_postgrest_cap_audit',
  true,
  '24:00:00'::interval,
  NULL,
  false
)
ON CONFLICT (job_name) DO UPDATE SET
  worker_target = EXCLUDED.worker_target,
  cadence_param = EXCLUDED.cadence_param,
  enabled = EXCLUDED.enabled,
  updated_at = now();
