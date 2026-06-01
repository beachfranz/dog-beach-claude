-- 20260531_orch_engine_pipeline_health.sql
--
-- PL/pgSQL port of scripts/audit/state_population_audit.py (--check mode).
-- The original Python script has 11 sections of human-readable report + 8
-- threshold assertions. The orchestrator only needs the assertions;
-- the human report has no audience in headless mode.
--
-- Assertions (per state):
--   1. ≥4 external_source_status (pad_us, osm_landing, osm_amenities,
--      tiger_places) with status IN (ok, skipped)
--   2. ≥1 active beach in beaches_gold (skip inland)
--   3. county_fips coverage = 100% for tier-1+2 beaches
--   4. name_source coverage = 100% for tier-1+2 beaches
--   5. ≥1 beach in tier-1+2 scope (skip inland)
--   6. today rec coverage ≥ threshold (95% steady / 85% launch)
--   7. state_dogs_policy_v1 in BEP for tier-1+2 fids
--   8. 0 pending seasonal_closure_seed
--
-- "Inland" state = beaches_gold count 0 but dog_parks_gold count > 0;
-- beach-specific assertions are downgraded to soft warnings.
--
-- worker_payload knobs:
--   {"states": ["CA","OR","WA","MD","UT"]}     defaults to MVP+
--   {"launch_mode": "steady-state"|"state-launch"}  default "steady-state"
--
-- Source-of-truth Python (scripts/audit/state_population_audit.py)
-- remains in repo for the full human report.

BEGIN;

CREATE OR REPLACE FUNCTION public._orch_w_pipeline_health(p jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $health$
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
    -- ─── Inland detection ─────────────────────────────────────────────
    SELECT count(*) INTO v_beach_cnt FROM public.beaches_gold
     WHERE state = v_state AND is_active;
    SELECT count(*) INTO v_dp_cnt FROM public.dog_parks_gold
     WHERE state = v_state AND is_active;
    v_inland := (v_beach_cnt = 0 AND v_dp_cnt > 0);

    -- ─── 1. External sources ──────────────────────────────────────────
    SELECT count(*) INTO v_ext_cnt FROM public.external_source_status
     WHERE state = v_state
       AND source IN ('pad_us','osm_landing','osm_amenities','tiger_places')
       AND status IN ('ok','skipped');
    IF v_ext_cnt < 4 THEN
      v_failures := v_failures || format(
        '%s: external sources missing (%s of 4 required)', v_state, v_ext_cnt);
    END IF;

    -- ─── 2. Active beaches ───────────────────────────────────────────
    IF v_beach_cnt = 0 AND NOT v_inland THEN
      v_failures := v_failures || format('%s: 0 active beaches in beaches_gold', v_state);
    END IF;

    -- ─── 3 / 4 / 5. Tier-1+2 scope + structural enrichment ───────────
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

    -- ─── 6. Daily refresh coverage (today rec for scoreable beaches) ─
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

    -- ─── 7. state_dogs_policy_v1 in BEP ───────────────────────────────
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

    -- ─── 8. Seasonal closure seed pending ────────────────────────────
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
END;
$health$;

COMMENT ON FUNCTION public._orch_w_pipeline_health IS
  'orch_jobs worker: per-state population/freshness assertions. PL/pgSQL port of state_population_audit.py --check.';

-- Catalog: noop -> sql_function
UPDATE public.orch_jobs
   SET worker_kind   = 'sql_function',
       worker_target = 'public._orch_w_pipeline_health',
       worker_payload = jsonb_build_object(
         'states', jsonb_build_array('CA','OR','WA','MD','UT'),
         'launch_mode', 'steady-state'
       ),
       description = 'Per-state population + freshness assertions. PL/pgSQL port of state_population_audit.py --check (8 hard-threshold checks).',
       updated_at = now()
 WHERE job_name = 'weekly_pipeline_health';

COMMIT;
