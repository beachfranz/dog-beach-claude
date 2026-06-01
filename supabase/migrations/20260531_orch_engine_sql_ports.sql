-- 20260531_orch_engine_sql_ports.sql
--
-- Port Dagster Python assets to native SQL functions where the work is
-- pure DB queries. Three of the noop-cataloged assets are trivial SQL
-- (codify_coverage_audit, dog_park_data_quality_audit, weather_grid_inventory)
-- and ship here. The Dagster Python source-of-truth remains for local
-- dev / debugging.
--
-- Not ported in this migration (need TS edge function or are too heavy):
--   weather_grid_hourly         — Open-Meteo HTTP × ~4000 cells
--   daily_weather_advisories    — wraps 339-line compute_weather_advisories.py
--   daily_dog_park_advisories   — same shape as above
--   weekly_codify_gap_clone     — wraps 352-line codify_clone_gap_beaches.py
--   weekly_pipeline_health      — multi-asset pipeline health probe
--   monthly_dog_park_coverage   — 9-op LLM pipeline (not portable)
--
-- These remain worker_kind='noop' in orch_jobs. The path to enable them
-- is to write a TS edge function (placed in supabase/functions/) and
-- update the orch_jobs row to worker_kind='edge_function'.

BEGIN;

-- ─── codify_coverage_audit ────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public._orch_w_codify_coverage_audit(p jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $audit$
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
END;
$audit$;

COMMENT ON FUNCTION public._orch_w_codify_coverage_audit IS
  'orch_jobs worker: per-state sand-bps coverage audit. Raises on regression below threshold_pct (default 80).';

-- ─── dog_park_data_quality_audit ─────────────────────────────────────

CREATE OR REPLACE FUNCTION public._orch_w_dog_park_data_quality_audit(p jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $dqa$
DECLARE
  v_n_conflicts INT;
  v_sample TEXT;
BEGIN
  WITH lit_dusk AS (
    SELECT g.fid, g.state,
           COALESCE(g.display_name_override, g.name) AS name,
           policy.hours_text
      FROM public.dog_parks_gold g
      JOIN public.dog_park_dog_policy policy ON policy.dog_park_fid = g.fid
     WHERE g.is_active
       AND policy.lighting = true
       AND (policy.hours_text ILIKE '%dawn%dusk%'
            OR policy.hours_text ILIKE '%sunrise%sunset%'
            OR policy.hours_text ILIKE 'dawn-dusk'
            OR policy.hours_text ILIKE 'dawn to dusk')
  ),
  agg AS (
    SELECT COUNT(*)::INT AS n,
           string_agg(format('fid=% % %', fid, state, name), E'\n' ORDER BY state, fid) AS sample
      FROM lit_dusk
  )
  SELECT n, COALESCE(sample, '(clean)') INTO v_n_conflicts, v_sample FROM agg;

  RAISE NOTICE 'dog_park_data_quality: lighting+dawn-dusk conflicts = %', v_n_conflicts;
  IF v_n_conflicts > 0 THEN
    RAISE NOTICE 'sample:%', E'\n' || v_sample;
    RAISE EXCEPTION 'dog_park_data_quality_audit: % lighting+dawn-dusk conflicts. '
      'Fix: python scripts/extract_dog_park_amenities.py --apply --fids <fids> '
      '--include-no-website --workers 4', v_n_conflicts;
  END IF;
END;
$dqa$;

COMMENT ON FUNCTION public._orch_w_dog_park_data_quality_audit IS
  'orch_jobs worker: surface dog parks with lighting=true but dawn-dusk hours (structurally inconsistent).';

-- ─── weather_grid_inventory ──────────────────────────────────────────

CREATE OR REPLACE FUNCTION public._orch_w_weather_grid_inventory(p jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $inv$
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
END;
$inv$;

COMMENT ON FUNCTION public._orch_w_weather_grid_inventory IS
  'orch_jobs worker: rebuild weather_grid from UNION of active scoreable beaches + dog parks (backstop).';

-- ─── Update orch_jobs catalog: noop → sql_function for ported assets ──

UPDATE public.orch_jobs
   SET worker_kind   = 'sql_function',
       worker_target = 'public._orch_w_codify_coverage_audit',
       worker_payload = jsonb_build_object('threshold_pct', 80),
       description = 'Per-state sand-bps coverage audit. Raises on regression below 80%. SQL port of Dagster codify_coverage_audit.',
       updated_at = now()
 WHERE job_name = 'daily_codify_coverage_audit';

UPDATE public.orch_jobs
   SET worker_kind   = 'sql_function',
       worker_target = 'public._orch_w_dog_park_data_quality_audit',
       worker_payload = '{}'::jsonb,
       description = 'Detect dog parks with lighting=true + dawn-dusk hours. Raises on conflict. SQL port of Dagster dog_park_data_quality_audit.',
       updated_at = now()
 WHERE job_name = 'weekly_dog_park_data_quality_audit';

UPDATE public.orch_jobs
   SET worker_kind   = 'sql_function',
       worker_target = 'public._orch_w_weather_grid_inventory',
       worker_payload = '{}'::jsonb,
       description = 'Rebuild weather_grid cell inventory from active entity tables. SQL port of Dagster weather_grid_inventory.',
       updated_at = now()
 WHERE job_name = 'weather_grid_inventory';

COMMIT;
