-- Cascade verdict vs curated dog policy parity audit.
--
-- Consumer surface (5 edge fns) reads beach_dog_policy only. The cascade
-- (recompute_all_dogs_verdicts_by_origin, nightly 03:00 UTC) writes
-- beach_verdicts but NOTHING reads it downstream — by design, per CLAUDE.md
-- ("verdict cascade is parity/reference only"). The dbt parity report
-- (consumer_beach_with_verdict.sql) has been broken since 2026-05-02 because
-- it joins dropped public.beaches.
--
-- So when curated and cascade disagree, the divergence is silent. Two bite
-- shapes:
--   (a) curated says dogs OK, cascade says no → user sees "dogs OK" on a
--       beach where every cascade source says no dogs. This is the
--       dangerous case (worse than stale data — wrong data).
--   (b) curated says no, cascade says yes → user sees "no dogs" on a beach
--       where cascade evidence suggests dogs are allowed.
--
-- Bridge fid → origin_key: spatial proximity. beach_dog_policy is keyed on
-- arena_group_id (= beaches_gold.fid per [[arena_group_id_is_fid_not_group]]);
-- beach_verdicts is keyed on origin_key ('ubp/<fid>', 'osm/<type>/<id>',
-- 'ccc/<objectid>'). Union the three sources by their geometry, ST_DWithin
-- 500m of the gold pin, pick highest-confidence verdict per fid (tiebreak by
-- distance). Same approach as the broken dbt model used.
--
-- Comparison axis: dogs_allowed (curated, multi-valued) vs dogs_verdict
-- (cascade, binary yes/no). Curated 'mixed'/'restricted'/'seasonal' all
-- count as "dogs OK with conditions" for parity purposes — agree with
-- cascade 'yes', disagree with cascade 'no'.
--
-- Smoke test results before shipping (500m radius, scored beaches only):
--   2,503 cascade_missing | 381 agree_yes | 94 disagree_curated_yes_cascade_no
-- The 94 are exactly the user-visible risk surface this audit was built for.
--
-- Cadence: daily 04:00 UTC (after verdict_cascade_nightly @ 03:00 + NOAA
-- audit @ 03:30). Cascade output changes nightly, so hourly is overkill.

BEGIN;

CREATE OR REPLACE FUNCTION public._orch_w_dogs_policy_parity_audit(p jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_radius_m            int := COALESCE((p->>'radius_m')::int, 500);
  v_disagree_threshold  int := COALESCE((p->>'disagree_threshold')::int, 0);
  v_disagree_n          int;
  v_curated_yes_no_n    int;
  v_curated_no_yes_n    int;
  v_findings            text;
  v_err                 text;
BEGIN
  -- Union cascade source geometries into one logical table keyed on origin_key.
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
      'cascade vs curated dogs_allowed disagreement: %s fids total (%s curated_yes_cascade_no = users see dogs OK where cascade says no; %s curated_no_cascade_yes). Top states: [%s]',
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
END;
$$;

INSERT INTO public.orch_jobs (
  job_name, description, cadence_kind, cadence_param,
  worker_kind, worker_target, worker_payload,
  enabled, shadow_mode
) VALUES (
  'dogs_policy_parity_audit',
  'Per-fid parity check: beach_dog_policy.dogs_allowed (consumer surface) vs beach_verdicts.dogs_verdict (cascade, parity/reference only). Bridges via 500m spatial proximity from beaches_gold.geom to union(us_beach_points, osm_features, ccc_access_points). Surfaces the "wrong data" case the freshness audits cannot — i.e. consumer shows dogs OK where cascade evidence says no, or vice versa. Per-state breakdown in last_error.',
  'daily_at', '04:00',
  'sql_function', '_orch_w_dogs_policy_parity_audit', '{}'::jsonb,
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

-- Populate state immediately so the dashboard shows accurate counts.
SELECT public._orch_w_dogs_policy_parity_audit('{}'::jsonb);

COMMIT;
