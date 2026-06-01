-- 20260601_orch_engine_fix_dp_audit_format.sql
--
-- _orch_w_dog_park_data_quality_audit used `format('fid=% % %', ...)` —
-- bare `%` is not a valid format() specifier; needs `%s`. Caught by the
-- 24h smoke test (would never have been observed via scheduled fire
-- because the audit only RAISEs if there are conflicts).

CREATE OR REPLACE FUNCTION public._orch_w_dog_park_data_quality_audit(p jsonb)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
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
           string_agg(format('fid=%s %s %s', fid, state, name), E'\n' ORDER BY state, fid) AS sample
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
$function$;
