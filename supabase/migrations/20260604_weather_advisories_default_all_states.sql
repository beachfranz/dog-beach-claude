-- Make _orch_w_compute_weather_advisories default to all active+scored beaches.
--
-- Prior behavior: defaulted to MVP+ states (CA, OR, WA, MD, UT). New states
-- (AL beaches verified tonight) were silently excluded from the daily 12:00 UTC
-- cron, so consumers there saw empty cautions cards until someone ran the
-- Python script manually with --all-scored.
--
-- New behavior: when 'states' is absent or empty in the payload, fall back
-- to all active scored beaches regardless of state. Explicit state arrays
-- in the payload still work for scoped runs.
--
-- Matches what compute_weather_advisories.py --all-scored does, so future
-- state launches automatically light up without a payload update.

CREATE OR REPLACE FUNCTION public._orch_w_compute_weather_advisories(p jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
  v_states      TEXT[];
  v_only_fid    BIGINT;
  v_beach       RECORD;
  v_n_evaluated INT := 0;
  v_n_upserted  INT := 0;
  v_n_retired   INT := 0;
  v_one         JSONB;
BEGIN
  -- NULL when not provided → no state filter (all active scored beaches).
  v_states   := NULLIF(ARRAY(SELECT jsonb_array_elements_text(p->'states')), ARRAY[]::TEXT[]);
  v_only_fid := (p->>'only_fid')::BIGINT;

  FOR v_beach IN
    SELECT fid, location_id
      FROM public.beaches_gold
     WHERE is_active
       AND location_id IS NOT NULL
       AND (
         (v_only_fid IS NOT NULL AND fid = v_only_fid)
         OR (v_only_fid IS NULL
             AND scoring_tier IN ('daily','hourly')
             AND (v_states IS NULL OR state = ANY(v_states)))
       )
     ORDER BY fid
  LOOP
    v_one := public._orch_compute_weather_advisories_one(v_beach.fid, v_beach.location_id);
    v_n_evaluated := v_n_evaluated + 1;
    v_n_upserted  := v_n_upserted  + COALESCE((v_one->>'upserted')::INT, 0);
    v_n_retired   := v_n_retired   + COALESCE((v_one->>'retired')::INT, 0);
  END LOOP;

  RAISE NOTICE 'weather_advisories: evaluated=% upserted=% retired=%',
    v_n_evaluated, v_n_upserted, v_n_retired;
END;
$function$;
