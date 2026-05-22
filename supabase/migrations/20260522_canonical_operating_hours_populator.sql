-- 20260522_canonical_operating_hours_populator.sql
--
-- populate_operating_hours(p_fid) — picks the highest-priority hours
-- source for a beach and writes to beaches_gold.operating_hours.
--
-- Priority chain (highest wins, never overwrites manual_curator):
--   1. manual_curator           — already set; skip
--   2. operator                 — operator_dogs_policy.time_windows  [TODO]
--   3. osm                      — osm_features.opening_hours         [TODO]
--   4. agency                   — scraped from policy pages          [TODO]
--   5. legacy_open_close        — beaches_gold.open_time/close_time
--                                 (de-facto source today; 98.8% coverage)
--                                 written as source='inferred' conf=0.40
--   6. inferred sunrise-sunset  — final fallback, conf=0.30
--
-- Source-availability audit at write time (2026-05-22):
--   - osm: 2 / 1665 beach features have opening_hours → defer
--   - operator: 34 / 418 dogs_policy rows have time_windows → defer (also
--     requires bridge across singular/plural operator tables; see
--     [[operate-two-table-finding]])
--   - legacy open_time/close_time: 3159 / 3196 active beaches → primary
--     source until operator/osm/agency chains land
--
-- MVP strategy: ship legacy + sunrise-sunset only. Every beach ends up
-- with SOMETHING in operating_hours after first run. Layer in
-- operator/osm/agency in followup commits without re-architecting.

BEGIN;

CREATE OR REPLACE FUNCTION public.populate_operating_hours(p_fid bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_curr_source text;
  v_open  time;
  v_close time;
  v_hours_json jsonb;
BEGIN
  -- 0. Never overwrite manual_curator entries.
  SELECT operating_hours_source INTO v_curr_source
    FROM public.beaches_gold WHERE fid = p_fid;
  IF v_curr_source = 'manual_curator' THEN
    RETURN;
  END IF;

  -- 1-3. operator / osm / agency: TODOs (see header).

  -- 4. Legacy open_time/close_time → JSONB default spec.
  SELECT open_time, close_time INTO v_open, v_close
    FROM public.beaches_gold WHERE fid = p_fid;

  IF v_open IS NOT NULL AND v_close IS NOT NULL THEN
    v_hours_json := jsonb_build_object(
      'default', to_char(v_open, 'HH24:MI') || '-' || to_char(v_close, 'HH24:MI')
    );
    UPDATE public.beaches_gold
       SET operating_hours = v_hours_json,
           operating_hours_source = 'inferred',
           operating_hours_confidence = 0.40,
           operating_hours_last_verified = now()
     WHERE fid = p_fid;
    RETURN;
  END IF;

  -- 5. Final fallback: sunrise-sunset spec (consumer-side computed from
  --    lat/lon + date). Confidence 0.30 to flag the UI to hedge copy.
  UPDATE public.beaches_gold
     SET operating_hours = jsonb_build_object('default', 'sunrise-sunset'),
         operating_hours_source = 'inferred',
         operating_hours_confidence = 0.30,
         operating_hours_last_verified = now()
   WHERE fid = p_fid;
END;
$$;

GRANT EXECUTE ON FUNCTION public.populate_operating_hours(bigint)
  TO anon, authenticated, service_role;

-- Bulk per-state convenience: idempotent; safe to re-run.
CREATE OR REPLACE FUNCTION public.refresh_operating_hours_for_state(p_state text)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  n_processed bigint := 0;
  r record;
BEGIN
  FOR r IN
    SELECT fid FROM public.beaches_gold
     WHERE state = p_state AND is_active
     ORDER BY fid
  LOOP
    PERFORM public.populate_operating_hours(r.fid);
    n_processed := n_processed + 1;
  END LOOP;
  RETURN n_processed;
END;
$$;

GRANT EXECUTE ON FUNCTION public.refresh_operating_hours_for_state(text)
  TO anon, authenticated, service_role;

COMMIT;

-- Sanity check (post-apply diagnostic — not executed by migration):
--   SELECT operating_hours_source, COUNT(*)
--     FROM public.beaches_gold
--    WHERE is_active
--    GROUP BY 1 ORDER BY 2 DESC;
