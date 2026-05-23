-- 20260523_operating_hours_populator_guard_fix.sql
--
-- Corrective patch on top of 20260522_canonical_operating_hours_populator.sql.
--
-- Bug (ultrareview bug_006): the original step-0 guard only short-circuited
-- when operating_hours_source = 'manual_curator'. For agency / osm /
-- operator rows it fell through and overwrote with the legacy open/close
-- spec (source='inferred', conf 0.40) or sunrise-sunset (conf 0.30),
-- silently destroying agency-extracted hours (conf 0.80) on every
-- operating_hours_refresh phase run — i.e. wiping task #118's deliverable.
--
-- Fix: skip whenever the existing source outranks 'inferred'. Only NULL
-- or already-'inferred' rows fall through to the legacy/fallback writes.

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
  -- 0. Don't overwrite any source that outranks 'inferred'. The legacy
  --    and sunrise-sunset fallbacks below both write source='inferred' —
  --    they must not clobber agency/osm/operator/manual_curator rows.
  SELECT operating_hours_source INTO v_curr_source
    FROM public.beaches_gold WHERE fid = p_fid;
  IF v_curr_source IS NOT NULL AND v_curr_source <> 'inferred' THEN
    RETURN;
  END IF;

  -- 1-3. operator / osm / agency: TODOs (see header of original migration).

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

COMMIT;
