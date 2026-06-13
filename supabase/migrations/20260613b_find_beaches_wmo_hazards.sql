-- 20260613b_find_beaches_wmo_hazards.sql
--
-- Extend find_beaches RPC hazards filter to include the 5 new WMO
-- event_types emitted by refresh_beach_wmo_advisories so they flow
-- into find.html listing cards as condition chips.
--
-- Self-patch via pg_get_functiondef + replace + EXECUTE — same
-- pattern as 20260612a / 20260612c / 20260612e.

BEGIN;

DO $patch$
DECLARE
  v_src text;
  v_old text := E'\'shore_break_risk\'\n            -- \'beach_erosion_risk\' removed 2026-06-11 (low utility)';
  v_new text := E'\'shore_break_risk\',\n            \'lightning_status\',\n            \'fog_status\',\n            \'snow_status\',\n            \'heavy_rain_status\',\n            \'freezing_precip_status\'\n            -- \'beach_erosion_risk\' removed 2026-06-11 (low utility)';
BEGIN
  v_src := pg_get_functiondef('public.find_beaches'::regproc);

  IF position(v_old IN v_src) = 0 THEN
    RAISE EXCEPTION 'find_beaches: hazard IN list anchor missing — body drifted';
  END IF;

  v_src := replace(v_src, v_old, v_new);
  EXECUTE v_src;
END
$patch$;

COMMIT;

NOTIFY pgrst, 'reload schema';
