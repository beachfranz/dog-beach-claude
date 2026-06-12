-- 20260612a_advisory_valid_to_off_by_one.sql
--
-- Fix the deterministic + marine advisory writers so single-hour events
-- don't land with valid_to = valid_from (zero-length window), and
-- multi-hour events don't undershoot their actual end by one hour.
--
-- Bug surfaced 2026-06-12: HB City Beach (fid 8901) showed "Strong wind
-- from 4PM to 4PM" — a zero-length window. ~13% of last-24h
-- deterministic_weather rows were zero-length (~3,100 rows). The same
-- shape affects marine_threshold rows.
--
-- Cause: refresh_beach_advisories / refresh_dog_park_advisories /
-- refresh_marine_advisories all build _extremes with
--     min(forecast_ts) AS first_ts,
--     max(forecast_ts) AS last_ts
-- then upsert valid_from = first_ts, valid_to = last_ts. But forecast_ts
-- is the START of an hour bucket — a single hour triggered at 16:00
-- means the event spans 16:00–17:00, not 16:00 instant. So valid_to
-- should be max(forecast_ts) + interval '1 hour'.
--
-- Patch shape:
--   - Self-patches the live function bodies via pg_get_functiondef +
--     replace() + EXECUTE. Avoids re-shipping the ~500 lines of body
--     and any risk of regressing intermediate edits (e.g., the choppy
--     rule retirement landed earlier today).
--   - Backfills existing rows by adding 1 hour to valid_to where
--     source IN ('deterministic_weather','marine_threshold'). Bacteria
--     rows already write a day-long window, no fix needed there.
--
-- Affected scope (snapshot at fix time):
--   - beach_advisory.deterministic_weather: 30,678 rows
--   - beach_advisory.marine_threshold:      (count varies)
--   - dog_park_advisory.deterministic_weather: 21,037 rows
-- Next orch tick would have overwritten valid_to with the correct value
-- once the function is fixed, but the backfill makes the consumer
-- surface correct immediately.

BEGIN;

-- ─── 1. Patch the three writer functions ──────────────────────────────
DO $patch$
DECLARE
  v_src text;
BEGIN
  -- refresh_beach_advisories: aliased as `l`
  v_src := pg_get_functiondef('public.refresh_beach_advisories'::regproc);
  IF v_src !~ 'max\(l\.forecast_ts\) AS last_ts' THEN
    RAISE EXCEPTION 'refresh_beach_advisories: expected pattern not found — body may have drifted, abort';
  END IF;
  v_src := replace(
    v_src,
    'max(l.forecast_ts) AS last_ts',
    'max(l.forecast_ts) + interval ''1 hour'' AS last_ts'
  );
  EXECUTE v_src;

  -- refresh_dog_park_advisories: aliased as `l`
  v_src := pg_get_functiondef('public.refresh_dog_park_advisories'::regproc);
  IF v_src !~ 'max\(l\.forecast_ts\) AS last_ts' THEN
    RAISE EXCEPTION 'refresh_dog_park_advisories: expected pattern not found — body may have drifted, abort';
  END IF;
  v_src := replace(
    v_src,
    'max(l.forecast_ts) AS last_ts',
    'max(l.forecast_ts) + interval ''1 hour'' AS last_ts'
  );
  EXECUTE v_src;

  -- refresh_marine_advisories: aliased as plain `ts`
  v_src := pg_get_functiondef('public.refresh_marine_advisories'::regproc);
  IF v_src !~ 'max\(ts\) AS last_ts' THEN
    RAISE EXCEPTION 'refresh_marine_advisories: expected pattern not found — body may have drifted, abort';
  END IF;
  v_src := replace(
    v_src,
    'max(ts) AS last_ts',
    'max(ts) + interval ''1 hour'' AS last_ts'
  );
  EXECUTE v_src;
END
$patch$;

-- ─── 2. Backfill existing rows ───────────────────────────────────────
-- Every currently-live deterministic_weather / marine_threshold row has
-- the off-by-one. Safe blanket +1 hour because the function fix above
-- means no rows yet exist that were written with the corrected logic.

UPDATE public.beach_advisory
   SET valid_to = valid_to + interval '1 hour'
 WHERE source IN ('deterministic_weather', 'marine_threshold');

UPDATE public.dog_park_advisory
   SET valid_to = valid_to + interval '1 hour'
 WHERE source = 'deterministic_weather';

COMMIT;

NOTIFY pgrst, 'reload schema';
