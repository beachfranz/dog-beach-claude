-- public.state_code_to_fips(text) — canonical 2-letter → 2-digit FIPS lookup.
--
-- Added 2026-05-23 LATE (gap #31). Eliminates the 51-entry inline CASE
-- statement duplicated 2× in scripts/run_state_pipeline.py (arena_seed
-- + promote phases). Same dict also exists in scripts/load_state.py as
-- STATE_FIPS Python constant. Three sites must stay in lockstep — the
-- audit (2026-05-23) flagged this as broken-once-per-month risk.
--
-- After this migration: arena_seed and promote use
-- `public.state_code_to_fips($STATE)` instead of the inline CASE.
-- load_state.py keeps its own dict (it's Python-side, can't reach the DB
-- helper at the SQL-string level).
--
-- Coverage: 50 states + DC + PR/GU/VI (matches load_state.py STATE_FIPS).
-- Returns NULL for unknown input (rather than raising) so the join in
-- arena_seed / promote returns 0 rows for typo'd state codes rather than
-- crashing — caller's WHERE clause will already filter on the result.
--
-- Reversibility: DROP FUNCTION public.state_code_to_fips(text);

CREATE OR REPLACE FUNCTION public.state_code_to_fips(state_code text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT CASE upper(state_code)
    WHEN 'AL' THEN '01' WHEN 'AK' THEN '02' WHEN 'AZ' THEN '04' WHEN 'AR' THEN '05'
    WHEN 'CA' THEN '06' WHEN 'CO' THEN '08' WHEN 'CT' THEN '09' WHEN 'DE' THEN '10'
    WHEN 'DC' THEN '11' WHEN 'FL' THEN '12' WHEN 'GA' THEN '13' WHEN 'HI' THEN '15'
    WHEN 'ID' THEN '16' WHEN 'IL' THEN '17' WHEN 'IN' THEN '18' WHEN 'IA' THEN '19'
    WHEN 'KS' THEN '20' WHEN 'KY' THEN '21' WHEN 'LA' THEN '22' WHEN 'ME' THEN '23'
    WHEN 'MD' THEN '24' WHEN 'MA' THEN '25' WHEN 'MI' THEN '26' WHEN 'MN' THEN '27'
    WHEN 'MS' THEN '28' WHEN 'MO' THEN '29' WHEN 'MT' THEN '30' WHEN 'NE' THEN '31'
    WHEN 'NV' THEN '32' WHEN 'NH' THEN '33' WHEN 'NJ' THEN '34' WHEN 'NM' THEN '35'
    WHEN 'NY' THEN '36' WHEN 'NC' THEN '37' WHEN 'ND' THEN '38' WHEN 'OH' THEN '39'
    WHEN 'OK' THEN '40' WHEN 'OR' THEN '41' WHEN 'PA' THEN '42' WHEN 'RI' THEN '44'
    WHEN 'SC' THEN '45' WHEN 'SD' THEN '46' WHEN 'TN' THEN '47' WHEN 'TX' THEN '48'
    WHEN 'UT' THEN '49' WHEN 'VT' THEN '50' WHEN 'VA' THEN '51' WHEN 'WA' THEN '53'
    WHEN 'WV' THEN '54' WHEN 'WI' THEN '55' WHEN 'WY' THEN '56' WHEN 'PR' THEN '72'
    WHEN 'GU' THEN '66' WHEN 'VI' THEN '78'
    ELSE NULL
  END
$$;

COMMENT ON FUNCTION public.state_code_to_fips(text) IS
  'Canonical 2-letter → 2-digit FIPS lookup. Used by pipeline phases '
  'arena_seed + promote. Mirror in Python: scripts/load_state.py STATE_FIPS.';
