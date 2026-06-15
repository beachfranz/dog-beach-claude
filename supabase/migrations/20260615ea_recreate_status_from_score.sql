-- v3 score: re-create _v2_status_from_score helper.
--
-- The helper was dropped in 20260606c_drop_status_v2_entirely.sql
-- (v2 stopped writing hour_status_v2 / day_status_v2 columns), but v3
-- needs the same score→band mapping to emit status_v3 in
-- compute_beach_hourly_v2's JSON output (Phase C).
--
-- Same thresholds as the original (>=70 clear, >=50 advisory, >=30
-- caution, <30 no_go). The helper does NOT write any column — it's a
-- pure projection used at JSON-emit time. Re-creating it does not
-- resurrect the v2 status writers; those stay retired.
--
-- Surfaced at runtime when calling compute_beach_hourly_v2(8426, ...)
-- after 20260615e shipped: ERROR  42883: function
-- public._v2_status_from_score(integer) does not exist. Body of v3 path
-- references the helper. CREATE OR REPLACE FUNCTION compiles the body
-- lazily, so the missing reference only surfaces at first call.

BEGIN;

CREATE OR REPLACE FUNCTION public._v2_status_from_score(p_score integer)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN p_score IS NULL THEN NULL
    WHEN p_score >= 70   THEN 'clear'
    WHEN p_score >= 50   THEN 'advisory'
    WHEN p_score >= 30   THEN 'caution'
    ELSE                      'no_go'
  END;
$$;

COMMENT ON FUNCTION public._v2_status_from_score(integer) IS
  'Pure score→band projection. >=70 clear, >=50 advisory, >=30 caution, '
  '<30 no_go. Used by v3 singleton path to emit status_v3 at JSON-emit '
  'time. Not a column writer — original v2 writers stayed retired in '
  '20260606c.';

-- Verification
SELECT
  '_v2_status_from_score_exists' AS metric,
  CASE WHEN EXISTS (
    SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
    WHERE n.nspname='public' AND p.proname='_v2_status_from_score'
  ) THEN 'yes' ELSE 'no' END AS val
UNION ALL SELECT '_clear',    public._v2_status_from_score(85)
UNION ALL SELECT '_advisory', public._v2_status_from_score(60)
UNION ALL SELECT '_caution',  public._v2_status_from_score(40)
UNION ALL SELECT '_no_go',    public._v2_status_from_score(10);

COMMIT;
