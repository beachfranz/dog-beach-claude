-- 20260607b_drop_dp_lighting_dawn_dusk_audit.sql
--
-- Remove the lighting+dawn-dusk "conflict" check from
-- _orch_w_dog_park_data_quality_audit.
--
-- The original audit (20260531_orch_engine_sql_ports.sql) flagged any
-- dog park with lighting=true AND dawn-dusk-style hours_text as a
-- "structurally inconsistent" extraction error. That premise is wrong:
-- a park can legitimately have lighting AND post dawn-dusk hours — the
-- lights extend useful play during the short dawn-dusk window in
-- shorter days (winter). Beaver Brook Dog Park in Worcester MA was the
-- canary; the city's own site advertises the lights as "extending
-- play time for pooches" while also stating dawn-dusk hours.
--
-- This migration replaces the audit with a clean-pass no-op. The
-- orch_job slot is preserved (description updated) so future
-- dog-park data-quality checks can fill it without re-creating
-- plumbing. Today there is no remaining DP data-quality check to
-- run; the function simply NOTICEs that it's a placeholder.

CREATE OR REPLACE FUNCTION public._orch_w_dog_park_data_quality_audit(p jsonb)
RETURNS void
LANGUAGE plpgsql
AS $dqa$
BEGIN
  RAISE NOTICE 'dog_park_data_quality_audit: placeholder (no active checks). '
               'Original lighting+dawn-dusk check removed 2026-06-07 — '
               'legitimate combo (lights extend short winter dawn-dusk play).';
END;
$dqa$;

COMMENT ON FUNCTION public._orch_w_dog_park_data_quality_audit IS
  'orch_jobs worker: dog-park data-quality audit placeholder. The 2026-05-31 lighting+dawn-dusk conflict check was removed 2026-06-07 (legitimate combo: lights extend play during short winter dawn-dusk windows). Add new checks here when needed.';

UPDATE public.orch_jobs
   SET description = 'Placeholder dog-park data-quality audit. No active checks (lighting+dawn-dusk check retired 2026-06-07 — legitimate combo, not a conflict).'
 WHERE job_name = 'weekly_dog_park_data_quality_audit';
