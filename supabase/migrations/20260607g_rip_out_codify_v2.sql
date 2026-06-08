-- 20260607g_rip_out_codify_v2.sql
--
-- Revert the codify v2 + NH state baseline work from 2026-06-07.
-- That session built jurisdiction-keyed dog-policy infrastructure but
-- the original ADA-coverage goal was orthogonal. Removing all rows
-- added by that detour.
--
-- Order: bps → ps → unreferenced agency → drop SQL fn.
-- BPS delete trigger re-fires consensus cascade automatically.

BEGIN;

-- 1. Identify the policy_source rows we added today (codify v2 + state baseline + pilot).
CREATE TEMP TABLE _to_delete_ps AS
SELECT ps.id
FROM public.policy_source ps
WHERE ps.created_at > '2026-06-07 16:00:00+00'::timestamptz;

-- 2. Delete beach_policy_source rows linking to those policy_source rows.
DELETE FROM public.beach_policy_source bps
USING _to_delete_ps t
WHERE bps.policy_source_id = t.id;

-- 3. Also delete any orphan bps rows extracted_at > cutoff (defensive — covers
--    cases where ps was already gone but bps remained).
DELETE FROM public.beach_policy_source
WHERE extracted_at > '2026-06-07 16:00:00+00'::timestamptz;

-- 4. Capture agency_ids before deleting ps (so we can clean orphan agencies).
CREATE TEMP TABLE _maybe_orphan_agency AS
SELECT DISTINCT ps.issuing_agency_id AS id
FROM public.policy_source ps
WHERE ps.id IN (SELECT id FROM _to_delete_ps)
  AND ps.issuing_agency_id IS NOT NULL;

-- 5. Delete the policy_source rows.
DELETE FROM public.policy_source
WHERE id IN (SELECT id FROM _to_delete_ps);

-- 6. Delete agencies that no other policy_source references AND were created today.
DELETE FROM public.agency a
USING _maybe_orphan_agency m
WHERE a.id = m.id
  AND a.created_at > '2026-06-07 16:00:00+00'::timestamptz
  AND NOT EXISTS (
    SELECT 1 FROM public.policy_source ps2
     WHERE ps2.issuing_agency_id = a.id
  );

-- 7. Drop the codify v2 enumeration SQL function.
DROP FUNCTION IF EXISTS public.codify_v2_enumerate_state(text);

COMMIT;
