-- Operator unification — Phase 1: add is_canonical column.
--
-- Goal of the staged unification (Franz 2026-06-04): collapse `operator`
-- (singular, 214 hand-curated) and `operators` (plural, ~13,739 bulk-extracted)
-- into one table named `operator` with a boolean `is_canonical` flag. The
-- flag means "this row is eligible for cascade attribution (Tier 0,
-- entity_operator, beach_operator, policy_source.issuing_operator_id) and
-- consumer-surface rendering."
--
-- Phase 1 (this file) is BEHAVIOR-PRESERVING. It:
--   - Adds is_canonical to operators (defaults FALSE).
--   - Sets is_canonical=true on the 214 plural rows that match a singular
--     row by lower(name) (verified 100% match in pre-flight).
--   - Copies web_url + rules_url across from singular into the matched
--     plural rows.
--
-- After this migration:
--   - The cascade still uses the singular `operator` table. Nothing reads
--     is_canonical yet.
--   - 214 plural rows are now marked is_canonical=true and carry the same
--     web_url/rules_url that the singular table carried.
--   - Phase 2 will rewrite cascade functions to read is_canonical while
--     keeping both tables. Phases 3-5 do the FK repoint, drop, and rename.

BEGIN;

-- ── 1. Pre-flight: state must match expectations ─────────────────────
DO $$
DECLARE
  v_singular_count int;
  v_plural_count   int;
  v_unmatched      int;
BEGIN
  SELECT count(*) INTO v_singular_count FROM public.operator;
  SELECT count(*) INTO v_plural_count   FROM public.operators;

  IF v_singular_count NOT BETWEEN 200 AND 250 THEN
    RAISE EXCEPTION 'operator (singular) row count out of expected range: % (expected 200-250)', v_singular_count;
  END IF;
  IF v_plural_count NOT BETWEEN 13500 AND 14000 THEN
    RAISE EXCEPTION 'operators (plural) row count out of expected range: % (expected 13500-14000)', v_plural_count;
  END IF;

  SELECT count(*) INTO v_unmatched
    FROM public.operator s
    WHERE NOT EXISTS (
      SELECT 1 FROM public.operators p
       WHERE lower(p.canonical_name) = lower(s.name)
    );
  IF v_unmatched > 0 THEN
    RAISE EXCEPTION '% singular operator rows have no plural match — manual reconciliation required before re-running', v_unmatched;
  END IF;
END $$;

-- ── 2. Add columns ───────────────────────────────────────────────────
-- is_canonical: the new flag.
-- web_url / rules_url: copied across from the singular table so the data
-- moves with the row. These were only on singular `operator`.
ALTER TABLE public.operators
  ADD COLUMN is_canonical boolean NOT NULL DEFAULT false,
  ADD COLUMN web_url      text,
  ADD COLUMN rules_url    text;

COMMENT ON COLUMN public.operators.is_canonical IS
  'True when this row is eligible for cascade attribution (entity_operator, beach_operator, policy_source.issuing_operator_id) and consumer-surface rendering. Hand-curated rows from the former singular `operator` table all get true at Phase 1 of the operator unification (2026-06-04). PIP-based attribution may flip more rows true later. Read by policy_source_effective_tier_for_beach and downstream cascade functions starting in Phase 2.';

-- ── 3. Promote matched plural rows ───────────────────────────────────
-- Two-step promotion handles the 6 same-name-multi-state ambiguities:
--   * 4 cases (Long Beach, Orange/Riverside/SD County) where only the
--     CA plural row was historically bridged to the canonical via
--     operators.canonical_operator_id — Franz disambiguated these.
--   * 2 cases (California Coastal NM, CDFW) where BOTH plural rows
--     (CA + OR) were intentionally bridged — these CA agencies have
--     multi-state presence and both should remain canonical.
--
-- Step 3a: trust the existing bridge. Promote every plural row whose
--          canonical_operator_id IS NOT NULL.
-- Step 3b: for singular rows with NO bridge anywhere, fall back to
--          lower(name) match. Skip when any plural row is already
--          bridged for the same singular (avoids double-promoting
--          the non-CA dupe).

UPDATE public.operators p
   SET is_canonical = true,
       web_url      = COALESCE(p.web_url,   s.web_url),
       rules_url    = COALESCE(p.rules_url, s.rules_url)
  FROM public.operator s
 WHERE p.canonical_operator_id = s.id;

UPDATE public.operators p
   SET is_canonical = true,
       web_url      = COALESCE(p.web_url,   s.web_url),
       rules_url    = COALESCE(p.rules_url, s.rules_url)
  FROM public.operator s
 WHERE lower(p.canonical_name) = lower(s.name)
   AND NOT EXISTS (
     SELECT 1 FROM public.operators p2
      WHERE p2.canonical_operator_id = s.id
   );

-- ── 4. Post-condition: every singular row has ≥1 canonical match ─────
DO $$
DECLARE
  v_canonical_count   int;
  v_singular_unmapped int;
BEGIN
  SELECT count(*) INTO v_canonical_count FROM public.operators WHERE is_canonical = true;

  -- Every singular row must map to at least one canonical plural row.
  SELECT count(*) INTO v_singular_unmapped
    FROM public.operator s
   WHERE NOT EXISTS (
     SELECT 1 FROM public.operators p
      WHERE p.is_canonical = true
        AND (p.canonical_operator_id = s.id
             OR lower(p.canonical_name) = lower(s.name))
   );
  IF v_singular_unmapped > 0 THEN
    RAISE EXCEPTION '% singular rows have no canonical plural counterpart', v_singular_unmapped;
  END IF;

  RAISE NOTICE 'Phase 1 complete: % plural rows now is_canonical=true', v_canonical_count;
END $$;

COMMIT;
