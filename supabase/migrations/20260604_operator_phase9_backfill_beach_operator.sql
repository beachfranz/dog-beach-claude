-- Operator unification — Phase 9: backfill beach_operator from PIP.
--
-- For every active scored beach without an existing beach_operator linkage,
-- runs _resolve_beach_operator(fid) and INSERTs the canonical match with
-- scope_section='sand' (matches existing convention), precedence_rank=1.
--
-- Skips: beaches already linked in beach_operator (19 hand-curated rows
-- with detailed delegation notes — preserved). Beaches whose resolver
-- returns NULL (the 142 that lack a canonical PIP match — Phase 7 territory).
--
-- Behavioral consequence: every newly-linked beach is now Tier-0-eligible
-- IF a policy_source with operator_posted_policy subtype + the same
-- canonical operator as issuing_operator_id exists. Currently 92 such
-- policy_source rows across 25 operators; the join expansion will surface
-- on next call to policy_source_effective_tier_for_beach.

BEGIN;

-- beach_operator is a VIEW over entity_operator WHERE entity_type='beach'.
-- Write directly to entity_operator so the entity_type validation trigger
-- gets the correct value.
INSERT INTO public.entity_operator
  (entity_type, entity_id, operator_id, scope_section, precedence_rank, notes)
SELECT
  'beach'::entity_type,
  b.fid,
  public._resolve_beach_operator(b.fid),
  'sand',
  1,
  'PIP-attribution backfill (operator unification phase 9, 2026-06-04)'
FROM public.beaches_gold b
WHERE b.is_active
  AND b.scoring_tier IN ('hourly','daily')
  AND public._resolve_beach_operator(b.fid) IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM public.entity_operator eo
     WHERE eo.entity_type='beach' AND eo.entity_id = b.fid
  );

-- Post-condition: 19 (existing) + ~2,800 (backfill) ≈ 2,800 total rows.
DO $$
DECLARE v_n int;
BEGIN
  SELECT count(*) INTO v_n FROM public.beach_operator;
  IF v_n NOT BETWEEN 2500 AND 3000 THEN
    RAISE EXCEPTION 'beach_operator row count after backfill out of expected range: %', v_n;
  END IF;
  RAISE NOTICE 'Phase 9 complete: % total beach_operator rows', v_n;
END $$;

COMMIT;
