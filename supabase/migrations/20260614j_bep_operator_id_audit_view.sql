-- 20260614j_bep_operator_id_audit_view.sql
--
-- Audit + verification views for the 20260614i operator-consistency
-- tiebreak. Both are read-only views; safe to drop+recreate without
-- data risk.

BEGIN;

-- ─── 1. vw_dogs_operator_consistency_audit ──────────────────────────────
-- Per-fid summary: canonical operator + BEP-row operator coverage.
DROP VIEW IF EXISTS public.vw_dogs_operator_consistency_audit;
CREATE VIEW public.vw_dogs_operator_consistency_audit AS
SELECT
  bg.fid,
  coalesce(bg.display_name_override, bg.name) AS beach_name,
  bg.state,
  bg.canonical_operator_id,
  canon_op.canonical_name AS canonical_operator_name,
  canon_op.level          AS canonical_operator_level,
  count(bep.*)                                                          AS n_bep_rows,
  count(bep.*) FILTER (WHERE bep.is_canonical)                          AS n_canonical_rows,
  count(bep.*) FILTER (WHERE bep.operator_id IS NULL)                   AS n_null_operator,
  count(bep.*) FILTER (WHERE bep.operator_id = bg.canonical_operator_id) AS n_matched_operator,
  count(bep.*) FILTER (WHERE bep.operator_id IS NOT NULL
                         AND bep.operator_id <> bg.canonical_operator_id) AS n_mismatched_operator,
  -- Canonical row's operator linkage status
  bool_or(bep.is_canonical AND bep.operator_id IS NULL)        AS canonical_has_null_operator,
  bool_or(bep.is_canonical
          AND bep.operator_id = bg.canonical_operator_id)      AS canonical_operator_matched
FROM public.beaches_gold bg
LEFT JOIN public.operators canon_op ON canon_op.id = bg.canonical_operator_id
LEFT JOIN public.beach_enrichment_provenance bep
  ON bep.gold_fid = bg.fid AND bep.field_group = 'dogs'
WHERE bg.is_active AND bg.scoring_tier IN ('daily','hourly')
GROUP BY bg.fid, bg.display_name_override, bg.name, bg.state,
         bg.canonical_operator_id, canon_op.canonical_name, canon_op.level;

COMMENT ON VIEW public.vw_dogs_operator_consistency_audit IS
  'Per-fid summary of dogs BEP operator-linkage status. Use as PBI slicer '
  'to find fids where canonical_operator_matched=false (cascade picked a '
  'NULL-operator row when a matched-operator row was available) or where '
  'n_mismatched_operator > 0 (sources disagreeing on operator identity).';

-- ─── 2. vw_dogs_tiebreak_decided ────────────────────────────────────────
-- Fids where the 20260614i tiebreak actually flipped the canonical row
-- away from id-ASC default, because the canonical row has operator_id
-- matching beach.canonical_operator_id AND there's a same-priority +
-- same-confidence competitor with NULL or mismatched operator.
DROP VIEW IF EXISTS public.vw_dogs_tiebreak_decided;
CREATE VIEW public.vw_dogs_tiebreak_decided AS
SELECT
  bg.fid,
  coalesce(bg.display_name_override, bg.name) AS beach_name,
  bg.state,
  canon_op.canonical_name AS canonical_operator_name,
  e.source                AS canonical_source,
  e.confidence            AS canonical_confidence,
  e.claimed_values->>'allowed'        AS claim_allowed,
  e.claimed_values->>'leash_required' AS claim_leash,
  (SELECT count(*) FROM public.beach_enrichment_provenance e2
    WHERE e2.gold_fid = e.gold_fid
      AND e2.field_group = 'dogs'
      AND e2.id <> e.id
      AND public._gold_field_group_source_priority('dogs', e2.source)
          = public._gold_field_group_source_priority('dogs', e.source)
      AND coalesce(e2.confidence, -1) = coalesce(e.confidence, -1)
      AND (e2.operator_id IS NULL OR e2.operator_id <> bg.canonical_operator_id)
  ) AS n_outranked_competitors
FROM public.beach_enrichment_provenance e
JOIN public.beaches_gold bg ON bg.fid = e.gold_fid
LEFT JOIN public.operators canon_op ON canon_op.id = bg.canonical_operator_id
WHERE e.field_group = 'dogs'
  AND e.is_canonical = true
  AND e.operator_id IS NOT NULL
  AND e.operator_id = bg.canonical_operator_id
  AND EXISTS (
    SELECT 1 FROM public.beach_enrichment_provenance e2
     WHERE e2.gold_fid = e.gold_fid AND e2.field_group = 'dogs'
       AND e2.id <> e.id
       AND public._gold_field_group_source_priority('dogs', e2.source)
           = public._gold_field_group_source_priority('dogs', e.source)
       AND coalesce(e2.confidence, -1) = coalesce(e.confidence, -1)
       AND (e2.operator_id IS NULL OR e2.operator_id <> bg.canonical_operator_id)
  );

COMMENT ON VIEW public.vw_dogs_tiebreak_decided IS
  'Fids where the 20260614i operator-consistency tiebreak flipped the '
  'canonical row to the operator-matched candidate over a tied (same '
  'priority + same confidence) competitor with NULL or mismatched operator. '
  'Spot-check sample for verification of the WA State Parks motivating case.';

COMMIT;

NOTIFY pgrst, 'reload schema';
