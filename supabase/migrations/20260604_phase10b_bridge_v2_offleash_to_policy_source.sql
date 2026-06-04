-- Phase 10b bridge — promote v2 off-leash discoveries to beach_policy_source.
--
-- extract_per_beach_offleash_v2 produced 6 yes-with-cite off-leash discoveries
-- in the CA run. They live in beach_enrichment_provenance with
-- source='per_beach_offleash_v2' but never made it to beach_policy_source
-- (the script's design is BEP-only; the cascade reads beach_policy_source).
--
-- Bridge: for each (op, beach) pair, insert ONE policy_source per operator
-- (find-or-insert keyed on issuing_operator_id) + ONE beach_policy_source
-- per beach with the verbatim quote + rule='off_leash_voice_control'.
--
-- (5 of 6 rows have an attributed canonical operator. The 6th — Coronado Beach
-- fid 8715 — has no entity_operator row; skipped here, flag as parking-lot.)

BEGIN;

-- ─────────────────────────────────────────────────────────────────────
-- 1. Find-or-insert policy_source per (operator, source_url) pair
-- ─────────────────────────────────────────────────────────────────────
WITH v2_wins AS (
  SELECT
    bep.gold_fid                                         AS fid,
    eo.operator_id                                       AS op_id,
    op.canonical_name                                    AS op_name,
    bep.source_url,
    bep.claimed_values->>'source_quote'                  AS quote,
    bep.claimed_values->>'time_window'                   AS time_window
  FROM public.beach_enrichment_provenance bep
  JOIN public.entity_operator eo
    ON eo.entity_type='beach' AND eo.entity_id = bep.gold_fid
  JOIN public.operators op
    ON op.id = eo.operator_id AND op.is_canonical = true
  WHERE bep.source = 'per_beach_offleash_v2'
),
ps_inserted AS (
  INSERT INTO public.policy_source
    (subtype, issuing_operator_id, citation, source_url, full_text, last_verified)
  SELECT DISTINCT ON (v.op_id, v.source_url)
    'operator_posted_policy'::policy_source_subtype,
    v.op_id,
    v.op_name || ' — per-beach off-leash (v2 bridge)',
    v.source_url,
    'Per-beach off-leash discovery via extract_per_beach_offleash_v2 (cite-required quote).',
    now()
  FROM v2_wins v
  WHERE NOT EXISTS (
    SELECT 1 FROM public.policy_source ps
    WHERE ps.issuing_operator_id = v.op_id
      AND ps.source_url = v.source_url
      AND ps.subtype = 'operator_posted_policy'
  )
  RETURNING id, issuing_operator_id, source_url
),
all_ps AS (
  SELECT id, issuing_operator_id, source_url FROM ps_inserted
  UNION
  SELECT ps.id, ps.issuing_operator_id, ps.source_url
  FROM public.policy_source ps
  WHERE EXISTS (
    SELECT 1 FROM v2_wins v
    WHERE v.op_id = ps.issuing_operator_id
      AND v.source_url = ps.source_url
      AND ps.subtype = 'operator_posted_policy'
  )
)
-- ─────────────────────────────────────────────────────────────────────
-- 2. Insert beach_policy_source per (fid, ps) pair
-- ─────────────────────────────────────────────────────────────────────
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, evidence_verbatim, evidence_url, operative_status)
SELECT
  v.fid,
  ps.id,
  'sand',
  CASE
    WHEN v.time_window IS NULL OR v.time_window = '' THEN 'off_leash_voice_control'
    -- Seasonal / time-windowed: still off_leash but flag the window via evidence_verbatim
    ELSE 'off_leash_voice_control'
  END,
  v.quote,
  v.source_url,
  'operative'
FROM (
  SELECT
    bep.gold_fid                                         AS fid,
    eo.operator_id                                       AS op_id,
    bep.source_url,
    bep.claimed_values->>'source_quote'                  AS quote,
    bep.claimed_values->>'time_window'                   AS time_window
  FROM public.beach_enrichment_provenance bep
  JOIN public.entity_operator eo
    ON eo.entity_type='beach' AND eo.entity_id = bep.gold_fid
  JOIN public.operators op
    ON op.id = eo.operator_id AND op.is_canonical = true
  WHERE bep.source = 'per_beach_offleash_v2'
) v
JOIN all_ps ps
  ON ps.issuing_operator_id = v.op_id AND ps.source_url = v.source_url
WHERE NOT EXISTS (
  SELECT 1 FROM public.beach_policy_source bps
  WHERE bps.beach_fid = v.fid AND bps.policy_source_id = ps.id
);

-- ─────────────────────────────────────────────────────────────────────
-- 3. Audit
-- ─────────────────────────────────────────────────────────────────────
DO $$
DECLARE v_ps int; v_bps int; v_v2_rows int; v_skipped int;
BEGIN
  SELECT count(*) INTO v_v2_rows
    FROM public.beach_enrichment_provenance
    WHERE source = 'per_beach_offleash_v2';

  SELECT count(*) INTO v_skipped
    FROM public.beach_enrichment_provenance bep
    WHERE bep.source = 'per_beach_offleash_v2'
      AND NOT EXISTS (
        SELECT 1 FROM public.entity_operator eo
        WHERE eo.entity_type='beach' AND eo.entity_id = bep.gold_fid
      );

  SELECT count(*) INTO v_ps
    FROM public.policy_source
    WHERE citation LIKE '% — per-beach off-leash (v2 bridge)';

  SELECT count(*) INTO v_bps
    FROM public.beach_policy_source bps
    JOIN public.policy_source ps ON ps.id = bps.policy_source_id
    WHERE ps.citation LIKE '% — per-beach off-leash (v2 bridge)';

  RAISE NOTICE '──── Phase 10b v2-bridge audit ────';
  RAISE NOTICE 'v2 BEP rows in scope:               %', v_v2_rows;
  RAISE NOTICE 'Skipped (no entity_operator):       %', v_skipped;
  RAISE NOTICE 'policy_source rows (v2 bridge):     %', v_ps;
  RAISE NOTICE 'beach_policy_source rows written:   %', v_bps;
END $$;

COMMIT;
