-- Fix _governance_score: add completeness penalty for rows missing all
-- three operator-name keys.
--
-- Per Franz 2026-05-19 MVP+ governance investigation. Resolver was
-- picking a `pad_us_unit` row with `operator_name=null` (conf 0.95 +
-- 0.06 state bonus = 1.01) over `county_governance` at conf 0.99 with
-- `operator_name='King County'` (no bonus). Result: many WA state-park
-- beaches stayed unattributed.
--
-- Fix: subtract 0.50 when none of name / operator_name / governing_body_name
-- carry a value. This pushes empty-operator rows below ANY row that
-- actually has operator data, regardless of confidence or polygon_kind.
--
-- Safety: currently-attributed beaches are unaffected because their
-- winning row already has operator data → penalty=0 → unchanged. Only
-- beaches whose elected canonical was an empty row will re-elect.

BEGIN;

CREATE OR REPLACE FUNCTION public._governance_score(
  p_confidence numeric,
  p_claimed_values jsonb,
  p_source text,
  p_operator_level text DEFAULT NULL
)
RETURNS numeric
LANGUAGE sql IMMUTABLE
AS $$
  select
    coalesce(p_confidence, 0)
    -- ── authority_bonus ──
    + case
        when p_source = 'manual' then 0.10
        when p_claimed_values->>'polygon_kind' = 'pad_us_unit'
             and (p_claimed_values->>'mng_type' = 'FED'
                  or p_operator_level = 'federal') then 0.08
        when p_claimed_values->>'polygon_kind' = 'military_base' then 0.07
        when p_claimed_values->>'polygon_kind' = 'pad_us_unit'
             and (p_claimed_values->>'mng_type' = 'STAT'
                  or p_operator_level = 'state')   then 0.06
        when p_claimed_values->>'polygon_kind' = 'tribal_land'  then 0.06
        when p_claimed_values->>'polygon_kind' = 'cpad_unit'    then 0.06
        when p_claimed_values->>'polygon_kind' = 'c1_city'      then 0.03
        when p_claimed_values->>'polygon_kind' = 'cdp'          then 0.02
        when p_claimed_values->>'polygon_kind' = 'county'       then 0.00
        else                                                       -0.05
      end
    -- ── size_bonus ──
    + case
        when (p_claimed_values->>'area_m2')::numeric is null then 0
        when (p_claimed_values->>'area_m2')::numeric < 1e6   then 0.05
        when (p_claimed_values->>'area_m2')::numeric < 1e7   then 0.03
        when (p_claimed_values->>'area_m2')::numeric < 1e8   then 0.01
        else                                                      0.00
      end
    -- ── completeness penalty (2026-05-19) ──
    -- A row with no operator-name signal can't satisfy is_attributed
    -- regardless of confidence. Bury it below any alternative that
    -- carries actual operator data.
    + case
        when p_claimed_values->>'name'                IS NULL
         AND p_claimed_values->>'operator_name'       IS NULL
         AND p_claimed_values->>'governing_body_name' IS NULL
        then -0.50
        else  0.00
      end
$$;

COMMENT ON FUNCTION public._governance_score(numeric, jsonb, text, text) IS
  'Score governance BEP candidates for canonical election. Blends '
  'confidence + polygon_kind authority bonus + size bonus + '
  'completeness penalty. 2026-05-19: added -0.50 penalty when all three '
  'operator-name keys are NULL (otherwise empty pad_us rows with high '
  'confidence + federal/state bonus beat lower-conf rows that carry '
  'actual operator data, blocking is_attributed).';

-- ─── Re-run resolver for currently-unattributed beaches ──────────────
-- Targeted re-election: only beaches where current canonical is empty.
-- For each, _resolve_governance_gold(fid) demotes all canonical rows
-- for that fid + re-elects via the patched score function.

DO $$
DECLARE r record; n int := 0;
BEGIN
  FOR r IN
    SELECT s.fid FROM public.beach_governance_status s
     WHERE s.is_active AND s.is_scoring_active
       AND s.state IN ('CA','OR','WA')
       AND NOT s.is_attributed
  LOOP
    PERFORM public._resolve_governance_gold(r.fid);
    n := n + 1;
  END LOOP;
  RAISE NOTICE 'Re-elected canonical for % unattributed beaches', n;
END $$;

-- ─── Verification ────────────────────────────────────────────────────

SELECT 'OR' AS state, * FROM public.count_unattributed_beaches('OR')
UNION ALL SELECT 'WA', * FROM public.count_unattributed_beaches('WA')
UNION ALL SELECT 'CA', * FROM public.count_unattributed_beaches('CA');

COMMIT;
