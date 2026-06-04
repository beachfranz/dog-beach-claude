-- Phase 10.2 — re-run Phase 10's operator_dogs_policy → policy_source bridge.
--
-- After Step 3 promoted 28 operators to is_canonical=true AND Phase 14e
-- re-resolved entity_operator, some operators that were skipped by the
-- original Phase 10 are now bridge-eligible:
--   - is_canonical=true (Step 3 promotion)
--   - have at least one entity_operator beach row (Phase 14e attribution)
--   - have operator_dogs_policy with policy_found + applies_to_all
--   - no existing policy_source from them (NOT EXISTS guard)
--
-- Same SQL shape as the original Phase 10 — identical idempotent guards
-- ensure already-bridged ops are skipped.

BEGIN;

CREATE TEMP TABLE _bridge_ops AS
SELECT
  odp.operator_id,
  op.canonical_name,
  odp.source_url,
  odp.summary,
  CASE
    WHEN odp.default_rule IN ('prohibited','no') THEN 'not_allowed'
    WHEN odp.default_rule = 'restricted' AND odp.leash_required = true  THEN 'on_leash'
    WHEN odp.default_rule = 'restricted' AND odp.leash_required = false THEN 'off_leash_voice_control'
    WHEN odp.default_rule = 'allowed'    AND odp.leash_required = true  THEN 'on_leash'
    WHEN odp.default_rule = 'allowed'    AND odp.leash_required = false THEN 'off_leash'
    WHEN odp.default_rule = 'seasonal' THEN 'on_leash'
    ELSE NULL
  END AS derived_rule,
  CASE WHEN jsonb_typeof(odp.pass_a_quotes) = 'array' AND jsonb_array_length(odp.pass_a_quotes) > 0
       THEN odp.pass_a_quotes->>0 ELSE NULL END AS first_quote
FROM public.operator_dogs_policy odp
JOIN public.operators op ON op.id = odp.operator_id
WHERE odp.policy_found = true
  AND odp.applies_to_all = true
  AND odp.source_url IS NOT NULL
  AND odp.default_rule IS NOT NULL
  AND op.is_canonical = true
  AND EXISTS (
    SELECT 1 FROM public.entity_operator eo
    WHERE eo.entity_type='beach' AND eo.operator_id = odp.operator_id
  )
  AND NOT EXISTS (
    SELECT 1 FROM public.policy_source ps
    WHERE ps.issuing_operator_id = odp.operator_id
      AND ps.subtype = 'operator_posted_policy'
  );

-- Audit before insert
DO $$
DECLARE v_ops int; v_ops_with_rule int;
BEGIN
  SELECT count(*) INTO v_ops FROM _bridge_ops;
  SELECT count(*) INTO v_ops_with_rule FROM _bridge_ops WHERE derived_rule IS NOT NULL;
  RAISE NOTICE 'Phase 10.2: eligible ops to bridge: % (with derivable rule: %)', v_ops, v_ops_with_rule;
END $$;

WITH inserted AS (
  INSERT INTO public.policy_source
    (subtype, issuing_operator_id, citation, source_url, full_text, last_verified)
  SELECT 'operator_posted_policy'::policy_source_subtype,
         b.operator_id, b.canonical_name, b.source_url, b.summary, now()
  FROM _bridge_ops b
  WHERE b.derived_rule IS NOT NULL
  RETURNING id, issuing_operator_id
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, evidence_verbatim, evidence_url, operative_status)
SELECT
  eo.entity_id AS beach_fid,
  ins.id,
  'sand',
  b.derived_rule,
  b.first_quote,
  b.source_url,
  'operative'
FROM inserted ins
JOIN _bridge_ops b ON b.operator_id = ins.issuing_operator_id
JOIN public.entity_operator eo
  ON eo.entity_type='beach' AND eo.operator_id = ins.issuing_operator_id;

DO $$
DECLARE v_ps int; v_bps int;
BEGIN
  SELECT count(*) INTO v_ps FROM public.policy_source
   WHERE subtype = 'operator_posted_policy'
     AND last_verified > now() - interval '5 minutes';
  SELECT count(*) INTO v_bps FROM public.beach_policy_source bps
    JOIN public.policy_source ps ON ps.id = bps.policy_source_id
   WHERE ps.subtype = 'operator_posted_policy'
     AND ps.last_verified > now() - interval '5 minutes';

  RAISE NOTICE 'Phase 10.2: new policy_source rows: %', v_ps;
  RAISE NOTICE 'Phase 10.2: new beach_policy_source rows: %', v_bps;
END $$;

COMMIT;
