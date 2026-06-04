-- Operator unification — Phase 10: bridge operator_dogs_policy → policy_source.
--
-- operator_dogs_policy already holds the extracted operator-posted policies
-- (policy_found=true, default_rule set, source_url captured, verbatim quotes
-- in pass_a_quotes) for 208 canonical operators. But the cascade reads these
-- as the weak operator_default signal (weight 0.4). To get Tier 0 attribution,
-- the same content needs to live in policy_source with subtype='operator_posted_policy'
-- + beach_policy_source linking each beach.
--
-- This phase bridges the SAFE subset: applies_to_all=true (58 operators, 249
-- beaches). These are universal-scope policies (City of Seattle's leash law,
-- Dukes County's beach rules, etc.) where it's correct to apply the policy to
-- every beach attributed to that operator.
--
-- The applies_to_all=false subset (49 ops, 428 beaches) holds beach-specific
-- policies (Sandy Neck Park PDF, Long Beach seasonal rules, etc.). Bridging
-- those naively would mis-attribute one beach's rule to all beaches under the
-- operator. Handled separately.
--
-- Rule derivation from default_rule + leash_required:
--   default_rule='prohibited' or 'no' → 'not_allowed'
--   default_rule='restricted' + leash_required=true  → 'on_leash'
--   default_rule='restricted' + leash_required=false → 'off_leash_voice_control'
--   default_rule='allowed'    + leash_required=true  → 'on_leash'
--   default_rule='allowed'    + leash_required=false → 'off_leash'
--   default_rule='seasonal' → 'on_leash' (seasonal exceptions encoded elsewhere)

BEGIN;

-- Build a working set: (operator_id, rule, source_url, citation, evidence_verbatim)
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
  -- First quote in pass_a_quotes (cite text)
  CASE WHEN jsonb_typeof(odp.pass_a_quotes) = 'array' AND jsonb_array_length(odp.pass_a_quotes) > 0
       THEN odp.pass_a_quotes->>0 ELSE NULL END AS first_quote
FROM public.operator_dogs_policy odp
JOIN public.operators op ON op.id = odp.operator_id
WHERE odp.policy_found = true
  AND odp.applies_to_all = true
  AND odp.source_url IS NOT NULL
  AND odp.default_rule IS NOT NULL
  AND op.is_canonical = true
  AND EXISTS (SELECT 1 FROM public.entity_operator eo
              WHERE eo.entity_type='beach' AND eo.operator_id = odp.operator_id)
  -- Skip operators that already have policy_source entries (avoid double-promotion)
  AND NOT EXISTS (SELECT 1 FROM public.policy_source ps
                  WHERE ps.issuing_operator_id = odp.operator_id
                    AND ps.subtype = 'operator_posted_policy');

-- Insert ONE policy_source row per operator and capture the new id
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

COMMIT;
