-- Operate Phase 1a — backfill issuing_operator_id FK on the 3 real
-- operator_posted_policy ps rows that drive consensus but lack FK metadata.
--
-- Per docs/operate_pipeline_spec.md §7 Phase 1a; identified by today's audit.
--
-- ps_20  PSHDB published policy → operator_id=7 (existing)
-- ps_22  City of Long Beach Rosie's published rules → operator_id=52 (existing)
-- ps_233 Trinidad Coastal Land Trust → NEW operator row required (nonprofit
--        not yet in operator table)
--
-- Per [[dont-classify-unknowns-for-deletion]] — this migration UPDATEs
-- existing rows; does NOT delete or duplicate. The 3 ps rows already
-- exist with correct content; only the issuing_operator_id metadata
-- needs filling in.

BEGIN;

-- 1. Create Trinidad Coastal Land Trust operator row (idempotent by name+type)
-- Per [[operator-not-pseudo-agency]] + chk_operator_gov_or_delegated check
-- constraint: non-gov operators need delegated_authority_via set. TCLT manages
-- Houda Point via cooperation with the City of Trinidad (agency_id=81).
INSERT INTO public.operator (name, short_name, type, delegated_authority_via, web_url, rules_url, metadata)
SELECT
    'Trinidad Coastal Land Trust',
    'TCLT',
    'nonprofit'::operator_type,
    81,  -- City of Trinidad (delegating government authority)
    'https://www.trinidadcoastallandtrust.org/',
    'https://www.trinidadcoastallandtrust.org/houda-point.html',
    jsonb_build_object(
        'created_for', 'operate_phase1a_fk_backfill_2026_05_18',
        'manages_properties', ARRAY['Houda Point', 'Camel Rock'],
        'operator_class', 'land_trust',
        'fk_target_for_ps', 233,
        'delegated_authority_basis', 'cooperative land stewardship with City of Trinidad (CA); 501(c)(3) land trust'
    )
WHERE NOT EXISTS (
    SELECT 1 FROM public.operator
    WHERE name = 'Trinidad Coastal Land Trust' AND type = 'nonprofit'
);

-- 2. Backfill issuing_operator_id on the 3 ps rows
UPDATE public.policy_source SET
    issuing_operator_id = 7,
    updated_at = NOW(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
        'fk_backfilled_2026_05_18', true,
        'fk_backfill_reason', 'Phase 1a — row existed since 2026-05-16 with correct content but NULL issuing_operator_id; backfilled per operate spec §7'
    )
WHERE id = 20 AND issuing_operator_id IS NULL;

UPDATE public.policy_source SET
    issuing_operator_id = 52,
    updated_at = NOW(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
        'fk_backfilled_2026_05_18', true,
        'fk_backfill_reason', 'Phase 1a — row existed since 2026-05-16 with correct content but NULL issuing_operator_id; backfilled per operate spec §7'
    )
WHERE id = 22 AND issuing_operator_id IS NULL;

UPDATE public.policy_source SET
    issuing_operator_id = (
        SELECT id FROM public.operator
        WHERE name = 'Trinidad Coastal Land Trust' AND type = 'nonprofit'
    ),
    updated_at = NOW(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
        'fk_backfilled_2026_05_18', true,
        'fk_backfill_reason', 'Phase 1a — row existed since 2026-05-17 with correct content but NULL issuing_operator_id; TCLT operator row created in this migration; backfilled per operate spec §7'
    )
WHERE id = 233 AND issuing_operator_id IS NULL;

-- 3. Verification: all 3 ps rows should now have issuing_operator_id populated
SELECT ps.id, ps.citation,
       ps.issuing_operator_id AS op_id,
       o.name AS operator_name,
       (SELECT COUNT(*) FROM public.beach_policy_source bps WHERE bps.policy_source_id = ps.id) AS n_beaches
FROM public.policy_source ps
LEFT JOIN public.operator o ON o.id = ps.issuing_operator_id
WHERE ps.id IN (20, 22, 233)
ORDER BY ps.id;

COMMIT;
