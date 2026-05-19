-- Operate Phase 1d — Port San Luis Harbor District (curator-fixed bridge)
--
-- Per docs/operate_pipeline_spec.md (v3) Phase 1d + Franz "1d" 2026-05-18.
-- Pattern: agent-verified operator → singular operator row creation +
-- beach_operator links + policy_source(operator_posted_policy) +
-- per-beach beach_policy_source.
--
-- VERIFIED via agent dispatch 2026-05-18 (Explore subagent):
--   url: https://www.portsanluis.com/2162/Beach-Regulations
--   authority: state-granted trust 1984; Port San Luis Harbor District has
--     tidelands authority over Avila Beach + adjacent shoreline (Avila Beach
--     is an unincorporated community — no city government competes)
--   multi-zone rules:
--     - Avila Beach: off_leash_voice_control before 10am + after 5pm;
--       NOT_ALLOWED 10am-5pm (TEMPORAL — post_apply temporal extractor
--       will lift into beach_policy_source_temporal)
--     - Olde Port Beach: off_leash_voice_control always
--     - Fisherman's Beach: not_allowed (always)
--
-- Existing plural-operator id=1478 has a pass_c_confidence=0.99 extraction
-- with default_rule='restricted', leash_required=true — PARTIALLY WRONG
-- per agent (it captured "leash" but missed the actual rule which is
-- off_leash_voice_control with temporal carve-out). This migration uses
-- the curator-verified policy, not the existing extraction.
--
-- Port San Luis Harbor District is ALREADY in agency table as id=301
-- (special_district type). Use that as the operator's agency_id.

BEGIN;

-- 1. Create singular operator row (idempotent by name+type)
INSERT INTO public.operator (name, short_name, type, agency_id, web_url, rules_url, metadata)
SELECT
    'Port San Luis Harbor District',
    'PSLHD',
    'special_district'::operator_type,
    301,  -- existing agency.id for Port San Luis Harbor District
    'https://www.portsanluis.com/',
    'https://www.portsanluis.com/2162/Beach-Regulations',
    jsonb_build_object(
        'created_for', 'operate_phase1d_2026_05_18',
        'authority_basis', 'state-granted tidelands trust (1984)',
        'manages_beaches', ARRAY['Avila Beach', 'Olde Port Beach', 'Fisherman''s Beach'],
        'agent_verified', true,
        'plural_operator_id', 1478
    )
WHERE NOT EXISTS (
    SELECT 1 FROM public.operator
    WHERE name = 'Port San Luis Harbor District' AND type = 'special_district'
);

-- 2. beach_operator links for the 3 managed beaches
INSERT INTO public.beach_operator (beach_fid, operator_id, scope_section, precedence_rank, notes)
SELECT v.fid, op.id, 'sand', 1, v.note
FROM (VALUES
    (8751::bigint, 'Avila Beach (Port San Luis tidelands trust 1984; multi-zone temporal)'),
    (8767::bigint, 'Fisherman''s Beach (Port San Luis — no dogs)'),
    (8768::bigint, 'Olde Port Beach (Port San Luis — off-leash always)')
) AS v(fid, note)
CROSS JOIN public.operator op
WHERE op.name = 'Port San Luis Harbor District' AND op.type = 'special_district'
ON CONFLICT (beach_fid, operator_id, scope_section) DO NOTHING;

-- 3. policy_source (operator_posted_policy)
INSERT INTO public.policy_source (
    subtype, citation, issuing_operator_id, scope, source_url, full_text,
    last_verified, metadata
)
SELECT
    'operator_posted_policy',
    'Port San Luis Harbor District — Beach Regulations (portsanluis.com)',
    op.id,
    ARRAY['dog_policy']::text[],
    'https://www.portsanluis.com/2162/Beach-Regulations',
    $$Port San Luis Harbor District — Beach Regulations

OPERATIVE RULES per portsanluis.com/2162/Beach-Regulations (verified via curator agent dispatch 2026-05-18):

Olde Port Beach: Dogs are allowed off-leash and under voice-command control at all times (sunrise to sunset).

Avila Beach: Dogs are allowed off-leash and under voice-command control only before 10:00 a.m. and after 5:00 p.m. Between 10:00 am and 5:00 pm there are NO dogs allowed on Avila Beach except for Service dogs.

Fisherman's Beach: No dogs are allowed.

AUTHORITY:
Port San Luis Harbor District holds state-granted tidelands trust (1984) over the shoreline of San Luis Bay, including Avila Beach and its pier. Avila Beach is an unincorporated community — no city government competes; the District is the operating authority.

OPERATOR ENTITY:
Port San Luis Harbor District is a CA special-district public agency. Also registered in agency table as id=301.

[Bridged 2026-05-18 via Operate Phase 1d. Existing plural-operator extraction (id=1478, pass_c_confidence=0.99, default_rule='restricted', leash_required=true) captured the "leash" theme but MISSED the actual off-leash voice-control rule + temporal carve-out. This migration uses the curator-agent-verified policy instead.]$$,
    NOW(),
    jsonb_build_object(
        'curator_verified', true,
        'agent_verified_2026_05_18', true,
        'overrides_plural_extraction_id', 1478,
        'multi_zone', true,
        'temporal_carve_out', 'Avila Beach 10am-5pm not_allowed; before 10am/after 5pm off_leash_voice_control'
    )
FROM public.operator op
WHERE op.name = 'Port San Luis Harbor District' AND op.type = 'special_district'
  AND NOT EXISTS (
    SELECT 1 FROM public.policy_source
    WHERE source_url = 'https://www.portsanluis.com/2162/Beach-Regulations'
);

-- 4a. Olde Port Beach (fid 8768) → off_leash_voice_control (always)
INSERT INTO public.beach_policy_source (
    beach_fid, policy_source_id, section, rule, operative_status,
    evidence_verbatim, evidence_url, status_note, extracted_at, last_verified
)
SELECT 8768::bigint, ps.id, 'sand', 'off_leash_voice_control',
       'operative'::operative_status,
       'Port San Luis Beach Regulations: "Dogs are allowed off-leash and under voice-command control on Olde Port Beach" (always; sunrise to sunset).',
       ps.source_url,
       'Olde Port Beach — off-leash voice-control allowed at all times during open hours. Operator: Port San Luis Harbor District (special_district, agency_id=301).',
       NOW(), NOW()
FROM public.policy_source ps
WHERE ps.source_url = 'https://www.portsanluis.com/2162/Beach-Regulations'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- 4b. Avila Beach (fid 8751) → off_leash_voice_control (with 10am-5pm temporal not_allowed window)
-- Per playbook §6 temporal handling: write the BASE rule here; the
-- temporal extractor (post_apply) will lift the time window into
-- beach_policy_source_temporal.
INSERT INTO public.beach_policy_source (
    beach_fid, policy_source_id, section, rule, operative_status,
    evidence_verbatim, evidence_url, status_note, extracted_at, last_verified
)
SELECT 8751::bigint, ps.id, 'sand', 'off_leash_voice_control',
       'operative'::operative_status,
       'Port San Luis Beach Regulations: "Avila Beach: Dogs are allowed off-leash and under voice-command control only before 10:00 a.m. and after 5:00 p.m. Between 10:00 am and 5:00 pm there are NO dogs allowed on Avila Beach except for Service dogs."',
       ps.source_url,
       'Avila Beach — base rule off-leash voice-control during permitted hours. TEMPORAL: NOT_ALLOWED 10am-5pm (except service dogs). Post-apply temporal extractor will structure the window into beach_policy_source_temporal.',
       NOW(), NOW()
FROM public.policy_source ps
WHERE ps.source_url = 'https://www.portsanluis.com/2162/Beach-Regulations'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- 4c. Fisherman's Beach (fid 8767) → not_allowed (always)
INSERT INTO public.beach_policy_source (
    beach_fid, policy_source_id, section, rule, operative_status,
    evidence_verbatim, evidence_url, status_note, extracted_at, last_verified
)
SELECT 8767::bigint, ps.id, 'sand', 'not_allowed',
       'operative'::operative_status,
       'Port San Luis Beach Regulations: "No dogs are allowed on Fisherman''s Beach."',
       ps.source_url,
       'Fisherman''s Beach — dogs not allowed at any time. Operator: Port San Luis Harbor District.',
       NOW(), NOW()
FROM public.policy_source ps
WHERE ps.source_url = 'https://www.portsanluis.com/2162/Beach-Regulations'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- 5. Verification
SELECT
    'singular_operator' AS what,
    (SELECT id::text FROM public.operator
     WHERE name = 'Port San Luis Harbor District' AND type = 'special_district') AS result
UNION ALL
SELECT 'beach_operator_links',
    (SELECT COUNT(*)::text FROM public.beach_operator bo
     JOIN public.operator op ON op.id = bo.operator_id
     WHERE op.name = 'Port San Luis Harbor District')
UNION ALL
SELECT 'policy_source_id',
    (SELECT id::text FROM public.policy_source
     WHERE source_url = 'https://www.portsanluis.com/2162/Beach-Regulations')
UNION ALL
SELECT 'bps_rows_by_rule',
    (SELECT string_agg(rule || ':' || n::text, ', ' ORDER BY rule)
     FROM (
       SELECT bps.rule, COUNT(*) AS n
       FROM public.beach_policy_source bps
       JOIN public.policy_source ps ON ps.id = bps.policy_source_id
       WHERE ps.source_url = 'https://www.portsanluis.com/2162/Beach-Regulations'
       GROUP BY bps.rule
     ) t)
;

COMMIT;
