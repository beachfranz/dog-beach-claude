-- Operate Phase 2 — 5 CA special-district operators (13 beach links)
-- Per Franz "ship phase 2" 2026-05-18.
-- All 5 operators agent-verified via parallel Explore agent dispatch.
-- Pattern: PSHDB/PSLHD/TCLT prototype scaled to 5 entities.
--
-- For each:
--   - beach_operator links (idempotent ON CONFLICT)
--   - policy_source(operator_posted_policy) — new ps per operator
--   - beach_policy_source per managed beach with verified rule
-- Consensus engine (commit c5fac20) now boosts operator_posted_policy
-- to tier 0 when beach_operator link exists → these will outrank
-- territorial codify per [[walkthrough-hbdb]] layered authority.

BEGIN;

-- ─── 1. Monte Rio Recreation & Park District (operator_id=110) ────────
-- Agent verified mrrpd.org/monte-rio-beach. Sandy Beach is explicitly
-- dog-friendly with leash; Monte Rio Community Beach less explicit but
-- under same District policy.

INSERT INTO public.beach_operator (beach_fid, operator_id, scope_section, precedence_rank, notes)
SELECT v.fid, 110, 'sand', 1, v.note
FROM (VALUES
    (9083::bigint, 'Sandy Beach (Russian River; agent-verified MRRPD dog-friendly beach)'),
    (9064::bigint, 'Monte Rio Community Beach (MRRPD; agent-verified district scope)')
) AS v(fid, note)
ON CONFLICT (beach_fid, operator_id, scope_section) DO NOTHING;

INSERT INTO public.policy_source (
    subtype, citation, issuing_operator_id, scope, source_url, full_text, last_verified, metadata
) SELECT 'operator_posted_policy',
       'Monte Rio Recreation & Park District — Beach Policy (mrrpd.org)',
       110, ARRAY['dog_policy']::text[],
       'https://www.mrrpd.org/monte-rio-beach/',
       $$Monte Rio Recreation & Park District — Beach Policy

VERBATIM (mrrpd.org/monte-rio-beach, 2026-05-18 agent-verified):
"Sandy Beach is our dog-friendly beach and leashes are required."

OPERATIVE: Sandy Beach dog-friendly w/ leash; Monte Rio Beach + Dutch Bill Beach no explicit dog mention (conservative interpretation: same district leash rule).

District-managed beaches:
- Sandy Beach (west of Monte Rio Bridge) — dogs on leash
- Monte Rio Community Beach — district scope
- Dutch Bill Beach (across river from Sandy Beach) — district scope; not in catalog

[Bridged 2026-05-18 via Operate Phase 2 agent-verified. Existing plural-operator id=1381 extraction at conf 0.95 corroborates 'restricted' default rule.]$$,
       NOW(),
       jsonb_build_object('agent_verified', true, 'plural_operator_id', 1381)
WHERE NOT EXISTS (
    SELECT 1 FROM public.policy_source WHERE source_url = 'https://www.mrrpd.org/monte-rio-beach/'
);

INSERT INTO public.beach_policy_source
    (beach_fid, policy_source_id, section, rule, operative_status,
     evidence_verbatim, evidence_url, status_note, extracted_at, last_verified)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Monte Rio Rec & Park: "Sandy Beach is our dog-friendly beach and leashes are required."',
       ps.source_url,
       'Monte Rio Rec & Park District policy; leashes required on the district''s dog-friendly beach. Other district beaches assumed same rule.',
       NOW(), NOW()
FROM (VALUES (9083::bigint), (9064::bigint)) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.source_url = 'https://www.mrrpd.org/monte-rio-beach/'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 2. East Bay Regional Park District (operator_id=93) ──────────────
-- Agent verified ebparks.org/safety/dogs. District-wide on_leash with
-- swim-beach NOT_ALLOWED per Ordinance 38; Point Isabel = off_leash_voice_control.
-- All 7 of our matched beaches are swim/water beaches → not_allowed.

INSERT INTO public.beach_operator (beach_fid, operator_id, scope_section, precedence_rank, notes)
SELECT v.fid, 93, 'sand', 1, v.note
FROM (VALUES
    (8452::bigint, 'Robert W. Crown Memorial State Beach (EBRPD; Ord 38 — dogs prohibited on sand)'),
    (8534::bigint, 'Lake Anza Beach (EBRPD Tilden; swim beach Ord 38)'),
    (8533::bigint, 'Lake Temescal Beach (EBRPD; swim beach Ord 38)'),
    (8820::bigint, 'Shadow Cliffs Beach (EBRPD; swim beach Ord 38)'),
    (9431::bigint, 'Del Valle East Beach (EBRPD; swim beach Ord 38)'),
    (9432::bigint, 'Del Valle West Beach (EBRPD; swim beach Ord 38)'),
    (8659::bigint, 'Albany Beach (EBRPD-managed/McLaughlin Eastshore SP; jurisdiction note in attribution)')
) AS v(fid, note)
ON CONFLICT (beach_fid, operator_id, scope_section) DO NOTHING;

INSERT INTO public.policy_source (
    subtype, citation, issuing_operator_id, scope, source_url, full_text, last_verified, metadata
) SELECT 'operator_posted_policy',
       'East Bay Regional Park District — Dog Policy (ebparks.org/safety/dogs) — Ord 38',
       93, ARRAY['dog_policy']::text[],
       'https://www.ebparks.org/safety/dogs',
       $$East Bay Regional Park District — Dog Policy + Ordinance 38

VERBATIM (ebparks.org/safety/dogs, 2026-05-18 agent-verified):
"No dogs or other animals are permitted at any swimming pool, beach, wetland or marsh, or designated nature study area. Dogs may be off-leash in open space and undeveloped areas of parklands, provided they are under control at all times."

OPERATIVE — DISTRICT-WIDE LAYERED:
- Default: on_leash (max 6 ft) in parking lots, picnic areas, developed lawns, first 200 ft of trails
- Open space / undeveloped areas: off_leash_voice_control (under-control)
- SWIM BEACHES: not_allowed (per Ordinance 38 — Crown, Miller/Knox, Temescal, Quarry Lakes, Shadow Cliffs, Tilden Lake Anza, Del Valle)
- POINT ISABEL EXCEPTION (1987): off_leash_voice_control throughout — separate ps row recommended

7 catalog beaches all map to swim-beach not_allowed:
- Crown Memorial SB (sand prohibited; pathways/lawn leashed)
- Lake Anza, Lake Temescal, Shadow Cliffs, Del Valle E + W (all swim beaches)
- Albany Beach (EBRPD/McLaughlin Eastshore SP overlap)

[Bridged 2026-05-18 via Operate Phase 2 agent-verified. Pre-existing ps_id=21 EBRPD Crown Beach policy left intact (different scope). Point Isabel + open-space off-leash beaches deferred (not in catalog).]$$,
       NOW(),
       jsonb_build_object('agent_verified', true, 'plural_operator_id', 1160, 'pre_existing_ps_for_crown', 21)
WHERE NOT EXISTS (
    SELECT 1 FROM public.policy_source WHERE source_url = 'https://www.ebparks.org/safety/dogs'
);

INSERT INTO public.beach_policy_source
    (beach_fid, policy_source_id, section, rule, operative_status,
     evidence_verbatim, evidence_url, status_note, extracted_at, last_verified)
SELECT v.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'EBRPD Ord 38: "No dogs or other animals are permitted at any swimming pool, beach, wetland or marsh, or designated nature study area."',
       ps.source_url,
       'EBRPD-managed swim beach; dogs prohibited per district-wide Ordinance 38. Service animals exempt per ADA.',
       NOW(), NOW()
FROM (VALUES (8452::bigint), (8534::bigint), (8533::bigint), (8820::bigint), (9431::bigint), (9432::bigint), (8659::bigint)) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.source_url = 'https://www.ebparks.org/safety/dogs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 3. San Diego Unified Port District (operator_id=128) ─────────────
-- Agent verified portofsandiego.org/parks-faq. Beaches: not_allowed.
-- Catalog match: only Spanish Landing Beach (Silver Strand SB is CA state, not Port).

INSERT INTO public.beach_operator (beach_fid, operator_id, scope_section, precedence_rank, notes)
VALUES (8540::bigint, 128, 'sand', 1, 'Spanish Landing Beach (SD Port-managed shoreline; not_allowed for beach proper)')
ON CONFLICT (beach_fid, operator_id, scope_section) DO NOTHING;

INSERT INTO public.policy_source (
    subtype, citation, issuing_operator_id, scope, source_url, full_text, last_verified, metadata
) SELECT 'operator_posted_policy',
       'San Diego Unified Port District — Parks Pet Policy (portofsandiego.org/parks-faq)',
       128, ARRAY['dog_policy']::text[],
       'https://www.portofsandiego.org/parks-faq',
       $$San Diego Unified Port District — Parks Pet Policy

VERBATIM (portofsandiego.org/parks-faq, 2026-05-18 agent-verified):
"Dogs must be leashed at all times in all Port of San Diego parks."
"Dogs are not allowed on Port of San Diego beaches."

OPERATIVE — PARK vs BEACH split:
- Port-operated PARKS: on_leash (max 6 ft)
- Port-operated BEACHES: not_allowed
- Service animals exempt (ADA)

Port operates 22+ shoreline parks across Coronado, Chula Vista, National City, Imperial Beach, San Diego waterfront. Coronado Tidelands additional: grass-area restriction.

Catalog match: Spanish Landing Beach (fid 8540) is BEACH → not_allowed.

[Bridged 2026-05-18 via Operate Phase 2 agent-verified. Other Port parks (Embarcadero, Cesar Chavez, Coronado Tidelands, Dunes Park) not in beaches_gold catalog; not bridged.]$$,
       NOW(),
       jsonb_build_object('agent_verified', true, 'plural_operator_id', 1545)
WHERE NOT EXISTS (
    SELECT 1 FROM public.policy_source WHERE source_url = 'https://www.portofsandiego.org/parks-faq'
);

INSERT INTO public.beach_policy_source
    (beach_fid, policy_source_id, section, rule, operative_status,
     evidence_verbatim, evidence_url, status_note, extracted_at, last_verified)
SELECT 8540::bigint, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'SD Port: "Dogs are not allowed on Port of San Diego beaches."',
       ps.source_url,
       'Port-operated beach (Spanish Landing); dogs not permitted. Service animals exempt.',
       NOW(), NOW()
FROM public.policy_source ps
WHERE ps.source_url = 'https://www.portofsandiego.org/parks-faq'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 4. North Tahoe Public Utilities District (operator_id=115) ───────
-- Agent verified ntpud.org/resources/faqs. Beaches: not_allowed.

INSERT INTO public.beach_operator (beach_fid, operator_id, scope_section, precedence_rank, notes)
SELECT v.fid, 115, 'sand', 1, v.note
FROM (VALUES
    (6404::bigint, 'North Tahoe Beach (NTPUD-operated; not_allowed per district policy)'),
    (4715::bigint, 'Sandy Beach (NTPUD Placer County portion; partial — CA state parks owns other portion)')
) AS v(fid, note)
ON CONFLICT (beach_fid, operator_id, scope_section) DO NOTHING;

INSERT INTO public.policy_source (
    subtype, citation, issuing_operator_id, scope, source_url, full_text, last_verified, metadata
) SELECT 'operator_posted_policy',
       'North Tahoe PUD — Beach Pet Policy (ntpud.org/resources/faqs)',
       115, ARRAY['dog_policy']::text[],
       'https://ntpud.org/resources/faqs/',
       $$North Tahoe Public Utilities District — Beach Pet Policy

VERBATIM (ntpud.org/resources/faqs, 2026-05-18 agent-verified):
"The District has a policy of no dogs on the beaches for water quality and health reasons."

OPERATIVE: not_allowed on NTPUD-operated beaches.

Catalog matches: North Tahoe Beach (fid 6404), Sandy Beach (fid 4715 — Placer County portion).

NOT NTPUD (attribution notes per agent — covered by other operators):
- Kings Beach State Recreation Area → CA State Parks (transferred 2014)
- Patton Landing, Carnelian West → Tahoe Conservancy
- Coon Street Dog Beach → state-managed (leashed allowed)

[Bridged 2026-05-18 via Operate Phase 2 agent-verified.]$$,
       NOW(),
       jsonb_build_object('agent_verified', true, 'plural_operator_id', 1422)
WHERE NOT EXISTS (
    SELECT 1 FROM public.policy_source WHERE source_url = 'https://ntpud.org/resources/faqs/'
);

INSERT INTO public.beach_policy_source
    (beach_fid, policy_source_id, section, rule, operative_status,
     evidence_verbatim, evidence_url, status_note, extracted_at, last_verified)
SELECT v.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'NTPUD: "The District has a policy of no dogs on the beaches for water quality and health reasons."',
       ps.source_url,
       'North Tahoe PUD-operated beach; dogs not permitted (water quality / health). Service animals exempt.',
       NOW(), NOW()
FROM (VALUES (6404::bigint), (4715::bigint)) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.source_url = 'https://ntpud.org/resources/faqs/'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 5. Truckee-Donner Recreation & Park District (operator_id=143) ───
-- Agent partial DEFER (no comprehensive policy doc) but West End Beach
-- explicitly NO DOGS per official page. Other district beaches deferred.

INSERT INTO public.beach_operator (beach_fid, operator_id, scope_section, precedence_rank, notes)
VALUES (8272::bigint, 143, 'sand', 1, 'West End Beach Park (TDRPD; explicit NO DOGS per facility page)')
ON CONFLICT (beach_fid, operator_id, scope_section) DO NOTHING;

INSERT INTO public.policy_source (
    subtype, citation, issuing_operator_id, scope, source_url, full_text, last_verified, metadata
) SELECT 'operator_posted_policy',
       'Truckee-Donner Rec & Park District — West End Beach Park Rules (tdrpd.org/165)',
       143, ARRAY['dog_policy']::text[],
       'https://www.tdrpd.org/165/West-End-Beach',
       $$Truckee-Donner Recreation & Park District — West End Beach Park

VERBATIM (tdrpd.org/165/West-End-Beach, 2026-05-18 agent-verified):
"NO DOGS, GLASS, OR SMOKING" (West End Beach Park posted rules)

OPERATIVE: not_allowed at West End Beach Park (fid 8272).

TDRPD also manages Shoreline Park, Public Piers, Donner Boat Ramp on Donner Lake — no explicit dog policy published for these (curator review pending). District does have Legacy Trail (Truckee River Regional Park) where dogs are on_leash per posted rules (but that's not a beach in our catalog).

[Bridged 2026-05-18 via Operate Phase 2 agent-verified. Agent recommended contact TDRPD (530-582-7720) for comprehensive policy clarification; deferred to future. plural_operator_id=1659 extraction at conf 0.99 with default_rule='no' corroborates not_allowed for West End.]$$,
       NOW(),
       jsonb_build_object('agent_verified', true, 'plural_operator_id', 1659)
WHERE NOT EXISTS (
    SELECT 1 FROM public.policy_source WHERE source_url = 'https://www.tdrpd.org/165/West-End-Beach'
);

INSERT INTO public.beach_policy_source
    (beach_fid, policy_source_id, section, rule, operative_status,
     evidence_verbatim, evidence_url, status_note, extracted_at, last_verified)
SELECT 8272::bigint, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'TDRPD West End Beach Park: "NO DOGS, GLASS, OR SMOKING".',
       ps.source_url,
       'TDRPD West End Beach Park; dogs not permitted per posted rules.',
       NOW(), NOW()
FROM public.policy_source ps
WHERE ps.source_url = 'https://www.tdrpd.org/165/West-End-Beach'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 6. Verification ──────────────────────────────────────────────────

SELECT op.id AS singular_op_id, op.name AS operator,
       ps.id AS ps_id, ps.subtype,
       COUNT(bps.beach_fid) AS bps_count
FROM public.operator op
JOIN public.policy_source ps ON ps.issuing_operator_id = op.id
LEFT JOIN public.beach_policy_source bps ON bps.policy_source_id = ps.id
WHERE op.id IN (110, 93, 128, 115, 143)
  AND ps.subtype = 'operator_posted_policy'
  AND ps.last_verified >= '2026-05-18'::date
GROUP BY op.id, op.name, ps.id, ps.subtype
ORDER BY op.id;

COMMIT;
