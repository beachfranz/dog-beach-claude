-- Operate Phase 1 — PSHDB / Huntington Dog Beach bridge (curator-fixed)
--
-- The existing operator_dogs_policy row for operator_id=7 (Preservation
-- Society of Huntington Dog Beach, "PSHDB") was CORRUPT: its source_url
-- pointed to longbeach.gov/.../rosies-dog-beach (Rosie's Dog Beach in
-- LONG BEACH, CA) and pass_a_quotes described Rosie's "6am-8pm Dog Zone".
-- Wrong operator-to-source attribution in the existing 3-pass extraction
-- infrastructure.
--
-- Per Franz 2026-05-18: PSHDB operates Huntington Dog Beach (fid 6212);
-- their posted policy is OFF-LEASH ("leash optional").
--
-- Verified via dogbeach.org (PSHDB's official site, 2026-05-18):
--   "The leash law applies to all dogs! Dog Beach is leash optional.
--    Please be mindful of leashed dogs that may be new to the environment.
--    Dog Beach is next to Highway 1 and can be a danger to your dog..."
--
-- This migration bridges PSHDB → policy_source(operator_posted_policy)
-- → beach_policy_source(fid=6212, rule=off_leash). Per playbook tenet #5
-- the cascade fires automatically on bps INSERT.
--
-- The corrupt operator_dogs_policy row is NOT modified here; the data
-- quality issue is flagged for re-extraction in a separate ticket. This
-- migration is a CURATOR OVERRIDE that goes around the broken extraction.

BEGIN;

-- 1. policy_source (operator_posted_policy, issued by PSHDB operator)
INSERT INTO public.policy_source (
    subtype, citation, issuing_operator_id, scope, source_url, full_text,
    last_verified, metadata
)
SELECT
    'operator_posted_policy',
    'Preservation Society of Huntington Dog Beach (PSHDB) posted policy — Dog Beach is leash optional',
    7,  -- PSHDB operator id
    ARRAY['dog_policy']::text[],
    'https://www.dogbeach.org/financial',
    $$PSHDB (Preservation Society of Huntington Dog Beach) — Posted Policy

OPERATIVE RULE: Off-leash optional on the Dog Beach sand. On-leash required en route (until on the beach sand). Geographic scope: 1.5-mile stretch of beach between Goldenwest and Seapoint Streets, on the PCH side.

VERBATIM (dogbeach.org/financial, About page, 2026-05-18):
"The Preservation Society of Huntington Dog Beach is a 501(c)3 nonprofit organization, operating in the City of Huntington Beach, California as the Preservation Society of Huntington Dog Beach.

PRESERVE! PLAY! PICK UP!

We strive to PRESERVE the 'Off Leash Optional' status of Dog Beach for our canine companions so they can run and PLAY on sand and surf to their hearts' content...

HISTORY: Our founder, Martin Senat, campaigned for years to approve the use of the 1.5 mile stretch of beach between Goldenwest and Seapoint as an 'Off-Leash Optional' beach for dogs. In 1998 the Preservation Society of Huntington Dog Beach was founded by Martin Senat...

LOCATION: Huntington Beach is a special place because we have Dog Beach — a mile and a half stretch of sand located on Pacific Coast Highway between Goldenwest Street and Seapoint Street where our canine companions are welcome to run and play off leash. (Dogs must remain on leashes until you are on the beach sand)."

Also from dogbeach.org homepage:
"The leash law applies to all dogs! Dog Beach is leash optional. Please be mindful of leashed dogs that may be new to the environment. Dog Beach is next to Highway 1 and can be a danger to your dog."

CONTEXT:
- PSHDB is a 501(c)(3) nonprofit founded 1998; operates the dog beach via partnership w/ the City of Huntington Beach.
- City of Huntington Beach's general leash ordinance applies citywide and en route to the dog beach.
- The PSHDB-stewarded carve-out (Goldenwest-to-Seapoint on PCH) is the off-leash exception.
- 3,000 doggy-waste bags consumed daily — operational scale of the operator's role.

CITATION HIERARCHY (per [[walkthrough-hbdb]]):
- City of Huntington Beach Municipal Code (citywide leash) — territorial codify
- City/PSHDB partnership (501(c)(3) operator running the carve-out)
- PSHDB posted policy (this row) — operator-specific override at Dog Beach (fid 6212)

Per [[operators-as-first-class]] + [[walkthrough-hbdb]] layered authority: the operator-posted rule WINS for this specific entity (Dog Beach proper).

[Bridged 2026-05-18 via curator override. Existing operator_dogs_policy.operator_id=7 was CORRUPT (source_url pointed to Rosie's Dog Beach in Long Beach; pass_a_quotes described Rosie's 6am-8pm Dog Zone — wrong attribution). Verified via dogbeach.org/financial About page + homepage; URL pinned by Franz 2026-05-18. operator_dogs_policy.operator_id=7 flagged for re-extraction.]$$,
    NOW(),
    jsonb_build_object(
        'curator_override', true,
        'override_reason', 'Existing 3-pass extraction at operator_dogs_policy.operator_id=7 was corrupt (wrong source_url; described Rosie''s Dog Beach, not Huntington Dog Beach). PSHDB''s actual policy verified via dogbeach.org 2026-05-18.',
        'corrupt_extraction_source_url', 'https://www.longbeach.gov/park/park-and-facilities/directory/rosies-dog-beach',
        'spec_walkthrough', 'walkthrough-hbdb (layered authority: city leash → MOU → operator)'
    )
WHERE NOT EXISTS (
    SELECT 1 FROM public.policy_source
    WHERE issuing_operator_id = 7
      AND subtype = 'operator_posted_policy'
      AND source_url = 'https://www.dogbeach.org/financial'
);

-- 2. beach_policy_source link: PSHDB rule → Dog Beach (fid 6212)
INSERT INTO public.beach_policy_source (
    beach_fid, policy_source_id, section, rule, operative_status,
    evidence_verbatim, evidence_url, status_note, extracted_at, last_verified
)
SELECT
    6212::bigint,
    ps.id,
    'sand',
    'off_leash',
    'operative'::operative_status,
    'PSHDB posted policy (dogbeach.org): "PRESERVE the Off Leash Optional status of Dog Beach... 1.5 mile stretch of beach between Goldenwest and Seapoint as an Off-Leash Optional beach for dogs... Dogs must remain on leashes until you are on the beach sand."',
    ps.source_url,
    'Operator-specific override at Huntington Dog Beach (fid 6212). Off-leash on the 1.5-mile sand stretch Goldenwest-Seapoint; on-leash en route per PSHDB posted policy. Citywide HB leash ordinance applies en route + at adjacent beaches outside the carve-out. Highway 1 proximity hazard; PSHDB recommends mindfulness re: leashed dogs new to environment.',
    NOW(), NOW()
FROM public.policy_source ps
WHERE ps.issuing_operator_id = 7
  AND ps.subtype = 'operator_posted_policy'
  AND ps.source_url = 'https://www.dogbeach.org/financial'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- 3. Verification
SELECT
    ps.id AS ps_id,
    ps.citation,
    bps.rule,
    bps.beach_fid,
    b.name AS beach_name
FROM public.policy_source ps
JOIN public.beach_policy_source bps ON bps.policy_source_id = ps.id
JOIN public.beaches_gold b ON b.fid = bps.beach_fid
WHERE ps.issuing_operator_id = 7
  AND ps.subtype = 'operator_posted_policy'
  AND ps.source_url = 'https://www.dogbeach.org/financial';

COMMIT;
