-- NPS Lake Roosevelt National Recreation Area (LARO) — Pet Policy
-- per-unit override of the 36 CFR §2.15 federal baseline — 2026-05-18
--
-- Source: https://www.nps.gov/laro/planyourvisit/pets.htm (verified via agent
-- dispatch 2026-05-18)
--
-- OPERATIVE RULE (verbatim):
--   "Dogs are allowed in the park, except in developed swim beach areas,
--    as long as they wear their leash." [no more than 6 feet long]
--
-- MULTI-ZONE — per-beach interpretation:
--
-- DEFAULT (on_leash):
--   fid 17159836  Apple Beach           — Lake Roosevelt shoreline
--   fid 15029295  Bradbury Beach        — Lake Roosevelt shoreline
--   fid 16669115  Crescent Bay On Lake Roosevelt
--   fid 12121133  Long Point Beach
--
-- DEVELOPED SWIM BEACH (not_allowed):
--   fid 16890883  Fort Spokane Swim Beach  — explicitly named developed swim beach
--
-- Notes:
-- - Per LARO policy, the four "on_leash" beaches MAY actually be developed
--   swim beach areas (curator review recommended). Conservative ship:
--   only Fort Spokane is explicitly named in the agent's research as a
--   developed swim beach → not_allowed.
-- - The other 4 beach names contain "Beach" / "Bay" but aren't explicitly
--   tagged as "developed swim beach areas" in the LARO Compendium text.

BEGIN;

-- 1. policy_source row (idempotent by source_url)
INSERT INTO public.policy_source (
    subtype, citation, issuing_agency_id, scope, source_url, full_text,
    last_verified, metadata
)
SELECT
    'agency_administrative_policy',
    'Lake Roosevelt NRA Pet Policy (NPS — on-leash on most park areas; prohibited in developed swim beach areas) — implements 36 CFR §2.15',
    307,  -- NPS
    ARRAY['dog_policy']::text[],
    'https://www.nps.gov/laro/planyourvisit/pets.htm',
    $$Lake Roosevelt National Recreation Area — Pet Policy

OPERATIVE RULE: Dogs are allowed in the park, except in developed swim beach areas, as long as they wear their leash. Dogs must wear a leash no more than 6 feet long at all times while in the park. Pet owners must clean up after their dogs (bags provided at visitor centers and parking areas). Animals cannot be left unattended in vehicles. Barking dogs disturbing the campground must be removed. Wildlife should not be chased or harassed.

Verbatim: "Dogs are allowed in the park, except in developed swim beach areas, as long as they wear their leash."

Citation: Lake Roosevelt NRA Pet Policy (NPS official page)
URL: https://www.nps.gov/laro/planyourvisit/pets.htm
Subtype: agency_administrative_policy (implements 36 CFR §2.15 baseline + adds developed-swim-beach prohibition)

ZONE-SPECIFIC application:
- Developed swim beach areas (e.g., Fort Spokane Swim Beach): dogs PROHIBITED
- Other shoreline / trails / campgrounds: dogs allowed on 6-ft leash
- Designated picnic areas: dogs prohibited
- Hawk Creek: identified as dog-friendly beach alternative

Per-beach interpretation needed to determine if a given PAD-US-tagged beach is a "developed swim beach area" or a non-swim shoreline area. Conservative classification: explicitly-named developed swim beaches → not_allowed; others → on_leash with status_note.
$$,
    NOW(),
    jsonb_build_object(
        'codify_source_curator', 'agent_dispatch_2026_05_18',
        'overrides_federal_baseline', '36 CFR §2.15 (ps id=1)',
        'multi_zone', true,
        'developed_swim_beaches_named', ARRAY['Fort Spokane Swim Beach'],
        'on_leash_default_with_curator_review', ARRAY['Apple Beach','Bradbury Beach','Crescent Bay On Lake Roosevelt','Long Point Beach']
    )
WHERE NOT EXISTS (
    SELECT 1 FROM public.policy_source
    WHERE source_url = 'https://www.nps.gov/laro/planyourvisit/pets.htm'
);

-- 2a. Fort Spokane Swim Beach → not_allowed
INSERT INTO public.beach_policy_source (
    beach_fid, policy_source_id, section, rule, operative_status,
    evidence_verbatim, evidence_url, status_note, extracted_at, last_verified
)
SELECT 16890883::bigint, ps.id, 'sand', 'not_allowed',
       'operative'::operative_status,
       'LARO Pet Policy: "Dogs are allowed in the park, EXCEPT in developed swim beach areas." Fort Spokane Swim Beach is an explicitly developed NPS swim beach → dogs prohibited.',
       ps.source_url,
       'Fort Spokane Swim Beach is a developed NPS swim area; per LARO policy, dogs prohibited. Service animals exempt per 36 CFR §2.15.',
       NOW(), NOW()
FROM public.policy_source ps
WHERE ps.source_url = 'https://www.nps.gov/laro/planyourvisit/pets.htm'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- 2b. Other 4 beaches → on_leash with curator-review status_note
INSERT INTO public.beach_policy_source (
    beach_fid, policy_source_id, section, rule, operative_status,
    evidence_verbatim, evidence_url, status_note, extracted_at, last_verified
)
SELECT v.fid, ps.id, 'sand', 'on_leash',
       'operative'::operative_status,
       'LARO Pet Policy: "Dogs are allowed in the park, except in developed swim beach areas, as long as they wear their leash." 6-ft leash maximum.',
       ps.source_url,
       'On-leash default per LARO Pet Policy. If this specific beach is a developed swim beach area per NPS designation, dogs are prohibited there — curator review recommended.',
       NOW(), NOW()
FROM (VALUES
    (17159836::bigint),  -- Apple Beach
    (15029295::bigint),  -- Bradbury Beach
    (16669115::bigint),  -- Crescent Bay On Lake Roosevelt
    (12121133::bigint)   -- Long Point Beach
) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.source_url = 'https://www.nps.gov/laro/planyourvisit/pets.htm'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- 3. Verification
SELECT (SELECT id FROM public.policy_source WHERE source_url='https://www.nps.gov/laro/planyourvisit/pets.htm') AS ps_id,
       (SELECT COUNT(*) FROM public.beach_policy_source bps JOIN public.policy_source ps ON ps.id=bps.policy_source_id WHERE ps.source_url='https://www.nps.gov/laro/planyourvisit/pets.htm' AND bps.rule='on_leash') AS n_on_leash,
       (SELECT COUNT(*) FROM public.beach_policy_source bps JOIN public.policy_source ps ON ps.id=bps.policy_source_id WHERE ps.source_url='https://www.nps.gov/laro/planyourvisit/pets.htm' AND bps.rule='not_allowed') AS n_not_allowed;

COMMIT;
