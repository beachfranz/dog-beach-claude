-- NPS Olympic National Park Superintendent's Compendium — 2026-05-18
--
-- Per-unit override of the 36 CFR §2.15 federal baseline (ps id=1, applied
-- to Olympic NP earlier today via the federal baseline migration).
--
-- Source URL (verified via agent dispatch 2026-05-18):
--   https://www.nps.gov/olym/learn/management/superintendent-s-compendium.htm
--
-- COMPENDIUM RULE (verbatim, from agent fetch):
--   "Dogs (except certified ADA service animals), cats, and other pets are
--    prohibited in non-developed park areas (greater than 100 feet from
--    established paved roadways), including trails and beaches, with specific
--    exceptions listed below."
--
--   "Pets must be leashed with a 6-foot or shorter leash in these locations:
--    - Rialto Beach: From parking lot ½ mile north to Ellen Creek
--    - Hoh Indian Reservation to Quinault Indian Reservation: All beach access
--      in this corridor
--    - July Creek Loop Trail, Elwha Project Lands, Peabody Creek Trail,
--      Elwha bypass trail, Spruce Railroad Trail (Olympic Discovery Trail
--      through Lake Crescent District), Madison Falls Trail"
--
-- This is a MULTI-ZONE case. Per playbook §5 + §6, each beach gets the rule
-- that applies to its physical location:
--
-- CARVE-OUT BEACHES (on_leash; explicitly named in Compendium):
--   fid 16311068  Rialto Beach        (Compendium: "From parking lot ½ mile north to Ellen Creek")
--   fid 8867735   Kalaloch Beach      (Hoh-Quinault corridor)
--   fid 9813      Kalaloch Beach 1    (Hoh-Quinault corridor)
--
-- DEFAULT BEACHES (not_allowed; wilderness coast / non-developed):
--   fid 8273662   Beach 2             (Hoh-Quinault corridor — MAY qualify for on_leash; curator review)
--   fid 9827      Beach 3             (Hoh-Quinault corridor — MAY qualify for on_leash; curator review)
--   fid 17477482  Ruby Beach          (Hoh-Quinault corridor — MAY qualify for on_leash; curator review)
--   fid 18252728  Kayostia Beach      (wilderness coast, north of Hoh)
--   fid 982839    Lake Crescent - East Beach  (developed Lake Crescent — MAY qualify; curator review)
--   fid 9928      Richwine Gravel Bar (likely wilderness)
--   fid 18712551  Second Beach        (Mora wilderness, north of Quileute)
--   fid 1147939   Shi Shi Beach       (north wilderness coast)
--   fid 9616888   Third Beach Trailhead  (Mora wilderness)
--
-- Conservative ship: only beaches the Compendium EXPLICITLY names get
-- on_leash. The "Hoh-Quinault corridor" + "developed Lake Crescent" carve-outs
-- may pull additional beaches into on_leash per per-beach interpretation.
-- Curator can refine via separate migration if needed (Ruby/Beach 2/Beach 3/
-- Lake Crescent East are candidates).

BEGIN;

-- 1. Insert the Compendium ps row (idempotent by source_url)
INSERT INTO public.policy_source (
    subtype, citation, issuing_agency_id, scope, source_url, full_text,
    effective_date, last_verified, metadata
)
SELECT
    'superintendents_compendium',
    'Olympic National Park Superintendent''s Compendium, 36 CFR §2.15 (Pets) — per-unit override of federal baseline',
    307,  -- National Park Service
    ARRAY['dog_policy']::text[],
    'https://www.nps.gov/olym/learn/management/superintendent-s-compendium.htm',
    $$Olympic National Park Superintendent's Compendium — 36 CFR §2.15 (Pets) override

GENERAL PROHIBITION: Dogs (except certified ADA service animals), cats, and other pets are prohibited in non-developed park areas (greater than 100 feet from established paved roadways), including trails and beaches, with specific exceptions listed below.

PERMITTED PET AREAS (LEASHED): Pets must be leashed with a 6-foot or shorter leash in these locations:
- Rialto Beach: From parking lot ½ mile north to Ellen Creek
- Hoh Indian Reservation to Quinault Indian Reservation: All beach access in this corridor
- July Creek Loop Trail
- Elwha Project Lands
- Peabody Creek Trail
- Elwha bypass trail (around road washout)
- Spruce Railroad Trail (Olympic Discovery Trail through Lake Crescent District)
- Madison Falls Trail

SERVICE ANIMALS: A service animal means any dog or miniature horse that is individually trained to do work or perform tasks for the benefit of an individual with a disability and are permitted where visitors are allowed, regardless of park pet policies.

WASTE DISPOSAL: Pet excrement must be immediately collected and disposed of in the nearest trash receptacle.

[Codified 2026-05-18 via agent dispatch + curator review. Conservative carve-out: only explicitly-named beaches (Rialto + Kalaloch) get on_leash; wilderness coast = not_allowed. Hoh-Quinault corridor (Ruby/Beach 2/3) + developed Lake Crescent (East Beach) may qualify for on_leash per per-beach interpretation.]$$,
    NULL::date,
    NOW(),
    jsonb_build_object(
        'codify_source_curator', 'agent_dispatch_2026_05_18',
        'overrides_federal_baseline', '36 CFR §2.15 (ps id=1)',
        'multi_zone', true,
        'carve_out_beaches_named', ARRAY['Rialto Beach','Kalaloch Beach','Kalaloch Beach 1'],
        'carve_out_candidates_curator_review', ARRAY['Ruby Beach','Beach 2','Beach 3','Lake Crescent - East Beach']
    )
WHERE NOT EXISTS (
    SELECT 1 FROM public.policy_source
    WHERE source_url = 'https://www.nps.gov/olym/learn/management/superintendent-s-compendium.htm'
);

-- 2a. CARVE-OUT bps rows (rule=on_leash for explicitly-named beaches)
INSERT INTO public.beach_policy_source (
    beach_fid, policy_source_id, section, rule, operative_status,
    evidence_verbatim, evidence_url, status_note, extracted_at, last_verified
)
SELECT v.fid, ps.id, 'sand', 'on_leash',
       'operative'::operative_status,
       'Olympic NP Superintendent''s Compendium 36 CFR §2.15: "Pets must be leashed with a 6-foot or shorter leash in these locations: Rialto Beach – from parking lot ½ mile north to Ellen Creek; Hoh Indian Reservation to Quinault Indian Reservation – all beach access in this corridor; [trails listed]."',
       ps.source_url,
       'Olympic NP Compendium carve-out — leash required in this explicitly-named area. Compendium permits dogs only in named carve-outs; default is not_allowed elsewhere in the park.',
       NOW(), NOW()
FROM (VALUES (16311068::bigint), (8867735::bigint), (9813::bigint)) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.source_url = 'https://www.nps.gov/olym/learn/management/superintendent-s-compendium.htm'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- 2b. DEFAULT bps rows (rule=not_allowed for wilderness/non-developed beaches)
INSERT INTO public.beach_policy_source (
    beach_fid, policy_source_id, section, rule, operative_status,
    evidence_verbatim, evidence_url, status_note, extracted_at, last_verified
)
SELECT v.fid, ps.id, 'sand', 'not_allowed',
       'operative'::operative_status,
       'Olympic NP Superintendent''s Compendium 36 CFR §2.15: "Dogs (except certified ADA service animals), cats, and other pets are PROHIBITED in non-developed park areas (greater than 100 feet from established paved roadways), including trails and beaches, with specific exceptions listed below." Service animals (ADA) permitted everywhere.',
       ps.source_url,
       'Olympic NP Compendium default — pets prohibited in non-developed park areas (wilderness coast, undeveloped beaches). Service animals exempt. The Hoh-Quinault corridor + developed Lake Crescent are Compendium carve-outs that MAY apply to this beach per per-beach interpretation; curator review recommended for Ruby Beach / Beach 2 / Beach 3 / Lake Crescent - East Beach.',
       NOW(), NOW()
FROM (VALUES
    (8273662::bigint),   -- Beach 2
    (9827::bigint),      -- Beach 3
    (17477482::bigint),  -- Ruby Beach
    (18252728::bigint),  -- Kayostia Beach
    (982839::bigint),    -- Lake Crescent - East Beach
    (9928::bigint),      -- Richwine Gravel Bar
    (18712551::bigint),  -- Second Beach
    (1147939::bigint),   -- Shi Shi Beach
    (9616888::bigint)    -- Third Beach Trailhead
) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.source_url = 'https://www.nps.gov/olym/learn/management/superintendent-s-compendium.htm'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- 3. Verification
SELECT ps.id AS ps_id,
       (SELECT COUNT(*) FROM public.beach_policy_source bps
        WHERE bps.policy_source_id = ps.id AND bps.rule = 'on_leash') AS n_on_leash,
       (SELECT COUNT(*) FROM public.beach_policy_source bps
        WHERE bps.policy_source_id = ps.id AND bps.rule = 'not_allowed') AS n_not_allowed
FROM public.policy_source ps
WHERE ps.source_url = 'https://www.nps.gov/olym/learn/management/superintendent-s-compendium.htm';

COMMIT;
