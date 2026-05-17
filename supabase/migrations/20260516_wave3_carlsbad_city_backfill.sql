-- Wave 3: City of Carlsbad — 6 beaches with per-location rule
-- (ocean = not_allowed; Agua Hedionda Lagoon = off_leash/on_leash).
--
-- Source: Carlsbad Municipal Code §11.32 Parks and Beaches.
-- URL: https://ecode360.com/44007383
-- Also accessible via qcode.us/codes/carlsbad/ (General Code platform).
--
-- Operative rules:
--
-- §11.32.020 Beaches—Scope (verbatim):
--   "For the purpose of this chapter, beaches shall include all beach
--   areas bordering the Pacific Ocean, the Batiquitos Lagoon, the
--   Buena Vista Lagoon and the Agua Hedionda Lagoon."
--
-- §11.32.030(11) Unlawful acts — animal prohibition (verbatim):
--   "To ride or lead horses, or to hitch, fasten, lead, drive or let
--   loose any animal or fowl of any kind. Dogs are not allowed in
--   Carlsbad's parks or on Carlsbad's beaches.
--   a. Notwithstanding any other provisions of this chapter, this
--   section does not apply to Batiquitos Lagoon, Buena Vista Lagoon or
--   Agua Hedionda Lagoon.
--   b. This prohibition does not apply to a dog accompanying an
--   unsighted person, or other person who by reason of medical
--   necessity must be accompanied by a dog, dogs while assisting peace
--   officers in law enforcement duties, dog parks or other areas
--   specifically designated for dog use by the City Council, or to
--   dogs participating in shows or obedience classes authorized by the
--   Carlsbad community services department on specified areas of parks
--   or beaches."
--
-- 6 Carlsbad-city-polygon beaches, per-beach mapping:
--
--   Pacific Ocean beaches (NOT_ALLOWED per §11.32.030(11)):
--     fid 8775 Carlsbad City Beach
--     fid 8282 Ponto Beach
--     fid 8281 South Ponto
--     fid 8877 Tamarack Beach
--
--   Agua Hedionda Lagoon (EXEMPT from §11.32.030(11) per (11)(a);
--   citywide leash law still applies for general lagoon access):
--     fid 6622 Carlsbad Lagoon Dog Beach — designated dog beach (the
--              name confirms it's a §11.32.030(11)(b) "area specifically
--              designated for dog use by the City Council") → off_leash
--     fid 8608 Warm Waters Jetty Beach — Agua Hedionda Lagoon, not
--              named as a dog beach in the code; exempt from prohibition
--              but no specific designation → on_leash
--
-- All 6 already have existing policy_source links (from CA DPR / SD
-- County); these city-policy links supplement and refine.

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Carlsbad Municipal Code §11.32.030(11) (Dogs prohibited on parks/beaches) + §11.32.020 (Beaches—Scope; includes Pacific Ocean and three lagoons)',
       (SELECT id FROM public.agency WHERE name='Carlsbad' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/44007383',
       'Carlsbad Municipal Code Chapter 11.32 PARKS AND BEACHES. §11.32.020 Beaches—Scope: For the purpose of this chapter, beaches shall include all beach areas bordering the Pacific Ocean, the Batiquitos Lagoon, the Buena Vista Lagoon and the Agua Hedionda Lagoon. §11.32.030(11) Unlawful acts (animals): Dogs are not allowed in Carlsbad''s parks or on Carlsbad''s beaches. (a) Notwithstanding any other provisions of this chapter, this section does not apply to Batiquitos Lagoon, Buena Vista Lagoon or Agua Hedionda Lagoon. (b) This prohibition does not apply to a dog accompanying an unsighted person, or other person who by reason of medical necessity must be accompanied by a dog, dogs while assisting peace officers in law enforcement duties, dog parks or other areas specifically designated for dog use by the City Council, or to dogs participating in shows or obedience classes authorized by the Carlsbad community services department on specified areas of parks or beaches. Ord. 3222 (1987); Ord. NS-894 (2008); Ord. CS-022 (2009); Ord. CS-473 (8/20/2024). [Hosted on ecode360 (qcode/General Code) at ecode360.com/44007383. Also accessible via qcode.us/codes/carlsbad/. Fetched verbatim via Playwright 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Carlsbad Municipal Code §11.32.030%'
);

-- ─── 2. Pacific Ocean beaches → not_allowed ─────────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'Carlsbad Municipal Code §11.32.030(11): "Dogs are not allowed in Carlsbad''s parks or on Carlsbad''s beaches." Applies to Pacific Ocean beaches (lagoon exemption in (11)(a) does not apply).',
       ps.source_url, NULL
FROM (values (8775),(8282),(8281),(8877)) as v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Carlsbad Municipal Code §11.32.030%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 3. Carlsbad Lagoon Dog Beach (designated dog use) → off_leash ──

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 6622, ps.id, 'sand', 'off_leash', 'operative'::operative_status,
       'Carlsbad Lagoon Dog Beach is at Agua Hedionda Lagoon — EXEMPT from §11.32.030(11) per (11)(a). The name "Dog Beach" + dedicated location confirms §11.32.030(11)(b) "area specifically designated for dog use by the City Council."',
       ps.source_url,
       'Verify off-leash rule via on-site signage during walkthrough; some designated dog beaches require leash.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Carlsbad Municipal Code §11.32.030%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 4. Warm Waters Jetty Beach (Agua Hedionda, not named) → on_leash ──

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8608, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Warm Waters Jetty Beach is at Agua Hedionda Lagoon — EXEMPT from §11.32.030(11) prohibition per (11)(a). Not named as a designated dog beach, so default leash law applies (citywide leash requirements).',
       ps.source_url,
       'Lagoon area beach; not a city-designated dog beach. Walkthrough recommended to confirm lagoon access policy and any on-site signage.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Carlsbad Municipal Code §11.32.030%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
