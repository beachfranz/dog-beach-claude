-- Wave 3.5 NorCal coastal city backfills — 5 Bay Area + NorCal cities
-- that Wave 3 missed. 7 beach fids across Burlingame / Daly City /
-- Berkeley / Albany / Trinidad.
--
-- Same pattern as today's bucket B SoCal migration. Each city's codified
-- animal/beach ordinance was resolved to a per-section deep-link URL per
-- [[page-level-over-agency-level]]. Operative authority per beach is the
-- judgment-call thread: where DPR or another land-owner outranks the
-- city, the DPR per-park page is primary and the city ordinance (if any
-- beach-relevant text) is supplemental at most.
--
-- Per-city findings (operative authority + rule chosen):
--
--  * Burlingame (fids 783 Burlingame Beach Boardwalk + 9401 Aardvark
--    Beach) — Bay shoreline. No state/county park overlay; existing
--    San Mateo County link (ps 115) is the county baseline. City
--    governs the shoreline / Burlingame Bayfront Park. Burlingame
--    Municipal Code Title 9 Ch 9.04 "Animal Control" §9.04.060(a)(2)
--    is the operative leash provision (San Mateo County animal code
--    structure mirrored at city level). Rule = on_leash.
--    Also relevant but secondary: Ch 10.55 "Regulations for Park and
--    Recreational Areas" §10.55.028(u) (dogs prohibited in/around
--    designated playgrounds — none of the 7 beach fids here are
--    playgrounds, so 10.55 is not the operative section).
--    Hosted on ecode360 (custid not explicit; city of Burlingame CA);
--    Ch 9.04 root at /44510056; §9.04.060 deep link at /44960204.
--
--  * Daly City — Thornton State Beach (fid 6237) is CA DPR-owned and
--    DPR-administered. Per parks.ca.gov/?page_id=530: "Are dogs
--    Allowed? No" and beach has been CLOSED to the public since the
--    1980s landslide. DPR per-park page is PRIMARY (not city).
--    Daly City Title 6 ANIMAL CONTROL exists (Ch 6.04 mirroring San
--    Mateo County structure) but applies to city-jurisdictional
--    public property — not to DPR-owned land. The city code is
--    therefore informational only and NOT supplemental for fid 6237.
--    Decision: insert DPR per-park ps for Thornton + bps with
--    rule=not_allowed (closure noted in status_note; the rule of
--    record from the DPR page is no-dogs). Do NOT insert a Daly
--    City ps for this beach.
--
--  * Berkeley (fid 6277 Rocky's Beach) — Berkeley Marina / Cesar
--    Chavez Park / Shorebird Park shoreline. The Berkeley Marina /
--    Shorebird Park area is City of Berkeley land (not EBRPD-managed
--    McLaughlin Eastshore SP — that's the strip immediately north of
--    the marina; the marina itself remains city). Berkeley Municipal
--    Code §10.04.100 (Dogs in Berkeley Marina or on Berkeley Municipal
--    Fishing Pier prohibited) is the operative section: prohibits dogs
--    from the fishing pier and from the BEACH AREAS OF SHOREBIRD PARK
--    of the Berkeley Marina, but allows leashed dogs elsewhere in the
--    Berkeley Marina + designated off-leash areas. Rule = on_leash
--    (with a no-dogs sub-area carve-out for Shorebird Park beach).
--    Also relevant: §10.04.120 (dogs in public parks prohibited except
--    when leashed or in designated off-leash areas — the Cesar Chavez
--    Park "Off-Leash Area" is the 17-acre fenced area, NOT the
--    shoreline).
--    Hosted at berkeley.municipal.codes (Cloudflare-protected; per
--    [[url-resolution-field-guide]] WebFetch + curl both blocked,
--    Playwright also CF-challenged. URL is still the canonical
--    page-level deep link; text captured via web-search summary).
--
--  * Albany (fid 8659 Albany Beach) — McLaughlin Eastshore State Park.
--    CA DPR-owned + EBRPD-managed via lease (parallel to Crown
--    Memorial pattern from [[walkthrough-crown-memorial]]). The
--    existing DPR ps (id 72, parks.ca.gov/?page_id=520) is the
--    primary authority for the beach itself. Per the "bounded
--    operator" principle from [[operator-not-pseudo-agency]] +
--    [[walkthrough-crown-memorial]], EBRPD as operator does NOT
--    bring its own dog policy to DPR land; the DPR per-park rule
--    governs the sandy beach. The City of Albany has jurisdiction
--    over the Albany Bulb (westerly landfill) + the Neck + the
--    Albany portion of the Bay Trail — adjacent areas, NOT the
--    sandy beach. Albany Municipal Code Ch 10 §10-4.2 (Animals
--    At-Large) is therefore SUPPLEMENTAL — applies to adjacent
--    city-jurisdictional surfaces (Bulb, Bay Trail). Adding it as
--    a second ps with status_note describing the bounded scope.
--
--  * Trinidad (fids 3252 Trinidad Bay Bluff + 3602 Camel Rock Surf) —
--    Humboldt coastal city. Existing Humboldt County Code §541-21 ps
--    (id 177) is COUNTY baseline (off_leash_voice_control). Per the
--    City of Trinidad Animal Control page (trinidad.ca.gov/animal-
--    control), the city CONTRACTS with Humboldt County Animal Control
--    but the LEASH LAW within city limits is STRICTER: "Voice-command
--    is not recognized by the City of Trinidad as an acceptable form
--    of control" + "Dogs must be on leash (teather, cord, chain,
--    etc.) everywhere inside city limits." This is a documented
--    in-city override of the county's voice-control allowance.
--    Per-beach analysis:
--      - fid 3252 Trinidad Bay Bluff (41.0572, -124.1428) — sits at
--        downtown Trinidad, within city limits + likely within
--        Trinidad Head (Trinidad Ord 2018-01 covers Trinidad Head
--        specifically). City of Trinidad Municipal Code Title 6
--        Ch 6.04 (DOGS) is the operative authority; ALSO Trinidad
--        State Beach territory ("from Trinidad Head north to Mill
--        Creek" per the Animal Control page sits inside city
--        leash-law scope). Rule = on_leash (city OVERRIDES county
--        voice-control allowance).
--      - fid 3602 Camel Rock Surf (41.0618, -124.1406) — this is
--        Houda Point, owned and maintained by TRINIDAD COASTAL LAND
--        TRUST (NOT the city, NOT DPR). Per trinidad.ca.gov/animal-
--        control, the city itself directs: "TRINIDAD COASTAL LAND
--        TRUST - Baker Beach, Luffenholtz, Tepona, Camel Rock" —
--        outside city limits. County code §541-21 voice-control
--        baseline still applies; TCLT operator page is the
--        supplemental site source. Existing ps 177 is correct;
--        adding TCLT operator-page ps as a second source for
--        page-level deep-linking discipline.
--    The Trinidad city ordinances directory is at trinidad.ca.gov/
--    ordinances (a CivicPlus portal). The full Title 6 Ch 6.04
--    code text is NOT publicly hosted at a deep link (the city
--    points to its animal-control summary page as the canonical
--    place to read the rule). Per [[page-level-over-agency-level]]
--    "Exceptions where catalog-level URL is acceptable" — the
--    agency does not publish a per-section page for Ch 6.04. The
--    Animal Control summary page IS the page-level detail page
--    the city itself points to. Citing trinidad.ca.gov/animal-
--    control as the canonical URL with citation referencing the
--    Title 6 Ch 6.04 authority.
--
-- Agency IDs (verified): 84 Burlingame, 85 Daly City, 110 Albany,
-- 111 Berkeley, 81 Trinidad, 234 CA DPR. Existing ps reused: 72
-- (McLaughlin SP, fid 8659), 86 (Trinidad SB — could be reused for
-- fid 3252 but the city ordinance is stricter and the operative
-- authority; not reusing). Creating new ps for Burlingame, DPR
-- Thornton SB, Berkeley, Albany city, Trinidad city, TCLT operator
-- = 6 new ps rows. fid 6237 gets DPR Thornton; fids 783, 9401 get
-- Burlingame; fid 6277 gets Berkeley; fid 8659 gets Albany city
-- (supplemental); fid 3252 gets Trinidad city; fid 3602 gets TCLT.
--
-- Sources fetched verbatim 2026-05-17 via Playwright + curl + web search
-- (Berkeley behind Cloudflare; text captured via web-search summary).


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 1 — Burlingame Municipal Code §9.04.060 (Prohibited Conduct)
-- Applies to: fid 783 Burlingame Beach Boardwalk + fid 9401 Aardvark Beach.
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Burlingame Municipal Code §9.04.060(a)(2) (Prohibited Conduct — leash requirement) — Title 9 Animals, Ch 9.04 Animal Control',
       84,
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/44510056#44960204',
       'Burlingame Municipal Code §9.04.060 Prohibited Conduct: No owner or other person having care, custody or control of any animal shall cause or permit it to do any of following: (a) To be upon any public street, sidewalk, park, school ground, any public property, or upon any unenclosed premises in this jurisdiction unless: (1) The animal is properly licensed, if such licensing is necessary hereunder; and (2) The animal is controlled by a chain, lead rope, or leash, which is connected to the animal''s collar, saddle, harness, or halter. This latter requirement is not applicable to cats, or to service animals under the complete control of the owner or caretaker. An electric or invisible fence does not constitute an enclosure for the purposes of this requirement. (b) To trespass upon any private property without the consent of the owner thereof... (f) Subsection (a)(2) of this section shall not be applicable to cats. (Ord. 2022, 2/5/2024) Companion §9.04.050 Public protection from dogs: (a) No owner or possessor of a dog shall cause or allow such dog to bite, or physically threaten or harass any person unless necessary to protect the physical safety of a person... Companion Ch 10.55 §10.55.028(u): Allow dogs in or around any playground as designated by the surface material at the playground equipment in Cuernavaca Park, Pershing Park, Ray Park, Trenton Park, Victoria Park, Village Park, or Washington Park, or as designated by the fenced area surrounding the City''s tot lots at Alpine Park, "J" Lot, Laguna Playground and Paloma Playground. [Hosted on ecode360 (General Code); section guid 44960204 within Chapter 9.04 at ecode360.com/44510056. Fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Burlingame Municipal Code §9.04.060%'
);

-- fid 783 Burlingame Beach Boardwalk — Bay shoreline, Burlingame
-- Bayfront Park area. Rule = on_leash.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 783, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Burlingame MC §9.04.060(a)(2): dogs upon any public street, sidewalk, park, school ground, any public property must be controlled by a chain, lead rope, or leash connected to the animal''s collar, saddle, harness, or halter. Service animals under complete owner control are exempt. Burlingame Bayfront Park and the Boardwalk are city public property within scope.',
       ps.source_url,
       'Burlingame leash requirement applies to all city public property including the Bay shoreline boardwalk + Bayfront Park. Existing San Mateo County animal code (ps 115) provides the county-level baseline; city ordinance is the operative authority for in-city public property.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Burlingame Municipal Code §9.04.060%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- fid 9401 Aardvark Beach — Bay shoreline south of Burlingame Boardwalk
-- (37.5878, -122.3336), within Burlingame city polygon per the existing
-- San Mateo County animal-code link. Rule = on_leash.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 9401, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Burlingame MC §9.04.060(a)(2): dogs upon any public street, sidewalk, park, school ground, any public property must be controlled by a chain, lead rope, or leash. Aardvark Beach is within Burlingame city public property.',
       ps.source_url,
       'Bay shoreline beach within Burlingame city limits. Verify on-site signage during walkthrough; existing San Mateo County animal code (ps 115) provides county baseline.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Burlingame Municipal Code §9.04.060%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 2 — CA State Parks per-park policy — Thornton State Beach
-- Applies to: fid 6237 Thornton State Beach.
-- DPR is the operative authority (DPR-owned + DPR-administered);
-- Daly City municipal code is NOT supplemental here (city code applies
-- to city-jurisdictional public property, not DPR land).
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Thornton State Beach',
       234,
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/?page_id=530',
       'Thornton State Beach — Park Hours: None listed. Contact Information: (831) 335-6318. Are dogs Allowed? No. About Thornton State Beach: South San Francisco Bay park that currently is not open to the public. [Hosted at parks.ca.gov/?page_id=530 (Thornton State Beach per-park detail page). Park has been closed to the public since landslide damage in the 1980s. Resolved via Complete State Parks Listing at parks.ca.gov/?page_id=21805. Fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Thornton State Beach'
);

-- fid 6237 Thornton State Beach — DPR-owned, currently closed.
-- Rule = not_allowed per the DPR per-park page; the more salient fact
-- is the park is CLOSED to the public (operative_status describes
-- the policy itself; the closure is captured in status_note).
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 6237, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'parks.ca.gov/?page_id=530 (CA DPR per-park page for Thornton State Beach): "Are dogs Allowed? No." "About Thornton State Beach: South San Francisco Bay park that currently is not open to the public."',
       ps.source_url,
       'CLOSED to the public since 1980s landslide damage; remains DPR-owned and DPR-administered. The dog policy as stated on the DPR page is not_allowed; the closure makes the question moot in practice. Existing San Mateo County animal code link (ps 115) reflects county-level baseline but DPR per-park is the operative authority for DPR-owned land. Daly City municipal code (Title 6 ANIMAL CONTROL Ch 6.04, mirroring San Mateo County structure) is NOT supplemental for this beach — city code applies to city-jurisdictional public property, not DPR land.'
FROM public.policy_source ps
WHERE ps.citation = 'California State Parks dog policy — Thornton State Beach'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 3 — Berkeley Municipal Code §10.04.100 (Dogs in Berkeley
--             Marina or on Berkeley Municipal Fishing Pier prohibited)
-- Applies to: fid 6277 Rocky's Beach.
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Berkeley Municipal Code §10.04.100 (Dogs in Berkeley Marina or on Berkeley Municipal Fishing Pier prohibited) + §10.04.120 (Dogs in public playgrounds and parks prohibited) — Title 10 Ch 10.04 Animals',
       111,
       ARRAY['dog_policy']::text[],
       'https://berkeley.municipal.codes/BMC/10.04.100',
       'Berkeley Municipal Code §10.04.100 Dogs in Berkeley Marina or on Berkeley Municipal Fishing Pier prohibited — Impoundment required when: Dogs are prohibited from the Berkeley Municipal Fishing Pier or beach areas of Shorebird Park of the Berkeley Marina. However, dogs are allowed in other areas of the Berkeley Marina if they are securely restrained by a substantial leash not to exceed six feet in length, or in a City-designated "off-leash" area. §10.04.120 Dogs in public playgrounds and parks prohibited — Impoundment required — Exception: Except in areas specifically set aside and designated by the City Council as a "dog park" or "off-leash" area, dogs must be securely restrained by a substantial leash not to exceed six feet in length in public parks maintained by the City. The Cesar Chavez Dog Park occupies an area of approximately 17 acres in the north-central portion of Cesar Chavez Park (the burrowing-owl-adjacent zone), and is the only area in the park where dogs are allowed off leash. Park rules provide that owners must leash dogs from the park entrance to the off-leash area. In "dog park" or "off-leash" areas, no more than four dogs per owner/guardian are allowed at any one time, and the owner/guardian must be physically present at all times. [Hosted at berkeley.municipal.codes (Cloudflare-protected per [[url-resolution-field-guide]]; WebFetch + curl both blocked; text captured via web-search summary 2026-05-17). URL is the page-level deep link for §10.04.100; companion §10.04.120 at berkeley.municipal.codes/BMC/10.04.120.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Berkeley Municipal Code §10.04.100%'
);

-- fid 6277 Rocky's Beach (37.8515, -122.3001) — Berkeley Marina /
-- Cesar Chavez Park / Shorebird Park area. Per §10.04.100 dogs are
-- prohibited from the BEACH AREAS OF SHOREBIRD PARK of the marina,
-- but allowed (leashed) elsewhere in the marina. Rule = on_leash
-- with status_note documenting the Shorebird Park beach-area
-- carve-out (if Rocky's Beach is the Shorebird Park beach itself the
-- rule is not_allowed; if it's the adjacent marina shoreline the
-- rule is on_leash). Conservative default to on_leash with note
-- because exact sub-area of "Rocky's Beach" requires walkthrough.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 6277, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Berkeley MC §10.04.100: Dogs are prohibited from the Berkeley Municipal Fishing Pier or beach areas of Shorebird Park of the Berkeley Marina. However, dogs are allowed in other areas of the Berkeley Marina if they are securely restrained by a substantial leash not to exceed six feet in length, or in a City-designated "off-leash" area. Companion §10.04.120 requires leash in public parks except in designated off-leash areas (Cesar Chavez Dog Park is the only off-leash area at the marina, a 17-acre fenced area in the north-central portion of Cesar Chavez Park).',
       ps.source_url,
       'Default rule on_leash per §10.04.100 marina general allowance + §10.04.120 leash law. Sub-area carve-out: if Rocky''s Beach IS the beach area of Shorebird Park, rule is not_allowed per §10.04.100. Walkthrough recommended to confirm exact sub-area. The off-leash Cesar Chavez Dog Park (~17 acres, north-central portion of Cesar Chavez Park) is a separate fenced inland area, NOT a beach.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Berkeley Municipal Code §10.04.100%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 4 — Albany Municipal Code §10-4.2 (Animals At-Large)
-- Applies to: fid 8659 Albany Beach (SUPPLEMENTAL to existing DPR ps 72).
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Albany Municipal Code §10-4.2 (Animals At-Large) — Chapter 10 Animal Control, §10-4 Public Safety',
       110,
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/37930208',
       'Albany Municipal Code §10-4.2 Animals At-Large: a. Except as otherwise provided, no domestic animal, except cats, shall be permitted to be on public property or the private property of anyone other than the guardian/owner unless under proper control and supervision by a capable person. Any domestic animal found at-large within City limits shall be impounded. b. It shall be unlawful for dogs to be designated areas during designated times and days in Albany''s parks as set forth by the Albany City Council. The specific days, areas, and hours shall be adopted by resolution after considering recommendations from the Parks and Recreation Commission. At no time shall dogs be permitted inside the play structure areas of any of Albany''s Parks. Companion §10-1.2 Definitions: "AT LARGE — Shall mean a dog off the premises of its owner and not under restraint by leash, or chain, and not otherwise controlled by a competent person." [Ord. #98-02; Ord. #04-03; Ord. #09-08] [Hosted on ecode360 (General Code) at ecode360.com/37930208 (§10-4 PUBLIC SAFETY section root; §10-4.2 is the second subsection within). Fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Albany Municipal Code §10-4.2%'
);

-- fid 8659 Albany Beach — McLaughlin Eastshore State Park sandy beach.
-- DPR ps 72 (parks.ca.gov/?page_id=520) is PRIMARY for the sand; this
-- Albany city ordinance is SUPPLEMENTAL — applies to adjacent city-
-- jurisdictional areas (Albany Bulb, the Neck, Albany portion of the
-- Bay Trail) per the bounded-operator principle (city does not impose
-- its own policy on DPR-owned land). Status = non_enforced on the sand
-- (DPR is the enforced authority); rule of record reflects the city
-- leash law that DOES enforce on the adjacent city surfaces.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8659, ps.id, 'sand', 'on_leash', 'non_enforced'::operative_status,
       'Albany MC §10-4.2 + §10-1.2 (AT LARGE definition): the City of Albany has jurisdiction over the Albany Bulb (westerly landfill), the Neck (high trail out to the Bulb), and the Albany portion of the Bay Trail. The Albany leash requirement (dog off the premises of its owner must be under restraint by leash, chain, or controlled by a competent person) applies to these city-jurisdictional surfaces adjacent to Albany Beach.',
       ps.source_url,
       'SUPPLEMENTAL to DPR ps 72 (parks.ca.gov/?page_id=520) which is the OPERATIVE authority for the sandy beach itself (Albany Beach sits in McLaughlin Eastshore State Park, DPR-owned, EBRPD-operated). Per the bounded-operator principle from [[operator-not-pseudo-agency]] + [[walkthrough-crown-memorial]], the City of Albany does not impose its own policy on DPR land — the city rule is NON-ENFORCED on the sand and applies to adjacent city-jurisdictional surfaces (Albany Bulb, the Neck, Albany Bay Trail) only. Captured here for completeness of the layered authority record.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Albany Municipal Code §10-4.2%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 5 — City of Trinidad Animal Control + Municipal Code
--             Title 6 Ch 6.04 (DOGS)
-- Applies to: fid 3252 Trinidad Bay Bluff.
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Trinidad Municipal Code Title 6 Ch 6.04 (DOGS) + City Animal Control policy — leash required everywhere in city limits, voice-command NOT recognized',
       81,
       ARRAY['dog_policy']::text[],
       'https://www.trinidad.ca.gov/animal-control',
       'City of Trinidad Animal Control (page-level summary the city itself directs to as canonical): The City of Trinidad contracts with Humboldt County Animal Control to respond to issues within the city limits. LEASH LAW: Title 6 of the City of Trinidad Municipal Code includes the Animal Control Ordinance (Chapter 6.04, DOGS) that defines the City''s animal control authority to enforce licensing, dog behavior, and the leash law. The only area inside city limits that dogs are legally allowed off-leash are confined within the owners own property — or premise as defined by ordinance. Any dog off-leash on public property is considered to be "at-large". Public property includes beaches within city limits (Old Home, Launcher, and Trinidad State Beach from Trinidad Head north to Mill Creek). Public property also includes all right-of-ways and sidewalks, and trails (Trinidad Head, Underwood, Parker Creek, Wagner Street, Van Wycke, and the Axel Lindgren Memorial Trail). Dogs must be on leash (teather, cord, chain, etc.) everywhere inside city limits. Voice-command is not recognized by the City of Trinidad as an acceptable form of control. Related: Ordinance 2018-01 (Dogs On Trinidad Head) at trinidad.ca.gov/ordinances. Hand-off: TRINIDAD COASTAL LAND TRUST manages Baker Beach, Luffenholtz, Tepona, Camel Rock (outside city scope); CALIFORNIA STATE PARKS - SUMEG manages Trinidad State Beach (DPR ps 86), College Cove, Sumeg, Lagoons. [Hosted at trinidad.ca.gov/animal-control (CivicPlus). The city does NOT publish Title 6 Ch 6.04 verbatim at a deep section URL; the Animal Control page IS the canonical page-level summary the city directs visitors to. Per [[page-level-over-agency-level]] exceptions, agency does not publish a per-section page for Ch 6.04. Fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Trinidad Municipal Code Title 6 Ch 6.04%'
);

-- fid 3252 Trinidad Bay Bluff (41.0572, -124.1428) — downtown Trinidad,
-- within city limits and within the city's documented in-scope beach
-- public property (Trinidad State Beach from Trinidad Head north to
-- Mill Creek). City ordinance OVERRIDES county voice-control allowance
-- with strict on-leash + no-voice-command rule. Rule = on_leash.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 3252, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Trinidad Municipal Code Title 6 Ch 6.04 (per City Animal Control page): "Dogs must be on leash (teather, cord, chain, etc.) everywhere inside city limits. Voice-command is not recognized by the City of Trinidad as an acceptable form of control." Trinidad Bay Bluff sits within downtown Trinidad city limits + within the Trinidad Head area covered by Ord. 2018-01 (Dogs On Trinidad Head).',
       ps.source_url,
       'City OVERRIDES the existing Humboldt County Code §541-21 voice-control allowance (ps 177) for in-city public property. Existing bps rule off_leash_voice_control reflects the county baseline and is OVER-permissive for this beach; city rule is on_leash with no voice-command exception. Walkthrough recommended for sub-area confirmation. ALSO: Trinidad State Beach proper (north of Trinidad Head) has its own DPR per-park ps 86 (parks.ca.gov/?page_id=418) — DPR says "Yes: Except for service animals, dogs not allowed on trails." For Bay Bluff specifically (city downtown frontage), the city ordinance is operative.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Trinidad Municipal Code Title 6 Ch 6.04%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 6 — Trinidad Coastal Land Trust operator policy (Houda Point)
-- Applies to: fid 3602 Camel Rock Surf (Houda Point).
-- Existing Humboldt County Code §541-21 (ps 177) remains operative
-- (off_leash_voice_control) for county-baseline; TCLT operator page is
-- the supplemental page-level deep link for Houda Point specifically.
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'operator_posted_policy',
       'Trinidad Coastal Land Trust — Houda Point / Camel Rock property page (operator)',
       NULL,
       ARRAY['dog_policy']::text[],
       'https://www.trinidadcoastallandtrust.org/houda-point.html',
       'Trinidad Coastal Land Trust — Houda Point: Houda Point is a paradise for surfers, beach goers, birders and painters alike. Can you see the two humps on Camel Rock? Surfers flock here during low tide swells, but the rock is also home to the largest breeding colony of Leach''s petrel on the north coast. Other birds that breed here include the pigeon guillemot, western gull, pelagic cormorant, and the black oyster catcher. This park offers plenty of parking, a flat grassy area on the bluff, and three maintained trails around the bluff and down to the beach. This property is owned and maintained by Trinidad Coastal Land Trust. [Hosted at trinidadcoastallandtrust.org/houda-point.html. The City of Trinidad''s Animal Control page (trinidad.ca.gov/animal-control) explicitly identifies Camel Rock as TCLT property and directs visitors to TCLT for property-specific guidance — confirming TCLT is the operator and Camel Rock is NOT within City of Trinidad jurisdiction. The TCLT property page does NOT publish an explicit on-leash/off-leash policy verbatim; deferring to Humboldt County Code §541-21 voice-control baseline (existing ps 177). Fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Trinidad Coastal Land Trust — Houda Point%'
);

-- fid 3602 Camel Rock Surf — Houda Point, owned and maintained by TCLT.
-- Outside City of Trinidad limits. Humboldt County Code §541-21
-- (existing ps 177) is the operative dog rule (off-leash voice-control).
-- This row adds the TCLT operator page-level deep link as a supplemental
-- source. Operative status = non_enforced (operator page; the operative
-- rule of record remains with the county code; TCLT doesn't publish
-- its own enforced policy here).
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 3602, ps.id, 'sand', 'off_leash_voice_control', 'non_enforced'::operative_status,
       'TCLT Houda Point property page: This property is owned and maintained by Trinidad Coastal Land Trust. The City of Trinidad''s own Animal Control page lists Camel Rock under TRINIDAD COASTAL LAND TRUST properties (outside city scope). TCLT does not publish a separate dog policy on this page; the operative dog rule defaults to Humboldt County Code §541-21 (existing ps 177) voice-control allowance.',
       ps.source_url,
       'SUPPLEMENTAL operator page-level deep link. Operative rule remains Humboldt County §541-21 (ps 177, off_leash_voice_control). Outside City of Trinidad jurisdiction. TCLT is a non-government operator (land trust); per [[operator-not-pseudo-agency]] left as operator (not added to agency table). Sensitive Leach''s petrel breeding colony noted on the TCLT page — adds a wildlife-sensitivity caution for off-leash use.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Trinidad Coastal Land Trust — Houda Point%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
