-- Wave-3.5: SoCal coastal city backfills — Hermosa Beach / Imperial Beach /
-- Torrance / El Segundo / Chula Vista (6 beaches across 5 cities).
--
-- Picks up SoCal coastal cities the Wave 3 + bucket B passes missed.
-- Each city's codified animal/beach ordinance was fetched verbatim and
-- resolved to a per-section deep-link URL per the
-- [[page-level-over-agency-level]] hard rule.
--
-- Per-city findings:
--
--  * Hermosa Beach (fid 8264 Hermosa City Beach) — encodeplus platform
--    (online.encodeplus.com/regs/hermosabeach-ca/). Hermosa MC §6.08.020
--    "Dogs at large prohibited in public places" includes a flat ban on
--    dogs on the beach: "no dog shall be allowed or permitted: 1. On the
--    beach;" Service-dog carve-out for ADA. Rule = not_allowed. Existing
--    LA County B&H link (ps 45) covered the operator-managed surface; the
--    Hermosa city ordinance is the codified primary source.
--
--  * Imperial Beach (fid 9059 City of Imperial Beach) — ecode360 platform
--    (custid IM4939). Title 12 Ch 12.60 BEACHES, §12.60.100 Animal
--    restrictions. Per-section deep ID = 42864512. SEGMENTED rule:
--    "Dogs are allowed in the beach area of the City if they are fastened
--    or led by chains or leashes of suitable strength which are not more
--    than six feet in length, AND as long as the dogs are not in any
--    beach area from Imperial Beach Boulevard to Palm Avenue."
--    Service-dog + law-enforcement-dog carve-outs.
--    fid 9059 covers the city's main beach strip stretching from Palm Ave
--    south past Imperial Beach Blvd; per the code the central segment
--    (IB Blvd → Palm Ave, ~6 blocks of pier-adjacent main beach) is
--    not_allowed while north/south are on_leash. Single bps row keyed at
--    on_leash (the prevailing rule on most of the strand) with a detailed
--    status_note documenting the central no-dogs zone. Single-fid
--    handling: status_note carries the carve-out detail because we don't
--    have sub-fids for north/central/south. Existing SD County ps 116/117
--    links are general-leash references, supplemented by this beach-
--    specific city ordinance.
--
--  * Torrance (fid 8268 Torrance County Beach) — codepublishing platform
--    (codepublishing.com/CA/Torrance/). Torrance Division 4 (Public
--    Health and Welfare) Chapter 4 OCEAN AND BEACHES. §44.3.11 ANIMALS
--    is a FLAT prohibition: "A person shall not lead, ride or bring onto
--    the beach or into the waters of the Pacific Ocean adjacent to the
--    beach, any cattle, horse, mule, goat, sheep, swine, dog, cat or
--    other animal of any kind." §44.1.3 BEACH definition explicitly
--    includes beaches "owned, managed or controlled by the County of Los
--    Angeles or the City of Torrance and located within the City of
--    Torrance" — so the city ordinance applies CONCURRENTLY with LA
--    County rules on Torrance County Beach (operated by LA County B&H but
--    sitting in city limits). Per §44.1.3(b) the chapter "does not apply
--    to parks" — so park-chapter (§49) is separately scoped. The Torrance
--    city ordinance is NOT a deferral; it's an independent codified
--    prohibition. Rule = not_allowed (matches existing LA County link).
--    Supplements (does not replace) the existing ps 45 LA County B&H link.
--
--  * El Segundo (fid 8895 El Segundo Beach) — amlegal platform
--    (codelibrary.amlegal.com/codes/elsegundoca/). Title 10 Ch 3 BEACH
--    REGULATIONS, §10-3-11(B)(1) "Animals — Bringing To Beach Prohibited":
--    "No person shall maintain or bring on any beach nor bring into the
--    waters of the Pacific Ocean adjacent to any beach, any cattle, horse,
--    mule, goat, sheep, swine, dog, cat or other animal of any kind,
--    whether such animal is leashed or unleashed."
--    Per §10-3-1 SCOPE, this chapter applies to "all ocean beaches in the
--    city...maintained for the use of the general public by the state,
--    the county, and the city, or by any department thereof, or by any
--    other public body" — and "does not apply to any parks." So it covers
--    LA County-operated El Segundo Beach inside city limits.
--    The El Segundo city ordinance is NOT a deferral; it's an independent
--    codified prohibition mirroring the LA County rule. Rule = not_allowed.
--    Supplements (does not replace) the existing ps 45 LA County B&H link.
--
--  * Chula Vista (fid 754 Big Bird's Beach, fid 6294 Dolphin Beach) —
--    chulavista.municipal.codes platform. Chula Vista MC §2.66.130
--    "Animals prohibited – Exceptions": "Except during a dog show or
--    exhibition...it is unlawful to bring or allow a dog to be within any
--    City park, recreation center or recreation area, unless the dog is
--    restrained by a chain, line or leash not to exceed six feet."
--    CRITICAL JUDGMENT CALL: Both target fids are NOT real Chula Vista
--    public coastal beaches —
--      - fid 754 "Big Bird's Beach" @ lat 32.587 lon -117.011 — this is
--        the wave pool INSIDE Sesame Place San Diego (2052 Entertainment
--        Cir, Chula Vista), a private theme-park feature. Not a public
--        beach; private park rules govern (no outside dogs allowed per
--        theme park policy). 4+ km inland from SD Bay shoreline.
--      - fid 6294 "Dolphin Beach" @ lat 32.634 lon -116.951 — this is
--        Dolphin Beach Club at 1455 Shoreacres Drive in EastLake Greens
--        (Chula Vista), a PRIVATE HOA pool/recreation club. Not a public
--        beach; HOA rules govern. ~10+ km inland from SD Bay shoreline.
--    The real Chula Vista bayfront (Bayside Park / Bayfront Park) is
--    operated by the Port of San Diego, NOT the City of Chula Vista, and
--    has its own pet policy (leashed allowed before 9am / after 6pm
--    summer). Neither target fid is governed by either Chula Vista city
--    code §2.66.130 (parks) or Port of SD tideland rules.
--    DEFERRED: Writing a Chula Vista city ps row for these two fids would
--    be incorrect — neither beach is a Chula Vista public park subject to
--    §2.66.130, and neither sits on Port-of-SD-managed bayfront. The
--    correct fix is upstream: flag both fids for governance + categorization
--    review (Big Bird's Beach is private/theme-park; Dolphin Beach is
--    private/HOA). They likely shouldn't be classified as scoreable public
--    beaches at all. Existing SD County ps 116/117 links (general SD
--    County leash law) carry — those at least cover the county-level
--    leash rule that applies on any public property.
--
-- Three city ps rows written (HB / IB / Torrance / ES — Chula Vista deferred).
-- All four cities exist in public.agency at the agency_ids supplied in the
-- task spec: 54 (Hermosa Beach), 97 (Imperial Beach), 83 (Torrance),
-- 53 (El Segundo), 100 (Chula Vista — unused, see Chula Vista deferral note).
--
-- Sources fetched verbatim 2026-05-17 via Playwright.


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 1 — Hermosa Beach MC §6.08.020 (Dogs at large prohibited in
--             public places) — flat beach prohibition.
-- Applies to: fid 8264 Hermosa City Beach.
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Hermosa Beach Municipal Code §6.08.020 (Dogs at large prohibited in public places) — Title 6 Animals, Chapter 6.08 Dogs; flat dog prohibition on the beach with ADA service-dog carve-out',
       54,
       ARRAY['dog_policy']::text[],
       'https://online.encodeplus.com/regs/hermosabeach-ca/doc-viewer.aspx?secid=783',
       'Hermosa Beach Municipal Code §6.08.020 Dogs at large prohibited in public places: "It is unlawful for any person to allow a dog, that he or she owns, or is in his or her care, possession or control, to run loose, or at large on any public street, alley, lane, park, the Strand, the Greenbelt, or other public place, or in or upon any unenclosed lot or premises in the City. When off the premises of the person in control or possession of a dog, a dog shall be restrained by a chain or cord not exceeding six (6) feet in length and is in the charge, care, custody or control of a competent person capable of controlling the dog. Except for ''service dogs'' which is defined to include dogs that are individually trained to do work or perform tasks for the benefit of an individual with a disability, no dog shall be allowed or permitted: 1. On the beach; 2. In any store, market, restaurant, cafe, lunchroom, bakery; or 3. In an establishment wherein vegetables, meats and other foods for human consumption are served, sold or kept for sale. A violation of this section shall be subject to the provisions of Chapter 1.10. (Prior code § 4-8) (Ord. # 21-1439 §3, adopted 11/23/2021, effective 12/23/2021)" [Hosted on enCodePlus at online.encodeplus.com/regs/hermosabeach-ca/. Per-section deep ID = secid=783. Fetched verbatim via Playwright 2026-05-17. Companion city pages: hermosabeach.gov/enjoying-hermosa/beach-strand-and-pier-regulations notes a $250 fine effective 12/9/2021 for each violation of leash law / dog-on-beach prohibition. Pets ALLOWED on the Strand on leash; PROHIBITED on the beach itself and on the Pier.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Hermosa Beach Municipal Code §6.08.020%'
);

-- fid 8264 Hermosa City Beach — flat dog prohibition per §6.08.020.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8264, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'Hermosa Beach Municipal Code §6.08.020: "Except for ''service dogs''...no dog shall be allowed or permitted: 1. On the beach;" Hermosa City Beach IS the Hermosa Beach city-named beach. ADA service-dog carve-out reads in.',
       ps.source_url,
       'Flat prohibition; ADA service-dog exception only. Pets are permitted on the elevated Strand path on leash, but NOT on the sandy beach itself or on the Pier. City enforces a $250 fine per violation (effective 12/9/2021). Existing LA County B&H link (ps 45) covers operator-managed surface; this city ordinance is the codified primary source.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Hermosa Beach Municipal Code §6.08.020%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 2 — Imperial Beach MC §12.60.100 (Animal restrictions) —
--             on-leash with central no-dogs carve-out.
-- Applies to: fid 9059 City of Imperial Beach.
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Imperial Beach Municipal Code §12.60.100 (Animal restrictions) — Title 12 Streets, Sidewalks and Public Places, Chapter 12.60 BEACHES; dogs on leash (≤6 ft) EXCEPT prohibited in the central beach segment between Imperial Beach Boulevard and Palm Avenue',
       97,
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/42864512',
       'Imperial Beach Municipal Code §12.60.100 Animal restrictions: "Except as provided below, it is unlawful for any person, firm or corporation to bring, leave, turn loose, ride, accompany or allow any animal, including, but not limited to, mammals, reptiles, and birds, in or upon the beach area of the City. A. Dogs are allowed in the beach area of the City if they are fastened or led by chains or leashes of suitable strength which are not more than six feet in length, and as long as the dogs are not in any beach area from Imperial Beach Boulevard to Palm Avenue. B. Service animals, usually dogs, that have been specially trained to help people with a disability, are allowed in the beach areas if the service animals are being used for such purposes. C. Official law enforcement dogs or other animals that are under the control of law enforcement officers on official duty are allowed in the beach areas. (Ord. 2003-1005 § 1)" Companion provisions in Chapter 12.60 BEACHES include §12.60.005 lifeguard authority, §12.60.080 kiteboarding restrictions (between Palm Ave and IB Blvd June 1 – Sept 1), §12.60.090 sleeping/camping prohibition between sunset and 7am, §12.60.120 glass containers prohibited. [Hosted on ecode360 (qcode/General Code property) at ecode360.com/42864512. Custid IM4939. Per-section deep ID = 42864512. Note: qcode.us URLs at library.qcode.us/lib/imperial_beach_ca/pub/municipal_code/item/title_12-chapter_12_60-12_60_100 redirect to ecode360 (qcode-to-ecode360 merger). Companion city pages: imperialbeachca.gov/319/Beach-Regulations notes dogs prohibited on the pier as well. Fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Imperial Beach Municipal Code §12.60.100%'
);

-- fid 9059 City of Imperial Beach — on-leash on most of the strand, with
-- a hard no-dogs carve-out in the central IB Blvd → Palm Ave segment.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 9059, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Imperial Beach Municipal Code §12.60.100(A): "Dogs are allowed in the beach area of the City if they are fastened or led by chains or leashes of suitable strength which are not more than six feet in length, and as long as the dogs are not in any beach area from Imperial Beach Boulevard to Palm Avenue." Service-dog and law-enforcement-dog carve-outs in (B) and (C).',
       ps.source_url,
       'SUB-AREA CARVE-OUT: dogs on-leash (≤6 ft) on the NORTH segment (north of Palm Ave) and the SOUTH segment (south of Imperial Beach Blvd) of the city beach strip. Dogs are PROHIBITED in the central segment between Imperial Beach Boulevard and Palm Avenue (the pier-adjacent main beach, roughly 6 blocks). fid 9059 covers the entire city beach as a single polygon; single-fid handling keys at on_leash (prevailing rule on most of the strand) with this status_note carrying the sub-area carve-out detail. Dogs also prohibited on the IB Municipal Pier per companion city page (imperialbeachca.gov/319/Beach-Regulations). Existing SD County ps 116/117 links are general county leash law; this city ordinance is the beach-specific primary source.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Imperial Beach Municipal Code §12.60.100%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 3 — Torrance MC §44.3.11 (Animals) — flat beach prohibition.
-- Applies to: fid 8268 Torrance County Beach.
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Torrance Municipal Code §44.3.11 (Animals) — Division 4 Public Health and Welfare, Chapter 4 OCEAN AND BEACHES, Article 3 Rules and Regulations; flat beach + ocean prohibition on dogs and other animals',
       83,
       ARRAY['dog_policy']::text[],
       'https://www.codepublishing.com/CA/Torrance/html/Torrance04/Torrance0404.html',
       'Torrance Municipal Code §44.3.11 ANIMALS: "A person shall not lead, ride or bring onto the beach or into the waters of the Pacific Ocean adjacent to the beach, any cattle, horse, mule, goat, sheep, swine, dog, cat or other animal of any kind." Scope definition §44.1.3 BEACH: "(a) Beach means a public beach or shoreline area bordering the Pacific Ocean, owned, managed or controlled by the County of Los Angeles or the City of Torrance and located within the City of Torrance...(b) The provisions of this Chapter shall apply to all Ocean beaches in the City whether publicly or privately owned or maintained, which are maintained for the use of the general public by the State of California, or by any department thereof, or by any other public body. This Chapter does not apply to parks." Short title §44.2.2: "This Chapter shall be known as, and may be cited as, the Beach Ordinance." Enforcement §44.2.4: jointly enforced by LA County Director of Beaches and Torrance City Manager. [Hosted on codepublishing at www.codepublishing.com/CA/Torrance/html/Torrance04/Torrance0404.html. Note Torrance''s code is structured as Divisions (1-9) → Chapters → Articles → Sections with section numbers like §44.3.11 = Division 4, Chapter 4, Article 3, Section 11. Fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Torrance Municipal Code §44.3.11%'
);

-- fid 8268 Torrance County Beach — flat dog prohibition per §44.3.11
-- AND parallel LA County rule (ps 45). City ordinance supplements, does
-- not replace, the LA County link.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8268, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'Torrance Municipal Code §44.3.11: "A person shall not lead, ride or bring onto the beach or into the waters of the Pacific Ocean adjacent to the beach, any cattle, horse, mule, goat, sheep, swine, dog, cat or other animal of any kind." §44.1.3 BEACH definition expressly includes beaches "owned, managed or controlled by the County of Los Angeles or the City of Torrance and located within the City of Torrance" — Torrance County Beach is LA-County-operated but sits in Torrance city limits, so the city ordinance applies concurrently.',
       ps.source_url,
       'Flat prohibition; service-dog exception reads in under ADA but is not explicit in the section. Torrance County Beach is operated by LA County Beaches & Harbors (see existing ps 45 link) but sits within Torrance city limits — so BOTH the LA County beach rules AND the Torrance city Beach Ordinance apply concurrently. §44.1.3(b) further provides "This Chapter does not apply to parks." (i.e., Miramar Park, the inland city park at the bluffs, is governed separately under §49 Conduct In Public Parks). The Torrance city ordinance is NOT a mere deferral to LA County; it is an independent codified prohibition that mirrors the county rule.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Torrance Municipal Code §44.3.11%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 4 — El Segundo City Code §10-3-11(B)(1) (Animals — Bringing
--             To Beach Prohibited) — flat beach + ocean prohibition.
-- Applies to: fid 8895 El Segundo Beach.
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'El Segundo City Code §10-3-11(B)(1) (Animals — Bringing To Beach Prohibited) — Title 10 Parks and Recreation, Chapter 3 BEACH REGULATIONS, §10-3-11 Rules and Regulations; flat beach + ocean prohibition on dogs and other animals whether leashed or unleashed',
       53,
       ARRAY['dog_policy']::text[],
       'https://codelibrary.amlegal.com/codes/elsegundoca/latest/elsegundo_ca/0-0-0-7295',
       'El Segundo City Code §10-3-11 Rules and Regulations, subsection B Animals: "1. Bringing To Beach Prohibited: No person shall maintain or bring on any beach nor bring into the waters of the Pacific Ocean adjacent to any beach, any cattle, horse, mule, goat, sheep, swine, dog, cat or other animal of any kind, whether such animal is leashed or unleashed. 2. Riding Horses, Other Animals: No person shall ride a horse, mule, burro, or donkey, or other similar animal, or lead such animal on, upon, along any beach or in or along the waters of the Pacific Ocean adjacent to any beach." Scope §10-3-1: "The provisions of this chapter shall apply to all ocean beaches in the city as defined in section 10-3-2 of this chapter, which are maintained for the use of the general public by the state, the county, and the city, or by any department thereof, or by any other public body. This chapter does not apply to any parks. (Ord. 803, 3-12-1973)" Definition §10-3-2: BEACH means "All that area lying adjacent to the Pacific Ocean within the city of El Segundo..." (full metes-and-bounds description references Tract 1314 Lot 1 and Tract 2356 Lot 22). Enforcement §10-3-5: by the chief of police. Exceptions §10-3-7: emergency, employees performing duties, instruction/training/exhibitions with chief-of-police permission, City Council special permit. [Hosted on American Legal Publishing at codelibrary.amlegal.com/codes/elsegundoca/. Section per-page deep ID = 0-0-0-7295 (the §10-3-7 EXCEPTIONS page; renders the entire Chapter 3 BEACH REGULATIONS). Fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'El Segundo City Code §10-3-11%'
);

-- fid 8895 El Segundo Beach — flat dog prohibition per §10-3-11(B)(1)
-- AND parallel LA County rule (ps 45). City ordinance supplements, does
-- not replace, the LA County link.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8895, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'El Segundo City Code §10-3-11(B)(1) Bringing To Beach Prohibited: "No person shall maintain or bring on any beach nor bring into the waters of the Pacific Ocean adjacent to any beach, any cattle, horse, mule, goat, sheep, swine, dog, cat or other animal of any kind, whether such animal is leashed or unleashed." Scope §10-3-1 covers "all ocean beaches in the city...maintained for the use of the general public by the state, the county, and the city, or by any department thereof, or by any other public body" — El Segundo Beach is LA-County-operated but sits in city limits, so the city ordinance applies concurrently.',
       ps.source_url,
       'Flat prohibition with explicit "whether leashed or unleashed" language; service-dog exception reads in under ADA but is not explicit in the section. Bike path and parking lot are typically also dogs-prohibited per the operator''s rules (LA County B&H). El Segundo Beach is operated by LA County Beaches & Harbors (see existing ps 45 link) but sits within El Segundo city limits — so BOTH the LA County beach rules AND the El Segundo city Beach Regulations chapter apply concurrently. §10-3-1 expressly states the chapter "does not apply to any parks." The El Segundo city ordinance is NOT a mere deferral to LA County; it is an independent codified prohibition that mirrors the county rule.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'El Segundo City Code §10-3-11%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 5 — Chula Vista — DEFERRED (both target fids are NOT real
--             Chula Vista public coastal beaches).
-- ─────────────────────────────────────────────────────────────────────
--
-- fid 754 Big Bird's Beach — the wave-pool feature INSIDE Sesame Place
--   San Diego at 2052 Entertainment Cir, Chula Vista. Private theme park
--   feature; not a public beach. Coordinates (32.587, -117.011) are ~4 km
--   inland from the San Diego Bay shoreline. Theme park rules govern
--   (no outside dogs).
--
-- fid 6294 Dolphin Beach — Dolphin Beach Club at 1455 Shoreacres Drive
--   in EastLake Greens (Chula Vista). Private HOA pool/recreation club;
--   not a public beach. Coordinates (32.634, -116.951) are ~10+ km
--   inland from the San Diego Bay shoreline. HOA rules govern.
--
-- The genuine Chula Vista bayfront (Bayside Park / Bayfront Park) is
-- operated by the Port of San Diego, NOT the City of Chula Vista. Port
-- pet policy: leashed dogs allowed before 9am / after 6pm summer; before
-- 9am / after 4pm winter. Neither target fid sits on Port-managed
-- tideland.
--
-- Writing a Chula Vista city ps row (§2.66.130 Animals prohibited –
-- Exceptions, the city parks ordinance) for these two fids would be
-- INCORRECT — neither beach is a Chula Vista public park subject to
-- §2.66.130. Existing SD County ps 116/117 links (general SD County
-- leash law) carry as the only generally applicable rule.
--
-- UPSTREAM FOLLOW-UP: flag both fids for governance + categorization
-- review. fid 754 (Big Bird's Beach) and fid 6294 (Dolphin Beach) should
-- likely NOT be classified as scoreable public beaches. Suggested
-- routing: deferred-private-facility-cleanup pin.
