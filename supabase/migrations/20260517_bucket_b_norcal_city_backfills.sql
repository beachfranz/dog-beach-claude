-- Bucket B Norcal/Central: 4 beach backfills — Sacramento (Paradise + Sand Cove),
-- San Mateo County (Coyote Point), and Sand City (Monterey State Beach).
--
-- Wave-3 follow-up. Per beach the operative authority was verified before
-- selecting the issuing_agency_id and source_url — and in two cases the
-- "obvious" agency in the input table was wrong:
--
--   * fid 6151 Paradise Beach — table said Sacramento city (57). Actually
--     part of the American River Parkway, operated by Sacramento COUNTY
--     Department of Regional Parks. Issuing agency = 32 (Sacramento County).
--
--   * fid 8965 Sand Cove Beach — table said Sacramento city (57). Sand
--     Cove Park (2005 Garden Hwy) is a City of Sacramento parks facility
--     (cityofsacramento.gov/ypce/parks/park-directory). Issuing agency = 57.
--
--   * fid 9313 Coyote Point Beach — table said San Mateo city (74).
--     Actually a San Mateo COUNTY park, operated by SMC Parks Dept.
--     Issuing agency = 282 (San Mateo County Parks and Recreation Dept.).
--
--   * fid 8485 Monterey State Beach — table said Sand City (98). Actually
--     a CA STATE BEACH administered by CA DPR. Sand City municipal
--     boundary clips the beach but DPR is the operative authority.
--     Issuing agency = 234 (California Department of Parks and Recreation).
--
-- Per-park page-level URL discovered via parks.ca.gov/?page_id=21805
-- (Complete State Parks Listing) per [[page-level-over-agency-level]]:
--     Monterey State Beach = parks.ca.gov/?page_id=576
--
-- Monterey State Beach has TWO operative rules on its DPR per-park page:
--   * On-leash, south of Monterey Tides Hotel
--   * Not allowed, north of Monterey Tides Hotel
-- A single policy_source (the DPR per-park page) covers both; emitted as
-- two beach_policy_source rows differentiated by section + rule + status_note.
--
-- Sources fetched verbatim 2026-05-17 via Playwright + WebSearch.


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 1 — Sacramento County Code §9.36.061 (American River Parkway)
-- Applies to: Paradise Beach (fid 6151)
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Sacramento County Code §9.36.061 (Animals) — Title 9 Chapter 9.36 Park Regulations (governs County Regional Parks including American River Parkway)',
       32,
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/44030941',
       'Sacramento County Code §9.36.061 Animals: (a) No person shall: ... (4) Bring into, maintain or allow in or upon any park facility any dog, cat, or other animal except a horse, unless such animal at all times is kept on a leash of sufficient strength and durability that it cannot be broken by the animal so leashed, and no longer than six feet in length, and be under the full and complete physical control of its owner or custodian at all times. Notwithstanding the provision of this subsection (a)(4), the Director may designate: (i) Areas and times where such animals may be unleashed for purposes of show, demonstration, or training, (ii) Areas and times where such animals are wholly restricted from entering designated parks or park areas; ... (8) Permit or suffer any animal owned by him or her, or in his or her possession, custody, or control, to defecate upon park facility property without immediately removing such animal feces, placing said feces in a sealed bag or other sealed container, and placing such bag or container with feces in a proper refuse receptacle. ... (b) Notwithstanding the provisions of subsection (a), all animals shall be prohibited at all times in, upon, or within 15 feet of any area, designated as a nature study area. (c) A violation of any of the provisions of this section shall be punishable as follows: (1) A first violation is punishable as an infraction; and (2) A second or subsequent violation of the same provisions within 30 days of the previous violation shall be a misdemeanor. (SCC 576 §16, 1983; SCC 0713 §3, 1988; SCC 0957 §1, 1994; SCC 1542 §1, 2013) [Park facility includes the American River Parkway operated by Sacramento County Department of Regional Parks. Hosted on ecode360 (General Code). Fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Sacramento County Code §9.36.061%'
);

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 6151, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Sacramento County Code §9.36.061(a)(4): dogs kept on a leash of sufficient strength, no longer than six feet in length, under full and complete physical control of owner. Paradise Beach is part of the American River Parkway, operated by Sacramento County Department of Regional Parks.',
       'https://regionalparks.saccounty.gov/Parks/Pages/ParadiseBeach.aspx',
       'American River Parkway access — see regionalparks.saccounty.gov per-park page for current designations (Director may designate off-leash or wholly-restricted areas under §9.36.061(a)(4)(i)/(ii)).'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Sacramento County Code §9.36.061%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 2 — Sacramento City Code §12.72.060(K) (City Parks)
-- Applies to: Sand Cove Beach (fid 8965) — Sand Cove Park, 2005 Garden Hwy
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Sacramento City Code §12.72.060(K) (Park use regulations — dogs leashed) — Title 12 Chapter 12.72 Parks, Park Buildings and Recreational Facilities',
       57,
       ARRAY['dog_policy']::text[],
       'https://codelibrary.amlegal.com/codes/sacramentoca/latest/sacramento_ca/0-0-0-24450',
       'Sacramento City Code §12.72.060 Park use regulations: No person shall: ... (J) Bring any wild or domesticated animal or pet into or upon grounds of the zoo, Fairytale Town, any swimming pool, any golf course, commercial amusement area or children''s playground area except as provided in Section 9.44.300(B); (K) Bring any domesticated animal or pets into park areas other than those designated in subsection J of this section unless they are leashed except as provided in Section 9.44.300(B), or pursuant to a special event permit issued under Chapter 12.48. ... §12.72.010 defines "Parks" as: all parks, parkways, medians, pedestrian malls, plazas, greenbelts, gardens, lakes, and any other property owned or controlled by the city, including structures thereon and off-street parking areas that are used in connection therewith, that are operated or maintained for passive or active recreational purposes. (Ord. 2022-0012 §1; Ord. 2020-0001 §5; Ord. 2017-0004 §1; Ord. 2012-042 §5) [Hosted on amlegal (codelibrary.amlegal.com); also accessible via qcode.us/codes/sacramento/ as the qcode↔amlegal merger passes through. Fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Sacramento City Code §12.72.060(K)%'
);

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8965, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Sacramento City Code §12.72.060(K): "Bring any domesticated animal or pets into park areas... unless they are leashed." Sand Cove Park (2005 Garden Highway) is a City of Sacramento Parks Department facility on the Sacramento River.',
       'https://www.cityofsacramento.gov/ypce/parks/park-directory/s-t/sand-cove-park',
       'Riverside park; seasonally closed in winter due to flooding (typically reopens around Memorial Day).'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Sacramento City Code §12.72.060(K)%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 3 — San Mateo County Code §3.68.180 (County Parks — Dogs)
-- Applies to: Coyote Point Beach (fid 9313)
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'San Mateo County Ordinance Code §3.68.180 (Dogs) — Title 3 Chapter 3.68 County Park and Recreation Area Rules (governs San Mateo County Parks Department facilities including Coyote Point Recreation Area)',
       282,
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/ca/san_mateo_county/codes/code_of_ordinances?nodeId=TIT3PUSAMOWE_CH3.68COPAREARRU_3.68.180DO',
       'San Mateo County Ordinance Code §3.68.180 Dogs: (a) No dogs shall be permitted in any San Mateo County Park or Recreation Area, unless such area is specifically designated and signed by the Department to allow dogs. This subsection shall not apply to (i) service dogs under physical control of the owner/handler, specifically trained to assist persons with disabilities in accordance with the American''s with Disabilities Act or (ii) a "police dog" under the control of a peace officer. (b) In any San Mateo County Park or Recreation Area where dogs are allowed, no person shall have more than three dogs at any time, provided that all dogs are leashed at all times. (c) No person shall cause or allow any dog under his or her ownership, possession, or control to enter or remain in any San Mateo County Park or Recreation Area unless the dog is licensed as required by the County of San Mateo, is wearing around its neck a collar and valid license tag, and the owner or possessor of the dog complies with all other conditions of this section. (d) In any San Mateo County Park or Recreation Area where dogs are allowed, no person shall cause or allow any dog under his or her ownership, possession, or control to enter or remain in such area unless such person restrains such dog, at all times, with a leash not to exceed six (6) feet in length (sixteen (16) feet when unretracted) and insures that the leash and control by the person are sufficient to prevent the dog from endangering other persons or animals. Where dogs are permitted only on designated trails, all dogs shall be restricted to the designated trails at all times and shall not be allowed to enter the natural habitat abutting the designated trails. (e) Any person bringing a dog into any San Mateo County Park or Recreation Area shall immediately remove any feces deposited by such dog. (f) Notwithstanding any other provision of this section or of Section 6.04.070, in any San Mateo County Park or Recreation Area specifically designated and signed by the Department to allow off-leash dogs, additional requirements apply (max 2 off-leash dogs under voice and sight control, must remain on designated trail within 25 ft, on-leash in developed areas). (g) Violations are infractions: $5 first / $30 second within 1 yr / $100 each additional. (Ord. No. 04805, §2, 11-6-2018) [Hosted on Municode. Fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'San Mateo County Ordinance Code §3.68.180%'
);

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 9313, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'San Mateo County Ordinance Code §3.68.180(a): "No dogs shall be permitted in any San Mateo County Park or Recreation Area, unless such area is specifically designated and signed by the Department to allow dogs." Coyote Point Recreation Area allows dogs on leash on designated trails (Bay Trail and posted areas); dogs prohibited at CuriOdyssey and unposted trails (§3.68.180(a) + (d)).',
       'https://www.smcgov.org/parks/dogs-coyote-point-recreation-area',
       'Coyote Point is a San Mateo County park (operated by SMC Parks Dept.) — within City of San Mateo limits but county-administered. Dogs on-leash on Bay Trail and designated/signed trails only; prohibited at CuriOdyssey and unposted areas. Sand-beach access policy follows §3.68.180(a) (signage-gated).'
FROM public.policy_source ps
WHERE ps.citation LIKE 'San Mateo County Ordinance Code §3.68.180%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 4 — CA State Parks per-park page (Monterey State Beach)
-- Applies to: Monterey State Beach (fid 8485) — TWO operative sub-area rules
-- ─────────────────────────────────────────────────────────────────────
--
-- DPR per-park page (parks.ca.gov/?page_id=576) is authoritative for both
-- sub-areas. Verbatim from page (DOGS section, fetched 2026-05-17):
--
--   "Dogs are allowed on leash at:
--    Monterey State Beach, South of the Monterey Tides Hotel.
--    Dogs are NOT allowed at:
--    Monterey State Beach, North of the Monterey Tides Hotel."
--
-- The unit also includes Western snowy plover seasonal resting areas
-- (early spring) per the same page. Sand City municipal boundary clips
-- the unit but DPR is the operative authority (state-park-as-primary-
-- agency pattern, cf. Crystal Cove walkthrough).

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Monterey State Beach',
       234,
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/?page_id=576',
       'California Department of Parks and Recreation, Monterey State Beach per-park page (parks.ca.gov/?page_id=576). DOGS section: "Dogs are allowed on leash at: Monterey State Beach, South of the Monterey Tides Hotel. Dogs are NOT allowed at: Monterey State Beach, North of the Monterey Tides Hotel. Please be aware of: Families with small children; Visitors with their own pets on leash; Early spring Western snowy plover seasonal resting areas. Thank you for keeping your dog on leash at all times!" Are dogs Allowed? "No" (top-of-page summary reflects the most restrictive sub-area; the DOGS subsection carves out the south-of-Tides-Hotel leashed access). Park Hours: 8 AM – Sunset. Address: Del Monte Avenue at Park Avenue, Monterey, CA 93940. Monterey District Headquarters (831) 649-2836. The state-beach unit comprises three expanses of golden sand between Monterey and Seaside; underwater area is part of Monterey Bay National Marine Sanctuary. Sand City municipal boundary clips the unit but CA DPR is the operative administrative authority. Fetched verbatim 2026-05-17.'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Monterey State Beach'
);

-- Sub-area 1: south of Monterey Tides Hotel → on_leash
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8485, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'CA State Parks per-park page (Monterey State Beach): "Dogs are allowed on leash at: Monterey State Beach, South of the Monterey Tides Hotel."',
       ps.source_url,
       'Sub-area rule: south of the Monterey Tides Hotel only. North of the hotel is not_allowed (see companion row). Early spring Western snowy plover seasonal resting areas — leash compliance critical.'
FROM public.policy_source ps
WHERE ps.citation = 'California State Parks dog policy — Monterey State Beach'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Sub-area 2: north of Monterey Tides Hotel → not_allowed
-- Use a distinct `section` value so the composite PK doesn't collide.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8485, ps.id, 'sand_north_of_tides_hotel', 'not_allowed', 'operative'::operative_status,
       'CA State Parks per-park page (Monterey State Beach): "Dogs are NOT allowed at: Monterey State Beach, North of the Monterey Tides Hotel."',
       ps.source_url,
       'Sub-area rule: north of the Monterey Tides Hotel. Companion to the south sub-area which permits on-leash dogs (see other row). Western snowy plover habitat protection is a likely driver.'
FROM public.policy_source ps
WHERE ps.citation = 'California State Parks dog policy — Monterey State Beach'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
