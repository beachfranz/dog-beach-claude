-- MD Anne Arundel County codify — Article 12 (Public Safety), Title 4
--   (Animal Control), Subtitle 9 § 12-4-905 (Animals running at large).
--
-- Source: Anne Arundel County Code, Article 12 Public Safety, Title 4
--   Animal Control, Subtitle 9 Miscellaneous Provisions, § 12-4-905
--   "Animals running at large prohibited" (+ § 12-4-101 definition of
--   "at large").
--
-- Deep-link URL (primary, amlegal):
--   https://codelibrary.amlegal.com/codes/annearundel/latest/annearundelco_md/0-0-0-111152
-- Alternate deep-link (elaws.us mirror):
--   http://annearundelco-md.elaws.us/code/coor_art12_t4_subt9_sec12-4-905
--
-- ─── Operative rule (§ 12-4-905) — verbatim, via web_search ─────────
--
-- "At large" means off the property of an animal's owner and not
-- leashed and under the control of a responsible person. A domesticated
-- animal may not be at large, and an owner of an animal at large is in
-- violation of this section.
--
-- An animal is not at large for the purposes of this section if it is:
--   - actively engaged in search and rescue operation training or the
--     sport of hunting or other legal dog sport or competition in
--     authorized areas while supervised by a competent individual;
--   - undergoing supervised training as part of an organized obedience
--     class;
--   - in a dog exercise area designated by the Anne Arundel County
--     Department of Recreation and Parks as an "off-leash dog park"
--     under supervision of the dog's owner; or
--   - in a secured dog exercise area designated by the property owner,
--     homeowner's association, condominium, or cooperative as an
--     "off-leash dog park."
--
-- Applicability: Anne Arundel County is a charter county; § 12-4-905
-- applies countywide, including within incorporated municipalities
-- (only Annapolis is incorporated within the county) absent a
-- conflicting local ordinance. No "town X exempted" carve-out appears
-- in § 12-4-905 (contrast with Worcester County PS 2-101(d) which
-- exempts Ocean City).
--
-- ─── Layered authority (swimming-beach rules) ───────────────────────
--
-- Anne Arundel County Department of Recreation & Parks publishes
-- additional rules for the THREE county-operated swimming beaches at
--   https://www.aacounty.org/recreation-parks/swimming-beaches-rules
--
-- Per that page:
--   • Fort Smallwood Park beach: "Dogs are permitted on the southernmost
--     (furthest away from the parking area) section of the beach only.
--     They must be leashed and under owners control at all times while
--     on the beach."
--   • Mayo Beach Park beach: "Dogs are not permitted on the swimming
--     beach, but are welcome on a leash anywhere else in the park."
--     (Not in our 49 beaches.)
--   • Beverly Triton Nature Park beach: "Dogs are not permitted on the
--     swimming beach, but are welcome on a leash anywhere else in the
--     park."
--
-- This is layered on top of the § 12-4-905 baseline for the two
-- in-scope county-park beaches:
--   • fid 1755695 Fort Smallwood At Pond Drive → on_leash (southernmost
--                                                section only)
--   • fid 14282   Triton Beach (= Beverly Triton) → not_allowed on
--                                                   swimming beach
--
-- ─── Per-beach mapping (48 in-scope, 1 deferred) ────────────────────
--
-- The 49 Anne Arundel scoreable beaches break down as:
--
--   • 1 state park (DEFERRED — different agency: MD DNR / Maryland
--     Park Service; COMAR 08.07.06.17 governs; seasonal Sandy Point
--     pet prohibition May 1 – Sept 30 in main park section):
--       fid 6310347 Sandy Point State Park
--
--   • 2 county-operated park beaches (baseline + layered overlay):
--       fid 1755695 Fort Smallwood At Pond Drive  → § 12-4-905 baseline
--                                                  + overlay (south section only)
--       fid 14282   Triton Beach (Beverly Triton) → § 12-4-905 baseline
--                                                  + overlay (not_allowed)
--
--   • 46 private community / HOA / shore-association beaches
--     (Chesapeake Bay shoreline communities, all unincorporated AA
--     County, all subject to § 12-4-905 leash-law baseline):
--       12943 Bare Neck Shore               | 12945 Gibson Island Beach
--       12949 Severn Beach                  | 12953 Wild Rose Shores
--       12965 North Severn Beach            | 13427 Lake Claire Beach
--       13428 Main Beach                    | 13431 Herald Harbor Beach 1
--       14315 Bay Ridge Beach               | 14317 Sparrows Beach
--       14365 Lindamoor Community Beach     | 15302 Carr's Beach
--       15323 Selby Beach                   | 560239 Fairhaven
--       1956310 Magothy beach               | 2083920 Elizabeth's Landing
--       2726273 Avalon Shores Beach         | 3802451 Ulmstead Beach
--       3993507 Round Bay Main Beach        | 4726642 Annapolis Landing
--       5168284 Arden Beach                 | 5508106 Cape St. John
--       5889311 Deale Beach                 | 6041888 North Shore
--       6069268 Magothy Manor               | 7626529 Whitney's Landing
--       8673534 Mountain Point At Gibson Island
--       8706368 High Point                  | 9705026 Masons Beach
--       10006721 Rosehaven                  | 11309125 Town Point At Arkhaven
--       11806946 Twin Harbors               | 11913576 Orchard Beach
--       12641689 Londontowne At Delmar      | 14036438 Londontown Beach At Highland
--       14686248 Sylvan View                | 16006736 South River Heights
--       16644940 Londontown At Silverrun Private Beach
--       17630386 Cape Arthur Beach          | 17725692 Chelsea Beach
--       18168677 Chandler Point             | 18264900 Long Point
--       19029573 Bahama Beach               | 19085616 Olde Severna Park - Hatton Memorial Beach
--       19777850 Londontown Beach At Midland | 20040305 Londontown Beach At Arundel
--
-- ─── Agency provisioning ────────────────────────────────────────────
--
-- No "Anne Arundel County" type=county agency row exists yet (verified
-- 2026-05-22). Create canonical bare-name row before inserting the
-- policy_source row. Also create county_department row for the
-- Recreation & Parks layered ps.

-- ─── 1. Provision Anne Arundel County agency (canonical bare-name) ─

INSERT INTO public.agency (name, type, web_url)
SELECT 'Anne Arundel County', 'county'::agency_type, 'https://www.aacounty.org/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency WHERE name='Anne Arundel County' AND type='county'::agency_type
);

-- Recreation & Parks dept (parent = Anne Arundel County)
INSERT INTO public.agency (name, type, parent_agency_id, web_url)
SELECT 'Anne Arundel County Department of Recreation and Parks',
       'county_department'::agency_type,
       (SELECT id FROM public.agency WHERE name='Anne Arundel County' AND type='county'::agency_type),
       'https://www.aacounty.org/recreation-parks'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency
  WHERE name='Anne Arundel County Department of Recreation and Parks'
    AND type='county_department'::agency_type
);

-- ─── 2. Insert codified-ordinance policy_source (§ 12-4-905 baseline) ─

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Anne Arundel County Code, Article 12 Public Safety, Title 4 Animal Control, Subtitle 9 § 12-4-905 (Animals running at large prohibited)',
       (SELECT id FROM public.agency WHERE name='Anne Arundel County' AND type='county'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://codelibrary.amlegal.com/codes/annearundel/latest/annearundelco_md/0-0-0-111152',
       E'Anne Arundel County Code, Article 12 Public Safety, Title 4 Animal Control, Subtitle 9 Miscellaneous Provisions, § 12-4-905 Animals running at large prohibited. '
       E'"At large" means off the property of an animal''s owner and not leashed and under the control of a responsible person (per § 12-4-101 definitions). '
       E'A domesticated animal may not be at large, and an owner of an animal at large is in violation of this section. '
       E'Exceptions — an animal is not at large for purposes of this section if it is: (i) actively engaged in search and rescue operation training or the sport of hunting or other legal dog sport or competition in authorized areas while supervised by a competent individual; (ii) undergoing supervised training as part of an organized obedience class; (iii) in a dog exercise area designated by the Anne Arundel County Department of Recreation and Parks as an "off-leash dog park" under supervision of the dog''s owner; or (iv) in a secured dog exercise area designated by the property owner, homeowner''s association, condominium, or cooperative as an "off-leash dog park." '
       E'Applicability: Anne Arundel County is a charter county; § 12-4-905 applies countywide, including within the City of Annapolis (the only incorporated municipality in the county) absent a conflicting local ordinance. '
       E'[Hosted on American Legal Publishing at codelibrary.amlegal.com/codes/annearundel; alternate mirror at annearundelco-md.elaws.us/code/coor_art12_t4_subt9_sec12-4-905. Verbatim text obtained via web_search 2026-05-22; Playwright and direct HTTP both blocked by SPA / 403/503 — text confirmed cross-platform.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source
  WHERE citation LIKE 'Anne Arundel County Code, Article 12%§ 12-4-905%'
);

-- ─── 3. Insert layered overlay (Rec & Parks swimming-beach rules) ──

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'Anne Arundel County Recreation & Parks — Swimming Beaches Rules and Regulations (per-beach dog policy: Fort Smallwood, Mayo Beach, Beverly Triton)',
       (SELECT id FROM public.agency
        WHERE name='Anne Arundel County Department of Recreation and Parks'
          AND type='county_department'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://www.aacounty.org/recreation-parks/swimming-beaches-rules',
       E'Anne Arundel County Department of Recreation and Parks — Swimming Beaches Rules and Regulations. Dogs are listed under prohibited items at the county swimming beaches, with per-park exceptions: '
       E'• Fort Smallwood Park: "Dogs are permitted on the southernmost (furthest away from the parking area) section of the beach only. They must be leashed and under owners control at all times while on the beach." '
       E'• Mayo Beach Park: "Dogs are not permitted on the swimming beach, but are welcome on a leash anywhere else in the park." '
       E'• Beverly Triton Nature Park: "Dogs are not permitted on the swimming beach, but are welcome on a leash anywhere else in the park." '
       E'[Hosted on www.aacounty.org. Fetched 2026-05-22.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source
  WHERE citation LIKE 'Anne Arundel County Recreation & Parks — Swimming Beaches Rules%'
);

-- ─── 4. Baseline § 12-4-905 → on_leash for 48 in-scope beaches ──────
--
-- Sandy Point State Park (fid 6310347) is EXCLUDED — state-park-
-- managed; governed by COMAR 08.07.06.17 + Maryland Park Service pet
-- policy (seasonal May 1 – Sept 30 prohibition in main park section).
-- Deferred to a separate MD DNR / Maryland Park Service codify
-- migration.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       E'Anne Arundel County Code § 12-4-905: "At large" means off the property of an animal''s owner and not leashed and under the control of a responsible person; a domesticated animal may not be at large. The leash requirement applies countywide to any animal off the owner''s property.',
       ps.source_url,
       'Countywide § 12-4-905 leash-law baseline (per [[citywide-leash-inference]] — territorial leash rules apply to beaches by default). Most Anne Arundel beaches are private community/HOA shoreline access points along the Chesapeake Bay; § 12-4-905 governs default behavior at all unincorporated beaches and within Annapolis absent a conflicting city ordinance.',
       NULL
FROM (values
  (12943),(12945),(12949),(12953),(12965),
  (13427),(13428),(13431),
  (14282),(14315),(14317),(14365),
  (15302),(15323),
  (560239),(1755695),
  (1956310),(2083920),(2726273),
  (3802451),(3993507),(4726642),
  (5168284),(5508106),(5889311),
  (6041888),(6069268),
  (7626529),(8673534),(8706368),
  (9705026),(10006721),(11309125),
  (11806946),(11913576),
  (12641689),(14036438),(14686248),
  (16006736),(16644940),
  (17630386),(17725692),
  (18168677),(18264900),
  (19029573),(19085616),
  (19777850),(20040305)
) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Anne Arundel County Code, Article 12%§ 12-4-905%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule)
DO NOTHING;

-- ─── 5. Layered overlay: Fort Smallwood (southernmost section only) ─

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT 1755695, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       E'Anne Arundel County Recreation & Parks Swimming Beach Rules — Fort Smallwood Park: "Dogs are permitted on the southernmost (furthest away from the parking area) section of the beach only. They must be leashed and under owners control at all times while on the beach."',
       ps.source_url,
       'Fort Smallwood Park county-operated swimming beach: dogs allowed ONLY on the southernmost section of the beach (farthest from the parking area); dogs prohibited on the remainder of the swimming beach. Walkthrough recommended to confirm posted signage delineating the dog-permitted zone.',
       'Southernmost section (farthest from parking area)'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Anne Arundel County Recreation & Parks — Swimming Beaches Rules%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule)
DO NOTHING;

-- ─── 6. Layered overlay: Beverly Triton (not_allowed on swim beach) ─

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT 14282, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       E'Anne Arundel County Recreation & Parks Swimming Beach Rules — Beverly Triton Nature Park: "Dogs are not permitted on the swimming beach, but are welcome on a leash anywhere else in the park."',
       ps.source_url,
       'Triton Beach = Beverly Triton Nature Park (county-operated, end of Mayo Peninsula). Dogs NOT permitted on the swimming beach proper. Dogs welcome leashed elsewhere in the park (per § 12-4-905 baseline). This overlay supersedes the § 12-4-905 on_leash baseline on the swimming beach sand only.',
       NULL
FROM public.policy_source ps
WHERE ps.citation LIKE 'Anne Arundel County Recreation & Parks — Swimming Beaches Rules%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule)
DO NOTHING;
