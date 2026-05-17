-- Bucket A: tier-3-flagged backfills for 18 Marin County beaches and
-- 6 mainland LA County beaches (24 total). Five sub-clusters by
-- operative authority, with documented deferrals for HOA/private and
-- ambiguous-jurisdiction cases.
--
-- WAVE-3 BACKLOG: these beaches were classified `3_limited_access`
-- by the scoring tier but had no precedence-1 governance / no
-- policy_source attribution. Per-beach geographic + authority
-- analysis reveals that several should actually be reclassified
-- `4_no_dogs` (Cabrillo San Pedro, El Segundo, Redondo Pier,
-- Tomales Bay SP beaches post-2023 PRNS rule change, Mt Tam SP
-- beaches, Red Rock Beach, Mickey's Beach, Steep Ravine Beach).
-- Flagged for follow-up tier-classification cleanup; current pass
-- just attributes operative source per page-level rule and lets
-- consensus engine surface the mismatches.
--
-- Sub-cluster index (per-cluster rule analysis below):
--   M1 — Point Reyes National Seashore + adjacent NPS-administered
--        Tomales Bay shoreline (5 beaches) → NPS Point Reyes pets
--        page; not_allowed on west-side Tomales Bay beaches per
--        May 2023 rule change.
--   M2 — Tomales Bay State Park beaches (5 beaches, all DPR-
--        administered within PRNS lease envelope but DPR rules
--        govern visitor conduct on the leased state-park land) →
--        new CA DPR per-park ps row at page_id=470; not_allowed
--        on beaches (developed-area-only carve-out).
--   M3 — Mount Tamalpais State Park beaches (3 beaches) → new CA
--        DPR per-park ps row at page_id=471; not_allowed on
--        beaches (developed-area-only carve-out).
--   M4 — Unincorporated Marin County, no state park (3 beaches:
--        Bolinas Beach, Chicken Ranch Beach, Brickyard Beach) →
--        existing Marin County ps id 185 reused (covers TIT8AN
--        chapter); Marin County animal code applies.
--        DEFERRED: Gallinas Beach (fid 337) — ambiguous bay-edge
--        with weak Marin County authority signal; needs
--        site-visit verification.
--   M5 — Stinson Beach area HOA / private (Seadrift fid 8238) →
--        DEFER (private community + Marin County co-jurisdiction
--        legal easement to high tide; not a clean policy_source
--        attribution).
--   M6 — Dillon Beach (fid 1783) → DEFER (private beach club; the
--        Dillon Beach Resort owns the beach, no statutory policy
--        source applies).
--   LA1 — LA City beaches (Cabrillo San Pedro fid 8555) → new ps
--         row for LAMC §63.44 (Park & Recreation regulations).
--         Operative rule: no dogs on beach per LA City Dept of
--         Rec & Parks (Board of Recreation and Park Commissioners
--         delegated authority under LAMC §63.44 + §63.02).
--   LA2 — LA County Beaches & Harbors (Mother's Beach Marina del
--         Rey, fid 8537) → existing ps id 45 reused.
--   LA3 — Independent LA-area cities (Redondo Beach fid 7404, El
--         Segundo fid 8895, Redondo Beach State Park fid 8731)
--         → new ps rows for Redondo §5-1.103 and El Segundo (LA
--         County baseline reuse for El Segundo since the city
--         hasn't published a per-beach ordinance and LA County
--         operates El Segundo Beach via the Beaches & Harbors
--         dept). Redondo Beach State Park: NOT actually a state
--         park (no CA DPR page); coords (33.83, -118.39) put it
--         within City of Redondo Beach jurisdiction → Redondo
--         §5-1.103 applies.
--   DEFERRED: fid 8392 "Cabrillo Beach" — coords 33.42/-118.40
--   place this on Santa Catalina Island (Two Harbors area, NOT
--   San Pedro). Operated by Catalina Island Company / Catalina
--   Island Conservancy as a boat-in primitive campsite. Not
--   the LA City Cabrillo Beach (fid 8555). Defer pending dedup
--   resolution or Catalina-Conservancy operator attribution.
--
-- Final tally: 19 of 24 attributed with new bps rows; 5 deferred:
--   - fid 337 Gallinas Beach (weak signal)
--   - fid 1307 Teachers Beach (location unverified; no public source)
--   - fid 1783 Dillon Beach (private resort)
--   - fid 8238 Seadrift Beach (HOA + Marin Co co-jurisdiction)
--   - fid 8392 Cabrillo Beach (Catalina Island; ambiguous operator)

-- ═══════════════════════════════════════════════════════════════════
-- M1 — Point Reyes NS — NPS pets page (pets prohibited on west-side
--      Tomales Bay beaches per May 2023 rule change; PRNS-administered
--      beaches with NPS as primary authority)
-- ═══════════════════════════════════════════════════════════════════

-- 1a. NPS Point Reyes pets-page policy_source row.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'NPS Point Reyes National Seashore — Pets',
       (SELECT id FROM public.agency WHERE name='National Park Service' AND type='federal_department' LIMIT 1),
       ARRAY['dog_policy']::text[],
       'https://www.nps.gov/pore/planyourvisit/pets.htm',
       'NPS Point Reyes National Seashore — Pets (planyourvisit/pets.htm). "Pets are allowed in parking lots, along public roads, and along with the trails and beaches described below. All other trails, beaches, and off-trail lands within Point Reyes National Seashore and the Northern District of Golden Gate National Recreation Area are closed to the possession of pets. Always keep your pets on a leash." Ocean-facing beaches where leashed pets are allowed year-round: Kehoe Beach (north of Kehoe Beach trail); Limantour Beach (SE of parking lot to beach adjacent to Coast Camp); Point Reyes/Great Beach (North Beach parking lot south to historic Navy installation ~1 mile south of South Beach parking lot). Tomales Bay rule (May 30, 2023 change): "As of May 30, 2023, pets are no longer permitted on National Seashore beaches on the west side of Tomales Bay. In addition to the National Seashore beaches on the west side of Tomales Bay, pets are not permitted on: Hog Island, Duck Island, or Pelican Point; beaches within Tomales Bay State Park." Parent citation chain: 36 CFR §2.15 (Pets) → NPS Point Reyes Pet Policy → may be operationalized in PORE Superintendent''s Compendium. [Source: nps.gov/pore/planyourvisit/pets.htm. Fetched verbatim 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'NPS Point Reyes National Seashore — Pets'
);

-- 1b. Link PRNS-administered beaches.
--
-- fid 6009 Alan P Sieroty Beach (38.108, -122.842) — east side of
--   Tomales Bay; PRNS-administered shoreline outside Tomales Bay SP
--   envelope. Despite being EAST-side of Tomales Bay (the post-2023
--   prohibition specifically names WEST-side beaches), the broader
--   PRNS pets rule applies and this is not on the allowed-beaches
--   list. Conservative: not_allowed.
--
-- fid 4510 Shallow Beach (38.124, -122.882) — west side of Tomales
--   Bay (Inverness Ridge side); PRNS-administered, west-side beach
--   subject to the May 2023 prohibition explicitly: not_allowed.
--
-- fid 6460 Brazil Beach (38.234, -122.957) — Marin coastal park
--   shows park_name='Greater Farallones National Marine Sanctuary'
--   but coords are inland on Tomales Bay shoreline (north end, near
--   Dillon Beach area). Land authority is most likely PRNS (Tomales
--   Bay west side) — not the marine sanctuary (which is offshore).
--   not_allowed via PRNS pets rule.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid,
       (SELECT id FROM public.policy_source WHERE citation = 'NPS Point Reyes National Seashore — Pets'),
       'sand', v.rule, 'operative'::operative_status,
       v.evidence,
       'https://www.nps.gov/pore/planyourvisit/pets.htm',
       v.note
FROM (VALUES
  (6009, 'not_allowed',
   'NPS Point Reyes pets page: "All other trails, beaches, and off-trail lands within Point Reyes National Seashore... are closed to the possession of pets." Alan P Sieroty Beach is not on the named-allowed list (Kehoe, Limantour, Point Reyes/Great Beach). East-side Tomales Bay PRNS-administered shoreline.',
   'East-side Tomales Bay PRNS shoreline; not a Tomales Bay SP beach. Default-prohibited per PRNS general rule.'),
  (4510, 'not_allowed',
   'NPS Point Reyes pets page (May 2023 update): "As of May 30, 2023, pets are no longer permitted on National Seashore beaches on the west side of Tomales Bay." Shallow Beach (west side of Tomales Bay, PRNS-administered) falls squarely under this prohibition.',
   'West-side Tomales Bay PRNS beach — explicitly named in the May 2023 closure scope.'),
  (6460, 'not_allowed',
   'NPS Point Reyes pets page: "All other trails, beaches, and off-trail lands within Point Reyes National Seashore... are closed to the possession of pets." Brazil Beach is PRNS-administered north-end Tomales Bay shoreline; not on the named-allowed list.',
   'Beaches_gold park_name lists Greater Farallones NMS but coords (38.234, -122.957) are inland on Tomales Bay; PRNS-administered land authority. NMS is a marine-water designation; land authority is PRNS/private.')
) AS v(fid, rule, evidence, note)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ═══════════════════════════════════════════════════════════════════
-- M2 — Tomales Bay State Park (CA DPR per-park, page_id=470).
--      DPR administers TBSP via long-standing arrangement within
--      PRNS boundaries; DPR per-park rule governs visitor conduct
--      on the state-park land. Cross-reinforced by the PRNS pets
--      page which explicitly says "pets are not permitted on...
--      beaches within Tomales Bay State Park." Both authorities
--      agree: not_allowed.
-- ═══════════════════════════════════════════════════════════════════

-- 2a. CA DPR per-park policy_source row for Tomales Bay State Park.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Tomales Bay State Park',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/?page_id=470',
       'California Department of Parks and Recreation — Tomales Bay State Park (Marin County; eastern edge of Point Reyes Peninsula). Dog policy (verbatim from parks.ca.gov/?page_id=470 "Are dogs Allowed?" section): "Yes: Dogs allowed only at Vista Point Picnic Area." Important Rules: "Dogs are not allowed on any of the beaches or trails at Tomales Bay State Park. Except for service animals, dogs are only permitted at the Vista Point picnic area and must remain on leash at all times." Park is administered by CA DPR within the broader PRNS envelope; DPR rule governs on the state-park land. Cross-reinforced by NPS Point Reyes pets page (May 2023): "pets are not permitted on... beaches within Tomales Bay State Park." [Source: parks.ca.gov page_id=470 (resolved via Complete State Parks Listing page_id=21805). Fetched verbatim 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Tomales Bay State Park'
);

-- 2b. Link the 5 Tomales Bay SP beaches.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid,
       (SELECT id FROM public.policy_source WHERE citation = 'California State Parks dog policy — Tomales Bay State Park'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'CA DPR Tomales Bay State Park per-park rule: "Dogs are not allowed on any of the beaches or trails at Tomales Bay State Park. Except for service animals, dogs are only permitted at the Vista Point picnic area and must remain on leash at all times." Cross-reinforced by NPS Point Reyes pets page: "pets are not permitted on... beaches within Tomales Bay State Park."',
       'https://www.parks.ca.gov/?page_id=470',
       'CA DPR administers Tomales Bay SP within the broader PRNS envelope; DPR rule operative on state-park land. Vista Point picnic-area carve-out is the only on-leash dog area in the park; beaches proper are off-limits.'
FROM (VALUES (8718),(8719),(5924),(6043),(6332)) AS v(fid)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- 2c. Also link these 5 to the NPS PRNS pets page as a cross-
--     reinforcing source (PRNS explicitly names TBSP beaches in its
--     prohibition). This documents the layered authority chain.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid,
       (SELECT id FROM public.policy_source WHERE citation = 'NPS Point Reyes National Seashore — Pets'),
       'sand_nps_overlay', 'not_allowed', 'operative'::operative_status,
       'NPS Point Reyes pets page (May 2023): "In addition to the National Seashore beaches on the west side of Tomales Bay, pets are not permitted on... beaches within Tomales Bay State Park." NPS explicitly extends its no-pets rule across the state-park land that sits within the PRNS envelope.',
       'https://www.nps.gov/pore/planyourvisit/pets.htm',
       'Cross-reinforcing NPS overlay on the CA DPR per-park rule. Both authorities agree: not_allowed. Used a distinct section value to avoid the (beach_fid, policy_source_id, section) PK conflict with the DPR row.'
FROM (VALUES (8718),(8719),(5924),(6043),(6332)) AS v(fid)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ═══════════════════════════════════════════════════════════════════
-- M3 — Mount Tamalpais State Park beaches (CA DPR page_id=471).
--      Park rule: "Dogs allowed only in developed areas. Dogs not
--      allowed in the following areas: trails, dirt roads, and
--      backcountry areas." All three of these beaches are coastal
--      access via Mt Tam SP trails (Steep Ravine, Rocky Point) —
--      so the trails-and-backcountry prohibition makes them
--      not_allowed in practice.
-- ═══════════════════════════════════════════════════════════════════

-- 3a. CA DPR per-park policy_source row for Mount Tamalpais State Park.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Mount Tamalpais State Park',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/?page_id=471',
       'California Department of Parks and Recreation — Mount Tamalpais State Park (Marin County; north of Golden Gate Bridge). Dog policy (verbatim from parks.ca.gov/?page_id=471 "Are dogs Allowed?" section): "Yes: Dogs allowed only in developed areas. Dogs not allowed in the following areas: trails, dirt roads, and backcountry areas." Independent verification from Mt Tam SP staff: "Dogs (except for service animals) are ONLY allowed on paved roads, Old Stage Fire Rd, Verna Dunshee Trail, in developed areas, campgrounds, and picnic areas." Steep Ravine Campground and cabins exclude dogs except service animals. Coastal beaches accessed via Mt Tam SP trails (Steep Ravine Trail, Rocky Point Trail to Red Rock Beach) fall under the trails prohibition. [Source: parks.ca.gov page_id=471 (resolved via Complete State Parks Listing page_id=21805). Fetched verbatim 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation = 'California State Parks dog policy — Mount Tamalpais State Park'
);

-- 3b. Link the 3 Mt Tam SP beaches.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid,
       (SELECT id FROM public.policy_source WHERE citation = 'California State Parks dog policy — Mount Tamalpais State Park'),
       'sand', 'not_allowed', 'operative'::operative_status,
       v.evidence,
       'https://www.parks.ca.gov/?page_id=471',
       v.note
FROM (VALUES
  (6116, 'Steep Ravine Beach is accessed only via the Steep Ravine Trail in Mt Tam SP. Mt Tam SP per-park rule: "Dogs not allowed in... trails, dirt roads, and backcountry areas." Steep Ravine campground/cabins also exclude dogs (except service animals).',
   'Trail-only access; subject to the trails prohibition. Tier-3 (limited_access) classification may be a mis-classification — this is functionally tier-4 (not_allowed) for general visitors. Flag for tier reclassification follow-up.'),
  (8240, 'Red Rock Beach is reached via the Rocky Point Trail within Mt Tam SP. Mt Tam SP per-park rule: "Dogs not allowed in... trails, dirt roads, and backcountry areas." Independent source: "No dogs or bikes are permitted on the Rocky Point Trail or on Red Rock Beach."',
   'Coastal beach accessed via Mt Tam SP trail; trails prohibition applies. Clothing-optional beach. Tier-3 classification likely should be tier-4 — flag for reclassification.'),
  (454, 'Mickey''s Beach Trail is within Mt Tam SP. Mt Tam SP per-park rule: "Dogs not allowed in... trails, dirt roads, and backcountry areas." Independent source: trail is marked "No dogs" in hiking guides.',
   'Trail-only access; subject to the trails prohibition. Tier-3 classification likely should be tier-4 — flag for reclassification.')
) AS v(fid, evidence, note)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ═══════════════════════════════════════════════════════════════════
-- M4 — Unincorporated Marin County (Bolinas Beach, Chicken Ranch
--      Beach, Brickyard Beach). No state-park overlay; Marin County
--      animal code (Chapter 8.04) is the operative authority. Reuse
--      existing ps id 185 (Marin County Code §8.04.176 — citation is
--      San-Quentin-specific but the underlying nodeId targets the
--      full Chapter 8.04 Animal Control). Add a second ps row for
--      the broader Marin County §8.04.175 dog-control-by-responsible-
--      person rule, which is the actual countywide leash rule
--      applicable here. (Existing ps 185 is San-Quentin-Village-
--      specific by citation but useful as a TIT8AN_CH8.04ANCO link;
--      §8.04.175 is the operative countywide responsible-person
--      provision.)
-- ═══════════════════════════════════════════════════════════════════

-- 4a. Marin County §8.04.175 — dog control by responsible person
--     (countywide responsibility-person rule). Distinct from ps 185
--     which is the San Quentin Village leash-required section.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Marin County Code §8.04.175 (Dog control by responsible person) + §8.04.160 (Dogs running in certain public areas) — Title 8 Chapter 8.04 Animal Control',
       (SELECT id FROM public.agency WHERE name='Marin County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/ca/marin_county/codes/municipal_code?nodeId=TIT8AN_CH8.04ANCO_8.04.175DOCONERESPE',
       'Marin County Code Title 8 (Animals), Chapter 8.04 (Animal Control). §8.04.175 Dog control by responsible person — countywide rule requiring dogs to be under the immediate control of a competent responsible person at all times. §8.04.160 Dogs running in certain public areas: "It is unlawful for the owner/guardian or person having control of any dog to permit the same, under any circumstances, to run at large in any public parks, or in any school, or upon any school grounds, or in any commercial district, or in any game refuge, or in any public watershed area; and every dog found running at large in violation of the provisions of this section shall be immediately seized and impounded." §8.04.176 Leash required in San Quentin Village: 6-ft leash required in San Quentin Village (location-specific carve-out). NOTE: Chapter 8.04 contains NO beach-specific prohibition in Marin County — operative beach rule for unincorporated Marin shorelines is the general §8.04.175 responsible-person rule (+ public-park leash via §8.04.160), neither of which prohibits off-leash on a beach absent a posted designation. [Hosted on library.municode.com; nodeId TIT8AN_CH8.04ANCO. Verified via Playwright fetch 2026-05-17. Section-level deep link to §8.04.175.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Marin County Code §8.04.175%'
);

-- 4b. Link the 3 unincorporated Marin beaches.
--
-- fid 1796 Bolinas Beach (37.906, -122.683) — Bolinas, unincorp Marin;
--   well-documented off-leash community standard. Marin County §8.04.175
--   responsible-person rule applies; no beach-specific prohibition.
--   Rule = off_leash (community-recognized off-leash beach with §8.04.175
--   responsible-person backstop).
--
-- fid 8799 Chicken Ranch Beach (38.110, -122.865) — Inverness, west side
--   of Tomales Bay; Marin County park (parks.marincounty.gov posted rule
--   = leashed). Marin County §8.04.175 + Marin County Parks designation
--   = on_leash.
--
-- fid 7160 Brickyard beach (37.985, -122.467) — Tiburon peninsula; small
--   inlet shoreline; reported dog-friendly in Sausalito/Tiburon area
--   guides. Marin County §8.04.175 responsible-person rule.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Marin County Code §8.04.175%'),
       'sand', v.rule, 'operative'::operative_status,
       v.evidence,
       'https://library.municode.com/ca/marin_county/codes/municipal_code?nodeId=TIT8AN_CH8.04ANCO_8.04.175DOCONERESPE',
       v.note
FROM (VALUES
  (1796, 'off_leash',
   'Bolinas Beach is in unincorporated Marin County (no city overlay). Marin County Code Chapter 8.04 contains no beach-specific prohibition; §8.04.175 (dog control by responsible person) is the operative countywide rule. Bolinas Beach is the recognized off-leash standard in Marin per multiple community + visitor-bureau sources.',
   'Community-recognized off-leash beach. Marin Convention & Visitors Bureau lists it as one of the few off-leash beaches in Marin. §8.04.175 responsible-person backstop applies.'),
  (8799, 'on_leash',
   'Chicken Ranch Beach is a Marin County Park (parks.marincounty.gov/parkspreserves/parks/chicken-ranch-beach). County rule: "Friendly dogs on leash are welcome. Dogs must be on a leash not exceeding 6 feet in length. Clean up and dispose of pet waste." West-side Tomales Bay; not on PRNS land.',
   'Marin County Park; on-leash via county park posted rule + §8.04.175 + §8.04.160 (parks leash). Outside PRNS land authority.'),
  (7160, 'on_leash',
   'Brickyard Beach is on the Tiburon peninsula in unincorporated Marin County. Marin County Code §8.04.175 (dog control by responsible person) is the operative countywide rule. No beach-specific prohibition.',
   'Coords (37.985, -122.467) on Tiburon peninsula. Marin County animal code applies; absent a posted carve-out, default is responsible-person/under-control (functionally on_leash in a populated shore area).')
) AS v(fid, rule, evidence, note)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ═══════════════════════════════════════════════════════════════════
-- DEFERRED (Marin):
--   fid 337  Gallinas Beach (San Rafael)        — bay-edge sanitary
--            district adjacency; weak Marin County authority signal;
--            no posted rule found. Defer pending site verification.
--   fid 1307 Teachers Beach (38.114, -122.870)  — location not
--            uniquely identifiable in public sources; coords near
--            Inverness/Tomales Bay west side which would normally
--            be PRNS-administered, but the name doesn't match any
--            named PRNS beach. Defer.
--   fid 1783 Dillon Beach                        — private community
--            beach owned by Dillon Beach Resort. Off-leash on the
--            beach below the high tide line is the operator-posted
--            rule, but the operator is a private entity (not an
--            agency) and no statutory policy_source applies. Should
--            be modeled as operator_posted_policy with the resort
--            as issuing_operator once the operator entity is
--            created. Defer to operator-side migration.
--   fid 8238 Seadrift Beach (Stinson)            — gated private
--            HOA community (~300 homes). Private beach below high
--            tide is California-public via the public-trust
--            doctrine + legal easement, but the operator is the
--            Seadrift HOA. Co-jurisdiction with Marin County
--            §8.04.175. Defer pending operator entity + HOA-
--            operator-attribution policy.
-- ═══════════════════════════════════════════════════════════════════

-- ═══════════════════════════════════════════════════════════════════
-- LA1 — LA City beaches (Cabrillo Beach San Pedro, fid 8555)
--       Operative authority: City of LA Department of Recreation
--       and Parks via LAMC §63.44 (Park & Recreation Regulations)
--       + §63.02 (Beach lands jurisdiction). Cabrillo Beach is
--       operated by LA Dept of Rec & Parks (Region: Pacific;
--       Neighborhood Service Area: Harbor) per
--       recreation.parks.lacity.gov/beach/cabrillo-beach.
-- ═══════════════════════════════════════════════════════════════════

-- LA1a. LAMC §63.44 + per-beach LA City Rec & Parks operator page.
--       The amlegal deep link uses the LAMC numeric anchor 0-0-0-
--       159459 for §63.44.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'LAMC §63.44 (Regulations Affecting Park and Recreation Areas) + §63.02 (Beach lands jurisdiction)',
       (SELECT id FROM public.agency WHERE name='Los Angeles' AND type='city' LIMIT 1),
       ARRAY['dog_policy']::text[],
       'https://codelibrary.amlegal.com/codes/los_angeles/latest/lamc/0-0-0-159459',
       'Los Angeles Municipal Code Chapter VI (Public Works and Property) Article 3 (Public Parks, Playgrounds, Beaches and Other Property). §63.44 Regulations Affecting Park and Recreation Areas (added Ord. 153,027 eff. 11/16/79; amended Ord. 187,525 eff. 7/16/22). Definitions: "Beach" means public seashore and shoreline areas bordering the Pacific Ocean that are owned, managed or controlled by the City. "Park" includes every public park, roadside rest area, playground, zoological garden, ocean, beach or other recreational facility area... in the City of Los Angeles and under the control, operation or management of the Board of Recreation and Park Commissioners, the Los Angeles County Department of Parks and Recreation, the Los Angeles County Department of Beaches, or the Los Angeles Memorial Coliseum Commission. Operative dog rule: dogs prohibited on the beach except in Board-designated dog parks; the boardwalk-adjacent-to-beach carve-out runs Memorial Day through October 31, 11am-8pm on weekends/holidays. §63.02 vests jurisdiction over LA City beach lands in the Department of Recreation and Parks. Cross-referenced by LAMC §53.06.2 (Restraint of Dogs — citywide leash law). [Hosted on codelibrary.amlegal.com; deep link to §63.44 via numeric anchor 0-0-0-159459. Fetched 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'LAMC §63.44%'
);

-- LA1b. Link Cabrillo Beach San Pedro (fid 8555).

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8555,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'LAMC §63.44%'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'LAMC §63.44 prohibits dogs on LA City beaches except in Board-designated dog parks. Cabrillo Beach (San Pedro) is operated by LA Dept of Rec & Parks (recreation.parks.lacity.gov/beach/cabrillo-beach); no dog-beach designation exists. Multiple sources confirm: "No dogs are allowed on Cabrillo Beach."',
       'https://codelibrary.amlegal.com/codes/los_angeles/latest/lamc/0-0-0-159459',
       'San Pedro Cabrillo Beach (33.71, -118.28). LA Dept of Rec & Parks operated; LAMC §63.44 + §53.06.2 apply. Tier-3 classification may be a mis-classification — operative rule is not_allowed (functionally tier-4). Flag for tier reclassification follow-up.'
WHERE NOT EXISTS (
  SELECT 1 FROM public.beach_policy_source
   WHERE beach_fid = 8555
     AND policy_source_id = (SELECT id FROM public.policy_source WHERE citation LIKE 'LAMC §63.44%')
     AND section = 'sand'
);

-- ═══════════════════════════════════════════════════════════════════
-- LA2 — LA County Beaches & Harbors (Mother's Beach / Marina Beach,
--       fid 8537). Operated by LA County Department of Beaches &
--       Harbors at beaches.lacounty.gov/marina-beach/. Reuse
--       existing ps id 45 (LA County Beaches & Harbors rules page)
--       AND existing ps id 6 (LA County Code Title 17). Both
--       sources concur: "NO Animals allowed on the beach (no cats,
--       dogs, horses, etc.)" per LA County Beach Rules.
-- ═══════════════════════════════════════════════════════════════════

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8537, 45, 'sand', 'not_allowed', 'operative'::operative_status,
       'LA County Beach Rules (beaches.lacounty.gov/la-county-beach-rules/): "NO Animals allowed on the beach (no cats, dogs, horses, etc.)" Mother''s Beach / Marina Beach is operated by LA County Beaches & Harbors per beaches.lacounty.gov/marina-beach/. Conflicting third-party guides exist but the official LA County rules page is dispositive.',
       'https://beaches.lacounty.gov/la-county-beach-rules/',
       'Marina Beach (a.k.a. Mother''s Beach), Marina del Rey. LA County Beaches & Harbors operated. Tier-3 classification likely should be tier-4 — flag for reclassification. The "dog-friendly" claims on third-party sites are factually incorrect per the official LA County rules.'
WHERE NOT EXISTS (
  SELECT 1 FROM public.beach_policy_source
   WHERE beach_fid = 8537 AND policy_source_id = 45 AND section = 'sand'
);

-- Also link to LA County Code Title 17 as the statutory backstop.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8537, 189, 'sand', 'not_allowed', 'operative'::operative_status,
       'LA County Code §17.04.410 (Dogs and Cats Permitted When) — Title 17 Ch. 17.04 Part 3: 6-ft leash baseline in parks excluding golf courses; LA County Beach Rules narrow this to NO animals on the beach proper. The county-operator (Beaches & Harbors) site rule is dispositive.',
       'https://library.municode.com/ca/los_angeles_county/codes/code_of_ordinances?nodeId=TIT17PABEOTPUAR_CH17.04PAREAR_PT3PARURE_17.04.410DOCAPEWH',
       'Statutory backstop. Operative narrowing is the LA County Beaches & Harbors posted site rule (ps id 45).'
WHERE NOT EXISTS (
  SELECT 1 FROM public.beach_policy_source
   WHERE beach_fid = 8537 AND policy_source_id = 189 AND section = 'sand'
);

-- ═══════════════════════════════════════════════════════════════════
-- LA3 — Independent LA-area cities (Redondo Beach Pier fid 7404,
--       El Segundo Beach fid 8895, Redondo Beach State Park fid 8731).
--
--       Redondo Beach (fid 7404 + 8731): both have City of Redondo
--       Beach jurisdiction. fid 8731 named "Redondo Beach State
--       Park" is NOT a CA DPR state park (no DPR page exists for
--       it). Coords (33.83, -118.39) place it in the City of
--       Redondo Beach; functionally a city beach. Both governed by
--       Redondo Beach Municipal Code §5-1.103 which prohibits dogs
--       on any beach citywide.
--
--       El Segundo Beach (fid 8895): the City of El Segundo's
--       coastal frontage; operated by LA County Beaches & Harbors
--       under the standard LA County beach operations envelope.
--       Both LA County Beach Rules (ps 45) and El Segundo
--       municipal practice align with "no dogs on the beach."
--       Direct El Segundo Municipal Code section not located via
--       Akamai-blocked elsegundo.gov fetch; reuse LA County
--       Beaches & Harbors row (ps 45) as the operator-posted rule
--       authority for the El Segundo beach.
-- ═══════════════════════════════════════════════════════════════════

-- LA3a. Redondo Beach Municipal Code §5-1.103 — dogs prohibited on
--       any beach citywide.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Redondo Beach Municipal Code §5-1.103 (Dogs running at large; beach prohibition)',
       (SELECT id FROM public.agency WHERE name='Redondo Beach' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://library.qcode.us/lib/redondo_beach_ca/pub/municipal_code/item/title_5-chapter_1-5_1_103',
       'Redondo Beach Municipal Code Title 5 (Sanitation and Health), Chapter 1, §5-1.103: "It is unlawful for any person to permit a dog to run at large on any public street, alley, lane, park or other public place unless restrained by a substantial chain or leash not exceeding six feet in length and in the charge, care, custody or control of a competent person; provided, however, that no dog shall be allowed or permitted on any beach." Citywide beach prohibition. Independent backstop: §3-16.03 Fisherman''s Wharf area additionally prohibits dogs with or without a leash in that specific commercial area. Citywide leash requirement under §5-1.103 applies to streets/parks/public places. [Hosted on library.qcode.us (qcode → ecode360 redirect). General Code platform. Verified 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Redondo Beach Municipal Code §5-1.103%'
);

-- LA3b. Link Redondo Beach Pier (fid 7404) + Redondo Beach State Park (fid 8731).

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Redondo Beach Municipal Code §5-1.103%'),
       'sand', 'not_allowed', 'operative'::operative_status,
       v.evidence,
       'https://library.qcode.us/lib/redondo_beach_ca/pub/municipal_code/item/title_5-chapter_1-5_1_103',
       v.note
FROM (VALUES
  (7404, 'Redondo Beach Municipal Code §5-1.103: "no dog shall be allowed or permitted on any beach." Citywide prohibition applies to all Redondo Beach Pier shoreline including the pier-adjacent sand.',
   'Pier-adjacent sand; subject to §5-1.103 citywide beach prohibition. Tier-3 classification likely should be tier-4 — flag for reclassification.'),
  (8731, 'Despite the name "Redondo Beach State Park", this beach is NOT a CA DPR state park (no DPR per-park page exists; not in parks.ca.gov/?page_id=21805 listing). Coords (33.83, -118.39) place it within the City of Redondo Beach. Redondo Beach Municipal Code §5-1.103 applies: "no dog shall be allowed or permitted on any beach."',
   'Name is misleading — this is functionally a Redondo Beach city beach, not a CA DPR state park. Tier-3 classification likely should be tier-4. Flag for name correction + tier reclassification.')
) AS v(fid, evidence, note)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- LA3c. El Segundo Beach (fid 8895) — link to LA County Beaches &
--       Harbors operator rule (ps 45). El Segundo's coastal
--       frontage is operated by LA County Beaches & Harbors under
--       standard county-beach rules; direct El Segundo MC section
--       not captured this pass (akamai 403 on elsegundo.gov).

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8895, 45, 'sand', 'not_allowed', 'operative'::operative_status,
       'El Segundo Beach is operated by LA County Department of Beaches & Harbors as part of the standard LA County beach operations envelope. LA County Beach Rules: "NO Animals allowed on the beach (no cats, dogs, horses, etc.)" Multiple independent sources confirm: "no dogs are allowed on the beach, bike path or parking lot" at El Segundo Beach.',
       'https://beaches.lacounty.gov/la-county-beach-rules/',
       'LA County Beaches & Harbors operated. Tier-3 classification likely should be tier-4 — flag for reclassification. El Segundo MC section not captured this pass (elsegundo.gov blocked by Akamai); operator-posted rule is dispositive. Direct ESMC capture deferred.'
WHERE NOT EXISTS (
  SELECT 1 FROM public.beach_policy_source
   WHERE beach_fid = 8895 AND policy_source_id = 45 AND section = 'sand'
);

-- ═══════════════════════════════════════════════════════════════════
-- DEFERRED (LA mainland):
--   fid 8392 "Cabrillo Beach" (33.42, -118.40) — coords place this
--            beach on Santa Catalina Island (Two Harbors area), NOT
--            San Pedro. Confirmed via visitcatalinaisland.com/two-
--            harbors/boating/boat-in-camping/cabrillo-beach/ — a
--            boat-in primitive campsite operated by Catalina Island
--            Company. Defer pending:
--              - dedup with fid 8555 (likely a misplaced duplicate
--                of the San Pedro Cabrillo); OR
--              - Catalina Island Conservancy / Catalina Island
--                Company operator entity + operator_posted_policy
--                attribution.
--            Unincorporated LA County (not in Avalon city limits;
--            Two Harbors is unincorporated), so LA County Code
--            Title 17 / §17.04.410 (6-ft leash baseline) could
--            apply as the statutory floor — but the operator's
--            posted rule is what visitors actually encounter.
-- ═══════════════════════════════════════════════════════════════════
