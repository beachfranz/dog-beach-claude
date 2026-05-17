-- Bucket D: state + federal backfills for 7 unlinked CA tier-1+2
-- beaches whose primary dog_policy governance is state-agency or
-- federal. Three sub-clusters:
--
--   1) CDWR — 4 Castaic Lagoon beaches (fids 8234, 8248, 8249, 8250)
--      Castaic Lake State Recreation Area is CDWR-owned (State Water
--      Project terminal reservoir) and OPERATED BY LA COUNTY PARKS
--      under a long-standing operating agreement. The DPR page itself
--      (parks.ca.gov/?page_id=628) says "This site is operated by Los
--      Angeles County" and defers visitor rules to LA County. The
--      operative visitor rule is therefore LA County Code Title 17
--      (Chapter 17.04 — Parks and Recreation Areas), §17.04.410 (Dogs
--      and Cats Permitted When — 6-ft leash baseline), reinforced by
--      LA County Parks' Castaic Lake site rule ("Animals must be on
--      leashes. No animals are allowed in the water or on the beach.").
--
--      This is the inverse of the Crown Memorial bounded-operator case
--      from [[walkthrough-crown-memorial]]: gov-owned land, gov-county
--      operator, county code applies because the county is the
--      operator and brings its own animal regulations to the leased
--      site. Reverse-geocoded 2026-05-17 via nominatim — all 4 beach
--      coords fall within the Castaic Lagoon bbox (34.497–34.514,
--      −118.614 to −118.607); NOT Pyramid Lake (which sits at 34.66,
--      −118.78). Castaic Lagoon is the lower-lake afterbay.
--
--   2) BLM — 2 Humboldt Bay South Spit beaches (fids 3151, 6169)
--      Governance shows CDFW (p=1), but in practice the operating
--      authority for the South Spit Cooperative Management Area is the
--      BLM Arcata Field Office. The area is part of the "Mike Thompson
--      Wildlife Area, South Spit Humboldt Bay" — owned by the State of
--      California and co-managed by BLM and CDFW, but day-to-day
--      visitor rules are set by BLM via 2009 supplementary rules
--      (Federal Register Vol. 74 No. 102, May 29 2009, pp. 25767–
--      25771, 43 CFR §8365.1-6 authority). Operative rule:
--        "Dogs must be leashed on the west side of South Jetty Road
--         March 1–September 15, and must be under the owner's control
--         at all times."
--      Snowy plover seasonal protection (Mar 1–Sep 15) drives the
--      seasonal leash requirement. CDFW governance is correct as the
--      land-owner reference but does not write the operative visitor
--      rule. Recommend follow-up governance-resolver pass to elevate
--      BLM Arcata Field Office to precedence-1 here.
--
--   3) CA DPR — San Onofre State Beach (fid 9719)
--      Governance shows DoD (p=1) + San Diego County (p=1). San Onofre
--      State Beach is LEASED from the U.S. Marine Corps (Camp
--      Pendleton) to CA State Parks and operated as a state beach.
--      The visitor rule on the leased land is CA DPR's, not DoD/USMC's
--      — DoD governance is an FYI about the underlying land, not the
--      operative authority for dogs. Resolved per-park via
--      parks.ca.gov/?page_id=647 (San Onofre State Beach detail page,
--      confirmed via Complete State Parks Listing page_id=21805).
--      Operative rule (verbatim from parks.ca.gov/?page_id=647):
--        "Except for service animals, dogs not allowed in buildings or
--         on the beach."

-- ─── 1. CDWR/LA County — Castaic Lagoon (4 beaches) ─────────────────

-- 1a. CDWR State Water Project recreation administrative policy
--     (catchall baseline — CDWR owns and operates the reservoir, but
--     defers visitor rules to the LA County operator).

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'CDWR State Water Project recreation — Castaic Lake (operated by LA County under operating agreement)',
       (SELECT id FROM public.agency WHERE name='California Department of Water Resources' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://water.ca.gov/What-We-Do/Recreation/Castaic-Lake-Recreation',
       'California Department of Water Resources — Castaic Lake Recreation. CDWR owns and operates Castaic Lake as part of the State Water Project (terminal reservoir). Day-to-day recreation management is delegated to Los Angeles County Department of Parks and Recreation, which operates Castaic Lake State Recreation Area under a longstanding operating agreement. Visitor rules (including dog/pet rules) are set by the LA County operator under LA County Code Title 17 Chapter 17.04. CDWR retains environmental monitoring (algal bloom advisories: "Keep yourself and pets away from algal blooms"). [Source: water.ca.gov/What-We-Do/Recreation/Castaic-Lake-Recreation. Fetched 2026-05-17. Reverse-geocode confirmed coords 34.50/-118.61 sit in Castaic Lagoon bbox — the lower-lake afterbay inside the SRA — NOT Pyramid Lake (34.66/-118.78). Per-place CDWR page rather than the generic recreation catalog.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'CDWR State Water Project recreation — Castaic Lake%'
);

-- 1b. LA County Code §17.04.410 — operative visitor rule for dogs in
--     LA County Parks (including the Castaic Lake SRA operator role).

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'LA County Code §17.04.410 (Dogs and Cats Permitted When) — Title 17 Ch. 17.04 Part 3',
       (SELECT id FROM public.agency WHERE name='Los Angeles County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/ca/los_angeles_county/codes/code_of_ordinances?nodeId=TIT17PABEOTPUAR_CH17.04PAREAR_PT3PARURE_17.04.410DOCAPEWH',
       'LA County Code Title 17 (Parks, Beaches and Other Public Areas), Chapter 17.04 (Parks and Recreation Areas), Part 3 (Park Rules and Regulations), §17.04.410 Dogs and Cats Permitted When: "A person may bring and maintain, in any park exclusive of golf courses, a dog or cat if such dog or cat is kept on a leash or chain no more than six feet in length and under full control of its owner or person responsible for the animal, or is confined in a vehicle, tent, trailer, enclosure, or other structure allowed by park rules and in accordance with applicable State law." §17.04.412 Dog Off-Leash Area Rules and Regulations: dogs only permitted unleashed in designated Dog Off-Leash Areas. Site-specific Castaic Lake rule posted by LA County Parks operator: "Animals must be on leashes. No animals are allowed in the water or on the beach." [Hosted on library.municode.com; nodeId TIT17PABEOTPUAR_CH17.04PAREAR_PT3PARURE_17.04.410DOCAPEWH. Site-specific Castaic posted rule fetched from castaiclake.com/rules-regulations 2026-05-17. Resolved via Playwright TOC fetch 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'LA County Code §17.04.410%'
);

-- 1c. Link all 4 Castaic Lagoon beaches to CDWR catchall (baseline)
--     and to LA County §17.04.410 (operative).
--     Rule = not_allowed (no animals in water or on beach per posted
--     Castaic Lake site rule; LA County 6-ft-leash rule applies in
--     trails/picnic areas but the section='sand' is the beach proper).

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'LA County Code §17.04.410%'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'Castaic Lake site-posted rule (LA County Parks operator): "Animals must be on leashes. No animals are allowed in the water or on the beach." This site rule narrows the LA County §17.04.410 6-ft-leash baseline to prohibit animals on the lake beach proper. Reference: castaiclake.com/rules-regulations.',
       'https://library.municode.com/ca/los_angeles_county/codes/code_of_ordinances?nodeId=TIT17PABEOTPUAR_CH17.04PAREAR_PT3PARURE_17.04.410DOCAPEWH',
       'CDWR-owned reservoir, LA-County-operated under operating agreement. Dogs welcome on leash in trails and picnic areas under §17.04.410; site rule prohibits animals on beach + in water. Walkthrough recommended for sub-area carve-outs.'
FROM (VALUES (8234),(8248),(8249),(8250)) AS v(fid)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- 1d. Also link the 4 Castaic beaches to the CDWR baseline as a
--     reference (not the operative rule; baseline reflecting the
--     land-owner administrative policy).

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'CDWR State Water Project recreation — Castaic Lake%'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'CDWR State Water Project — Castaic Lake (terminal reservoir). CDWR owns and operates the lake; recreation operations delegated to LA County Parks. Operative dog rule is the LA County operator''s site rule + LA County Code §17.04.410. CDWR row provides land-owner-baseline reference.',
       'https://water.ca.gov/What-We-Do/Recreation/Castaic-Lake-Recreation',
       'Land-owner-baseline reference; operative rule is LA County §17.04.410 + Castaic Lake site-posted rule.'
FROM (VALUES (8234),(8248),(8249),(8250)) AS v(fid)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 2. BLM — Humboldt Bay South Spit (2 beaches) ──────────────────

-- 2a. BLM Arcata Field Office supplementary rules (2009 Fed Reg) for
--     the South Spit Cooperative Management Area.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'federal_regulation',
       'BLM Arcata FO Supplementary Rules for South Spit Cooperative Management Area (74 Fed. Reg. 25767, May 29 2009; 43 CFR §8365.1-6 authority)',
       (SELECT id FROM public.agency WHERE name='United States Bureau of Land Management' AND type='federal_department'),
       ARRAY['dog_policy']::text[],
       'https://www.federalregister.gov/documents/2009/05/29/E9-12515/establishment-of-interim-final-supplementary-rules-within-the-south-spit-cooperative-management-area',
       'BLM Establishment of Interim Final Supplementary Rules Within the South Spit Cooperative Management Area, Managed by the Arcata Field Office, California. 74 Fed. Reg. 25767 (May 29, 2009), pp. 25767–25771. Authority: 43 CFR §8365.1-6; FLPMA §§302, 310 (43 U.S.C. 1732, 1740). Operative rule (Section 2, Rule 4): "Dogs must be leashed on the west side of South Jetty Road March 1–September 15, and must be under the owner''s control at all times." Habitat Restoration Area closure: public use within the Habitat Restoration Area is not allowed March 1–September 15. Kites, model airplanes, and campfires not allowed within 300 feet of temporary or permanent plover protection areas. Western Snowy Plover (Charadrius alexandrinus nivosus) is listed-threatened (ESA 1970) and nests in the sand March–September. The South Spit is part of "Mike Thompson Wildlife Area, South Spit Humboldt Bay" — state-owned land co-managed by BLM and CDFW; BLM Arcata FO sets day-to-day visitor rules. [Source: federalregister.gov document E9-12515. Fetched 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'BLM Arcata FO Supplementary Rules for South Spit%'
);

-- 2b. Link South Jetty (fid 3151) and South Spit (fid 6169) — both
--     are on the west side of South Jetty Road / the spit's outer
--     beach. Default rule on_leash (March–Sept seasonal leash); year-
--     round under-owner-control required.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'BLM Arcata FO Supplementary Rules for South Spit%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'BLM South Spit CMA Supplementary Rules (2009) §2 Rule 4: "Dogs must be leashed on the west side of South Jetty Road March 1–September 15, and must be under the owner''s control at all times." Snowy plover protection (Mar–Sep nesting). Habitat Restoration Area closure may overlap parts of the beach during plover season.',
       'https://www.federalregister.gov/documents/2009/05/29/E9-12515/establishment-of-interim-final-supplementary-rules-within-the-south-spit-cooperative-management-area',
       'Seasonal leash (Mar 1–Sep 15) on west side of South Jetty Road; year-round owner control required. Plover-protection temporary closures marked by symbolic fencing/signs. Governance currently shows CDFW p=1 (land-owner); operative visitor authority is BLM Arcata FO via 2009 supplementary rules — flagged for governance-resolver review.'
FROM (VALUES (3151),(6169)) AS v(fid)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 3. CA DPR — San Onofre State Beach (fid 9719) ─────────────────

-- 3a. CA DPR per-park policy for San Onofre State Beach.
--     Note: San Onofre is LEASED from USMC Camp Pendleton to CA DPR;
--     DPR rules govern visitor conduct on the leased land. DoD/USMC
--     governance is an FYI about the underlying land-owner, not the
--     operative authority.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — San Onofre State Beach',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/?page_id=647',
       'California Department of Parks and Recreation — San Onofre State Beach (San Diego County; 3 miles south of San Clemente). Dog policy (verbatim from parks.ca.gov/?page_id=647 "Are dogs Allowed?" section): "Except for service animals, dogs not allowed in buildings or on the beach." Site context: San Onofre State Beach is leased from U.S. Marine Corps Base Camp Pendleton to CA State Parks under a long-standing lease agreement; the underlying federal land does not change the operative visitor rule, which is CA DPR''s. Standard CA DPR coastal-beach rule. [Source: parks.ca.gov page_id=647 (resolved via Complete State Parks Listing page_id=21805). Fetched 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'California State Parks dog policy — San Onofre%'
);

-- 3b. Link San Onofre State Beach to its DPR per-park policy row.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 9719,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'California State Parks dog policy — San Onofre%'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'CA DPR San Onofre State Beach dog policy (verbatim): "Except for service animals, dogs not allowed in buildings or on the beach." Standard CA DPR coastal-beach exclusion.',
       'https://www.parks.ca.gov/?page_id=647',
       'San Onofre is leased from USMC Camp Pendleton to CA State Parks; DPR rule governs visitor conduct on leased land. DoD/USMC land-owner governance does not change the operative dog rule. Walkthrough may surface trail-specific carve-outs.'
WHERE NOT EXISTS (
  SELECT 1 FROM public.beach_policy_source
   WHERE beach_fid = 9719
     AND policy_source_id = (SELECT id FROM public.policy_source WHERE citation LIKE 'California State Parks dog policy — San Onofre%')
     AND section = 'sand'
);
