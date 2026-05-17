-- Wave 5 CA: special-district backfills for 5 beaches attributed to
-- 4 special districts that had no policy_source rows.
--
-- Districts + per-beach disposition:
--
--   294 Big Bear Valley Recreation and Park District
--       9150 Swim Beach (Big Bear Lake, San Bernardino) — NO BBVRPD ps
--            INSERT. Operator publishes a swim-beach page but it is
--            silent on dogs/pets; no codified district rule is online.
--            Operative dog_policy authority is USFS (San Bernardino
--            National Forest) at precedence-1; already linked to
--            36 CFR §261.16 (ps 89). BBVRPD is a subordinate operator
--            on USFS land. Skipping rather than fabricating a rule.
--            See defer block below.
--
--   302 Resort Improvement District No. 1 (Shelter Cove, Humboldt)
--       9228 Little Black Sands Beach — RID#1 is NOT the operative
--            authority. Little Black Sands sits at the north end of
--            Shelter Cove WITHIN the BLM King Range National
--            Conservation Area (NCA) boundary; the operative recreation
--            ruleset is BLM Arcata Field Office's 2006 supplementary
--            rules (71 FR 29974, May 24 2006, FR Doc. E6-7404) plus
--            the BLM-wide 43 CFR §8365 baseline (ps 90, already linked
--            for other BLM beaches). Adding the King Range NCA
--            supplementary-rules ps row and linking the beach. RID#1
--            ps deferred — district publishes utility/resort
--            governance, not beach-recreation rules; no codified
--            animal rule on the RID#1 site (sheltercove-ca.gov fetched
--            via curl; April 2025 newsletter and homepage do not
--            contain a dog/pet rule). Governance follow-up flagged:
--            beach_agency should add BLM Arcata FO as dog_policy p=1.
--
--   303 Sacramento and San Joaquin Drainage District
--       6414 Grays Beach (Yuba) — DEFERRED. SS&JDD is a state flood-
--            control entity (managed under California Department of
--            Water Resources) that holds flowage easements and manages
--            levees/bypasses. It does NOT issue dog/recreation rules.
--            Operative dog_policy authority is Yuba County (already
--            beach_agency p=1 for dog_policy), and the beach is
--            already linked to Yuba County Code (ps 108 — Animal
--            Regulation Chapter). No SS&JDD ps row should exist.
--
--   304 San Diego Unified Port District
--       8540 Spanish Landing Beach (Spanish Landing Park, §8.02(a)(3))
--       9014 Shoreline Beach Park (Shoreline Park on Shelter Island,
--             §8.02(a)(1))
--            Both are SDUPD-managed "Public Parks" enumerated in San
--            Diego Unified Port District Code §8.02(a). The operative
--            visitor dog rule is published by the Port at its Parks
--            FAQ ("Dogs must be leashed at all times in all Port of
--            San Diego parks. Dogs are not allowed on Port of San
--            Diego beaches."), authorized by SDUPD Code §8.02(b)(1)
--            (no use of a designated park area for a purpose contrary
--            to its designated purpose / posted signage). Adding a
--            single SDUPD ps row keyed to §8.02 + the FAQ verbatim,
--            and linking both fids. Governance follow-up flagged:
--            both fids currently show City of San Diego as
--            dog_policy p=1 (beach_agency) but both are Port District
--            parks per §8.02(a) — operative authority is SDUPD, not
--            the City. The existing City of San Diego ps 156 link
--            should remain as land-owner reference but is not the
--            controlling rule.

-- ─── 1. BLM King Range NCA — Little Black Sands Beach (fid 9228) ────

-- 1a. BLM Arcata FO supplementary rules for the King Range NCA
--     (2006 Federal Register), authority 43 CFR §8365.1-6.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'federal_regulation',
       'BLM King Range NCA Supplementary Rules (71 Fed. Reg. 29974, May 24 2006; 43 CFR §8365.1-6 authority) — managed by Arcata Field Office',
       (SELECT id FROM public.agency WHERE name='United States Bureau of Land Management' AND type='federal_department'),
       ARRAY['dog_policy']::text[],
       'https://www.federalregister.gov/documents/2006/05/24/E6-7404/establishment-of-interim-supplementary-rules-on-public-lands-within-the-king-range-national',
       'BLM Establishment of Interim Supplementary Rules on Public Lands Within the King Range National Conservation Area Management Area, Managed by the Arcata Field Office, California. 71 Fed. Reg. 29974 (May 24, 2006), pp. 29974–29979 (FR Doc. E6-7404). Authority: 43 CFR §8365.1-6; FLPMA §§302, 310 (43 U.S.C. 1732, 1740). Mill Creek ACEC sub-unit rule (Sec. 2.a.3): "Dogs must be on a leash at all times." Outside the Mill Creek ACEC the supplementary rules do not impose a beach-specific leash; the BLM-wide baseline at 43 CFR §8365 (and §8365.2 in developed recreation sites) controls. Other NCA-wide rules of relevance (Sec. 2.c): Backcountry Management Zone group-size cap (15), bear-canister requirement in dispersed-use areas, motorized-watercraft landing prohibition in the Backcountry Management Zone. Little Black Sands Beach (north end of Shelter Cove, coords 40.038/-124.080) is within the King Range NCA boundary; the BLM Arcata FO is the operative recreation authority for the beach surface, with the underlying federal land managed under FLPMA. [Source: federalregister.gov FR Doc. E6-7404, fetched verbatim 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'BLM King Range NCA Supplementary Rules%'
);

-- 1b. Link fid 9228 Little Black Sands Beach to the King Range ps.
--     Rule remains off_leash_voice_control consistent with general
--     undeveloped-BLM-land defaults (43 CFR §8365 baseline; no Mill
--     Creek ACEC overlap at this location); existing Humboldt County
--     §541-21 link (ps 177) is retained as land-of-residence overlay.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 9228,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'BLM King Range NCA Supplementary Rules%'),
       'sand', 'off_leash_voice_control', 'operative'::operative_status,
       'Little Black Sands Beach is within the BLM King Range NCA boundary (Arcata FO). The 2006 supplementary rules (71 FR 29974) impose a leash rule only within the Mill Creek ACEC (Sec. 2.a.3); outside that sub-unit, the BLM-wide §8365 baseline applies, which permits dogs under owner control (off-leash voice-control acceptable on undeveloped BLM land). Consistent with the existing Humboldt County §541-21 attachment.',
       'https://www.federalregister.gov/documents/2006/05/24/E6-7404/establishment-of-interim-supplementary-rules-on-public-lands-within-the-king-range-national',
       'Governance-resolver follow-up: beach_agency currently shows Humboldt County as dog_policy p=1 and RID#1 only as land_use p=1; the operative federal authority for the beach surface is BLM Arcata FO (King Range NCA). Recommend elevating BLM Arcata FO to dog_policy precedence-1 in beach_agency. RID#1 ps deferred — district publishes no animal/recreation rule online.'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 2. San Diego Unified Port District — Spanish Landing + Shoreline ──

-- 2a. SDUPD Port Code §8.02 (Park Areas Regulated) + Port Parks FAQ
--     verbatim operative dog rule. §8.02(a)(1) enumerates "Shoreline
--     Park" on Shelter Island; §8.02(a)(3) enumerates "Spanish
--     Landing Park". The "no dogs on beaches" + "leashed in all
--     parks" rule is published at portofsandiego.org/parks-faq under
--     the explicit heading "Are there any restrictions on dogs in
--     Port of San Diego parks?", authorized by §8.02(b)(1) (no use of
--     a designated park area for a purpose contrary to its designated
--     purpose or posted signage).

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'special_district_ordinance',
       'San Diego Unified Port District Code §8.02 (Park Areas Regulated) + Port Parks FAQ — dogs leashed in parks, prohibited on beaches',
       (SELECT id FROM public.agency WHERE name='San Diego Unified Port District' AND type='special_district'),
       ARRAY['dog_policy']::text[],
       'https://www.portofsandiego.org/parks-faq',
       'San Diego Unified Port District Code Article 8 §8.02 (Park Areas Regulated). §8.02(a) defines the District''s Public Parks; (a)(1) "Shoreline Park" on Shelter Island in the City of San Diego, the public area bayward of Shelter Island Drive to the waterline, bordered on the north by the northwestern fence line of the parking lot north of the traffic circle, on the west by the public dock, and on the east by the private leaseholds, including the fishing pier and boat launching ramp; (a)(3) "Spanish Landing Park" in the City of San Diego, bordered on the north by North Harbor Drive, on the south by the waterline, on the west by the intersection of the waterline with the Navy Estuary Bridge, and on the east by the end of the grassy area east of Cancer Survivors Park. §8.02(b)(1) "No person shall use any area or facility set aside, used, maintained or designated for a specific recreational or park purpose by the Board or the Executive Director, which purpose is reasonably apparent from the appearance, construction or designation of such facility or area, or as to which reasonable notice of such designation or purpose is given by signs posted thereon, for a purpose contrary to or inconsistent with such specific or designated purpose." Port Parks FAQ (portofsandiego.org/parks-faq) — heading "Are there any restrictions on dogs in Port of San Diego parks?" — answer (verbatim): "Dogs must be leashed at all times in all Port of San Diego parks. Dogs are not allowed on Port of San Diego beaches." (Additional published note: "Dogs cannot be on grass areas at Tidelands Park in Coronado.") [Sources: SDUPD Port Code (published per Ordinance No. 19); Port Parks FAQ at portofsandiego.org/parks-faq. Code fetched from pantheonstorage.blob.core.windows.net/administration/San-Diego-Unified-Port-District-Port-Code.pdf 2026-05-17 (Section 8.02 spans pp. 196–211 of the published PDF). FAQ verbatim fetched 2026-05-17. Sec. 8.02 enacted July 1, 1980, Ordinance No. 879; last amended March 14, 2023, Ordinance No. 3079.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'San Diego Unified Port District Code §8.02%'
);

-- 2b. Spanish Landing Beach (fid 8540) — Port park, §8.02(a)(3).
--     Beach surface → not_allowed per Port Parks FAQ.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8540,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'San Diego Unified Port District Code §8.02%'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'Spanish Landing Park is a SDUPD Public Park per Port Code §8.02(a)(3) (Spanish Landing Park, City of San Diego, bordered north by North Harbor Drive, south by the waterline). Operative dog rule per the Port''s published Parks FAQ: "Dogs are not allowed on Port of San Diego beaches." Leashed dogs are permitted in the park''s non-beach (grass/promenade) areas.',
       'https://www.portofsandiego.org/parks-faq',
       'Governance-resolver follow-up: beach_agency shows City of San Diego as dog_policy p=1; the operative authority is SDUPD (the City has land_use elsewhere on the bayfront but the Port Code controls Port parks). Existing City of San Diego ps 156 link should remain as informational baseline. Walkthrough recommended to confirm sub-area carve-outs (grass vs sand boundary).'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- 2c. Shoreline Beach Park (fid 9014) — Port park, §8.02(a)(1).
--     Beach surface → not_allowed per Port Parks FAQ.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 9014,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'San Diego Unified Port District Code §8.02%'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'Shoreline Beach Park (Shelter Island) is a SDUPD Public Park per Port Code §8.02(a)(1) (Shoreline Park on Shelter Island, City of San Diego, bayward of Shelter Island Drive). Operative dog rule per the Port''s published Parks FAQ: "Dogs are not allowed on Port of San Diego beaches." Leashed dogs are permitted in the park''s non-beach areas, with §8.02(b)(1) backing the posted signage authority. Note: §8.02(a)(1) explicitly includes the fishing pier and boat launching ramp in the Shoreline Park footprint.',
       'https://www.portofsandiego.org/parks-faq',
       'Governance-resolver follow-up: beach_agency shows City of San Diego as dog_policy p=1; the operative authority is SDUPD. Existing City of San Diego ps 156 link should remain as informational baseline; the Port Code is the controlling rule on Shelter Island Park land. Existing rule via ps 156 was already "not_allowed" — outcome unchanged, authority corrected.'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 3. DEFERS — no ps INSERT for 294 BBVRPD or 303 SS&JDD ─────────
--
-- Big Bear Valley Recreation and Park District (294) / Swim Beach (9150):
--   BBVRPD's published swim-beach page
--   (specialdistricts.sbcounty.gov/home/parks-and-recreation/big-bear/
--    parks-and-facilities/swim-beach/) is silent on pets/dogs. The
--   broader district pages list "Hound Town Dog Park" / "Bark Park" at
--   Meadow Park as the dog-designated zone but do not publish a Swim
--   Beach pet rule. No codified district ordinance is hosted online.
--   The operative dog_policy authority per beach_agency is USFS
--   (precedence-1; San Bernardino National Forest land) and the beach
--   is already linked to ps 89 (36 CFR §261.16, Developed Recreation
--   Sites — Pet Provisions). BBVRPD is a subordinate operator on USFS
--   land; SB County animal-control contract applies at the county
--   level (City of Big Bear Lake contracts SB County for animal care).
--   No fabrication: ps row deferred until BBVRPD publishes a written
--   rule.
--
-- Sacramento and San Joaquin Drainage District (303) / Grays Beach (6414):
--   SS&JDD is a state-level flood-control authority (California
--   Department of Water Resources auspice; established under CA Water
--   Code Div. 5 Part 4 to manage levees, bypasses, and flowage
--   easements along the Sacramento and San Joaquin rivers). It does
--   NOT issue dog/recreation rules. The operative dog_policy
--   authority per beach_agency is Yuba County (precedence-1), and the
--   beach is already linked to ps 108 (Yuba County Code — Animal
--   Regulation Chapter). No SS&JDD ps row is appropriate.
--
-- Resort Improvement District No. 1 (302) / Little Black Sands (9228):
--   RID#1's published material (sheltercove-ca.gov homepage and April
--   2025 newsletter PDF fetched 2026-05-17) does not contain a
--   beach/animal rule; the district publishes utility (electric,
--   water, sewer, fire, airport, golf, recreation-clubhouse)
--   governance, not beach-recreation regulation. The operative
--   federal authority for Little Black Sands is BLM Arcata FO (King
--   Range NCA — see Section 1 above). No RID#1 ps row is created.
