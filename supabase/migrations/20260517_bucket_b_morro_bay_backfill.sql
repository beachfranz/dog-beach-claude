-- Bucket B — City of Morro Bay: Morro Rock Beach (fid 8622).
--
-- Per the 2026-05-17 Wave-3 follow-up backfill pass.
--
-- Platform: Municode (library.municode.com), slug morro_bay, doc
-- code_of_ordinances. Discovered via the Municode TOC + Title 7 drill-in
-- (nodeId TIT7AN_CH7.04GEPR_7.04.010ADINSALUOBCOANCOCOTI9).
--
-- Beach attribution / authority judgment:
--
--   Morro Rock itself is a CA State Parks preserve (Morro Rock State
--   Preserve / Morro Bay State Park environs). The STRAND at the base of
--   the Rock, however, is city-operated public beach — confirmed by the
--   beaches_gold park_name = "Morro Bay City Beach". The City of Morro
--   Bay operates the beach surface; CA DPR holds the Rock proper. For
--   the purpose of dog-policy citation on the strand, the operative
--   authority is the City of Morro Bay (agency_id=59).
--
--   The neighboring Morro Strand State Beach (DPR-operated, agency_id
--   234, ps id 76) is a separate beach to the north; that one is
--   already correctly attributed to CA DPR.
--
-- Code structure (the layered/adopted pattern):
--
--   The Morro Bay Animal Control title was completely rewritten by
--   Ord. No. 669 (5-13-25). The current Title 7 §7.04.010 adopts the
--   SAN LUIS OBISPO COUNTY Animal Control Code (Title 9) by reference
--   as the operative regulation inside city limits:
--
--   §7.04.010 (verbatim):
--     "The provisions of San Luis Obispo County Animal Control Code,
--     Title 9, as amended from time to time, are adopted by reference
--     and incorporated in their entirety as equivalent provisions of
--     the Morro Bay Municipal Code. To the extent any provision or
--     provisions of the San Luis Obispo County Animal Control Code,
--     Title 9, as adopted, conflict with any other provision or
--     provisions of the Morro Bay City Municipal Code, the other
--     provision or provisions of the Morro Bay Municipal Code shall
--     take precedence."
--
--   §7.04.020 then designates SLO County Division of Animal Services as
--   the enforcement authority within city limits (via contract).
--
--   The Morro Bay code has NO dedicated parks/beaches chapter that
--   carves out a beach-specific dog rule (verified: Title 9 PUPEMOWE,
--   Title 12 Streets/Sidewalks, Title 15 Harbor/Ocean, all reviewed —
--   none modify the leash law for beach surfaces). Title 7 Ch. 7.08
--   (Other Animals, Poultry and Birds) does mention §7.08.030 pygmy
--   livestock being "subject to the leash law," but does not override
--   the adoption-by-reference for dogs.
--
-- Operative leash rule, via the adopted SLO County Title 9:
--
--   SLO County Code §9.03.005(a) Leash law (verbatim, per ps id 180):
--     "No person shall permit any dog owned, harbored, or controlled by
--     him to be in or upon any public roadway, walkway, park or other
--     public area unless securely restrained by a leash and under the
--     direct control of a person competent to exercise full care,
--     custody, and charge over such dog, or unless the dog is at a
--     'heel' beside a person and obedient to that person's command."
--
--   §9.03.005(c) exceptions: (1) herding/hunting dogs; (2) within
--   boundaries of any park or other public area specifically designated
--   and authorized by the controlling agency as an off-leash
--   recreational area for dogs; (3) registered service/SAR/LE dogs.
--
-- Rule choice: `on_leash` — matches the assigned 2_on-leash tier. The
-- §9.03.005(a) "heel" alternative is a narrow voice-control exception
-- requiring the dog to walk "directly adjacent" and "obedient"; it does
-- NOT justify a general voice-control characterization on the strand.
-- No Morro Bay city resolution designates Morro Rock Beach as an
-- off-leash area, so §9.03.005(c)(2) carve-out does not apply.
--
-- Source URL: per-section deep link to Morro Bay MC §7.04.010 (the
-- adoption-by-reference section — the operative city-level act), per
-- [[page-level-over-agency-level]] and the Municode anatomy in
-- [[url-resolution-field-guide]]. The pre-existing SLO County code
-- policy_source (ps id 180) holds the downstream substantive text for
-- cross-reference; we cite the Morro Bay city act here as the
-- governance hook for fid 8622.

-- ─── 1. Insert codified-ordinance policy_source ─────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Morro Bay Municipal Code §7.04.010 (Adoption and incorporation of San Luis Obispo County Animal Control Code Title 9 by reference) + §7.04.020 (Enforcement within city)',
       59,
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/ca/morro_bay/codes/code_of_ordinances?nodeId=TIT7AN_CH7.04GEPR_7.04.010ADINSALUOBCOANCOCOTI9',
       'Morro Bay Municipal Code Title 7 ANIMALS, Chapter 7.04 GENERAL PROVISIONS. §7.04.010 Adoption and incorporation of San Luis Obispo County Animal Control Code (Title 9): "The provisions of San Luis Obispo County Animal Control Code, Title 9, as amended from time to time, are adopted by reference and incorporated in their entirety as equivalent provisions of the Morro Bay Municipal Code. To the extent any provision or provisions of the San Luis Obispo County Animal Control Code, Title 9, as adopted, conflict with any other provision or provisions of the Morro Bay City Municipal Code, the other provision or provisions of the Morro Bay Municipal Code shall take precedence." §7.04.020 Enforcement of animal control provisions within city: "The provisions of this Title are enforceable within the jurisdictional boundaries of the city of Morro Bay by the San Luis Obispo County Division of Animal Services and County of San Luis Obispo Animal Control Officer, or designee, pursuant to and in accordance with the terms of a contract for animal care and control services entered into between the City and the County, and by City Code Enforcement and the City Police Department." (Ord. No. 669, § 2, 5-13-25.) Downstream substantive rule (via adoption): SLO County Code §9.03.005(a) leash law — "No person shall permit any dog owned, harbored, or controlled by him to be in or upon any public roadway, walkway, park or other public area unless securely restrained by a leash and under the direct control of a person competent to exercise full care, custody, and charge over such dog, or unless the dog is at a heel beside a person and obedient to that person''s command." §9.03.005(c) exceptions cover herding/hunting, controlling-agency-designated off-leash areas, and registered service/SAR/LE dogs (none of which designate Morro Rock Beach as off-leash). [Hosted on Municode (library.municode.com/ca/morro_bay/codes/code_of_ordinances) under doc slug code_of_ordinances. Section deep-link nodeId TIT7AN_CH7.04GEPR_7.04.010ADINSALUOBCOANCOCOTI9. The Morro Bay code has no dedicated beach-specific dog ordinance — the adopted SLO County leash law is the operative rule for the city''s public beach surfaces. Fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Morro Bay Municipal Code §7.04.010%'
);

-- ─── 2. Link Morro Rock Beach (fid 8622) → on_leash ─────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8622, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Morro Bay Municipal Code §7.04.010 adopts SLO County Animal Control Code Title 9 by reference as the operative regulation within city limits. The downstream rule, SLO County Code §9.03.005(a), requires: "No person shall permit any dog ... to be in or upon any public roadway, walkway, park or other public area unless securely restrained by a leash and under the direct control of a person competent to exercise full care, custody, and charge over such dog, or unless the dog is at a ''heel'' beside a person and obedient to that person''s command." Morro Rock Beach (park_name "Morro Bay City Beach" in beaches_gold) is city-operated public beach within Morro Bay city limits; no §9.03.005(c)(2) off-leash designation has been issued by the City Council. (Ord. No. 669, § 2, 5-13-25.)',
       'https://library.municode.com/ca/morro_bay/codes/code_of_ordinances?nodeId=TIT7AN_CH7.04GEPR_7.04.010ADINSALUOBCOANCOCOTI9',
       'Morro Rock proper is a CA State Parks preserve; the STRAND at its base is city-operated public beach. Citation chain: Morro Bay MC §7.04.010 (adoption-by-reference, the city governance hook) → SLO County Code §9.03.005(a) leash law (the substantive rule; see also ps id 180 for the SLO County source). Heel-exception in §9.03.005(a) is narrow; default on_leash for beaches. Walkthrough recommended to verify on-site signage near the Rock and any seasonal snowy-plover closures on the adjacent Morro Strand.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Morro Bay Municipal Code §7.04.010%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
