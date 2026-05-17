-- Bucket B SoCal: 3 city backfills — Del Mar / PVE / Port Hueneme.
-- (Canyon Lake stripped 2026-05-17 per [[deferred-canyon-lake]] — Franz's
-- AV checker flagged a Canyon Lake URL during the agent run; deferring until
-- a safer source channel is available.)
--
-- Wave-3 follow-up. Each city's codified animal/beach ordinance was fetched
-- verbatim and resolved to a per-section deep-link URL per the
-- [[page-level-over-agency-level]] hard rule. Per-beach rules picked from
-- the operative text; tier mismatches (input table tier vs. what the code
-- actually says) flagged in status_note.
--
-- Per-city findings:
--
--  * Del Mar — Municode (slug = `municipal_code`, NOT `code_of_ordinances`).
--    Title 4 Ch 4.08 §4.08.020 "Dogs and Animals" is the operative section.
--    Rich SEASONAL + SUB-AREA carve-outs:
--      - North of an east-west line extending 29th Street: off-leash voice-
--        control day-after-Labor-Day through June 15, AND dawn-8am year-round.
--      - Between 25th St and 29th St: dawn-8am off-leash voice-control day-
--        after-Labor-Day through June 15.
--      - Between Powerhouse Park (north end) and 29th St: dogs PROHIBITED
--        June 15 – Labor Day (summer).
--      - Everywhere else: on-leash (max 6 ft).
--    Two beach fids in scope:
--      - fid 8560 "Del Mar Dog Beach" @ 32.9732 — NORTH BEACH, north of 29th
--        St; the well-known seasonal voice-control off-leash strip. Rule =
--        off_leash_voice_control with seasonal status_note. Input tier
--        2_on-leash UNDER-states the rule.
--      - fid 8992 "Del Mar Beach, California, USA" @ 32.9606 — main / central
--        beach near Powerhouse Park / 29th St area. Falls inside the
--        June 15–Labor Day no-dogs summer prohibition zone (Powerhouse Park
--        boundary to 29th St line). Outside summer the on-leash rule applies.
--        Rule = seasonal (encoded as on_leash with status_note documenting
--        the summer prohibition); input tier 1_off-leash is WRONG for this
--        location — this is the leashed main beach, not the off-leash north
--        strip. Two-fid handling deliberate: same statute, two location-
--        specific status_notes.
--
--  * Canyon Lake — STRIPPED 2026-05-17 per [[deferred-canyon-lake]].
--
--  * Palos Verdes Estates (PVE) — codepublishing legacy URL migrated to
--    ecode360 (custid = PA4580). Title 6 Ch 6.08 has TWO operative sections:
--    §6.08.050 (citywide leash, max 6 ft on streets/parks/etc.) and §6.08.070
--    "Dogs on beach or in ocean prohibited" — which is BROAD:
--      "No person shall bring onto any beach or into the waters of the
--       Pacific Ocean adjacent to any beach any dog. 'Beach,' as used in
--       this section, is defined as follows: that portion of Lot F, Tract
--       10624, between the water's edge and the base of the bluffs."
--    Rat Beach (fid 8267) is THE PVE beach — sits within Lot F, Tract 10624.
--    Rule = not_allowed (the §6.08.070 prohibition), NOT on_leash. Input
--    tier 2_on-leash is WRONG; this beach is no-dogs by city code. Note
--    Rat Beach is also adjacent to the Torrance city boundary at the south
--    end; the PVE portion is no-dogs.
--
--  * Port Hueneme — Municode (`code_of_ordinances`). Article IV Ch 1 §4008
--    "Animals": "(a) No person shall bring, maintain, ride or lead any
--    animal as defined in Chapter 1 of Article III of this Code, except a
--    guide dog used by the physically handicapped, onto any sandy beach
--    area of the City or on the seaward side of the shorefront paved
--    pathways of Hueneme Beach Park or into the waters of the ocean."
--    Port Hueneme Beach (fid 8598) is the city's sole beach — Hueneme Beach
--    Park. Rule = not_allowed (with guide-dog/service-dog carve-out). Input
--    tier 2_on-leash is WRONG; the code prohibits all dogs except guide
--    dogs on the sandy beach and on the seaward paved path.
--    (Bubbling Springs Park has a designated off-leash dog area per
--    §4008(b) — that's an inland city park, not the beach.)
--
-- Three cities applied here exist in public.agency (48 Del Mar, 63 PVE,
-- 67 Port Hueneme). Canyon Lake (agency_id 87) deferred per pin above.
--
-- Sources fetched verbatim 2026-05-17 via Playwright.


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 1 — Del Mar Municipal Code §4.08.020 (Dogs and Animals)
-- Applies to: fid 8560 (North Beach / off-leash strip) +
--             fid 8992 (main beach / Powerhouse Park area, summer no-dogs).
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Del Mar Municipal Code §4.08.020 (Dogs and Animals) — Title 4 Ch 4.08 Animals on Beaches and in Parks; seasonal + sub-area carve-outs for the North Beach off-leash strip',
       48,
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/ca/del_mar/codes/municipal_code?nodeId=TIT4AN_CH4.08ANBEPA_4.08.020DOAN',
       'Del Mar Municipal Code §4.08.020 Dogs and Animals: A. It shall be unlawful for any person to bring, allow or permit any dog upon any of the beaches or in any of the waters within the City unless such dog is restrained by a leash not in excess of six feet except as stated in Subsection B. Notwithstanding the exceptions in Subsection B., every owner of a dog shall restrain his dog from running at large on any of the beaches or waters in the City. B. Provided such dogs are under the immediate voice command and control of their owners or keeper, this Subsection shall not prohibit off leash dogs in the following instances: 1. Dogs may be off leash on the beaches or waters located northerly of a line being an extension of 29th Street from the day after Labor Day through June 15th, and from dawn to 8:00 a.m. year-round. 2. Dogs may be off leash on the beaches or waters between an east-west line which is the westerly prolongation of the center line of 25th Street and an east-west line which is the westerly prolongation of the center line of 29th Street from dawn to 8:00 a.m. from the day after Labor Day through June 15th. C. It shall be unlawful for any person to bring, allow or permit any dog upon the beach or in any adjacent waters between an east-west line perpendicular to the northern end of the Powerhouse Park property and an east-west line which is the westerly prolongation of the center line of 29th Street during the period of June 15th through Labor Day, except as stated in Subsection B. Every owner of a dog shall prevent his dog from being in the area prohibited by this Subsection, whether on a leash or running at large or otherwise, except as stated in Subsection B. D. It shall be unlawful for any person to bring, allow or permit any animal upon the Tot Lot playground or southern turf (grass) portion of Powerhouse Park. Animals are permitted on all walkways and sidewalks but must be restrained by a leash not in excess of six feet and must be restrained so that the animal does not enter the prohibited areas. The prohibited area shall be posted with signage indicating the animal free zone(s). 1. For the purpose of this Subsection, the Tot Lot playground also includes the landscape surrounding the play equipment including but not limited to: play sand or dirt, sand boxes, grass, brush, plants, trees, benches, tables, fences, ropes barriers, and concrete hardscape. Any person that allows an animal to excessively bark, harass, create a nuisance, or injure a person within the areas covered in this Section, even if the animal is restrained by a leash, shall be in violation of this Section. (Ord. No. 684; Ord. No. 787; Ord. No. 883; Ord. No. 940, § 1, 9-4-2018) [Hosted on Municode at library.municode.com/ca/del_mar/codes/municipal_code (NOTE: doc slug is `municipal_code`, not the more common `code_of_ordinances`). Fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Del Mar Municipal Code §4.08.020%'
);

-- fid 8560 Del Mar Dog Beach (32.9732) — NORTH BEACH, north of 29th St.
-- Per §4.08.020(B)(1): off-leash voice-control day-after-Labor-Day
-- through June 15, AND dawn-8am year-round. During summer (Jun 16 –
-- Labor Day daytime) the on-leash rule of §4.08.020(A) applies here.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8560, ps.id, 'sand', 'off_leash_voice_control', 'operative'::operative_status,
       'Del Mar Municipal Code §4.08.020(B)(1): "Dogs may be off leash on the beaches or waters located northerly of a line being an extension of 29th Street from the day after Labor Day through June 15th, and from dawn to 8:00 a.m. year-round." Provided dogs are under immediate voice command and control of their owners.',
       ps.source_url,
       'SEASONAL: off-leash voice-control day after Labor Day through June 15 (off-season); during summer (Jun 16 – Labor Day) only dawn-to-8am off-leash, leash required all other hours (§4.08.020(A)). Located at the North Beach strip north of an east-west line extending 29th Street. Input tier 2_on-leash understates the seasonal off-leash carve-out.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Del Mar Municipal Code §4.08.020%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- fid 8992 Del Mar Beach (32.9606) — main beach near Powerhouse Park /
-- 29th St area. Per §4.08.020(C): dogs PROHIBITED June 15 – Labor Day
-- in the Powerhouse-Park-to-29th-St zone. Outside that summer window
-- §4.08.020(A) on-leash applies (with the 25th-29th St dawn-8am
-- off-leash window from §4.08.020(B)(2) for the upper portion).
-- Rule: on_leash with status_note documenting the summer prohibition.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8992, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Del Mar Municipal Code §4.08.020(A): "any dog upon any of the beaches or in any of the waters within the City unless such dog is restrained by a leash not in excess of six feet." §4.08.020(C) further provides: "It shall be unlawful for any person to bring, allow or permit any dog upon the beach or in any adjacent waters between an east-west line perpendicular to the northern end of the Powerhouse Park property and an east-west line which is the westerly prolongation of the center line of 29th Street during the period of June 15th through Labor Day."',
       ps.source_url,
       'SEASONAL: leashed (6-ft max) outside the Jun 15–Labor Day summer window; during summer dogs are PROHIBITED in this Powerhouse-Park-to-29th-St zone per §4.08.020(C). Likely the central/main Del Mar beach (~32.961, near 25th-29th St block); a dawn-8am off-leash voice-control window applies in the 25th–29th St strip in off-season per §4.08.020(B)(2). Input tier 1_off-leash is likely WRONG for this location — the off-leash strip is the North Beach (see fid 8560). Same §4.08.020 statute as fid 8560 with different sub-area carve-out applying.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Del Mar Municipal Code §4.08.020%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;


-- SECTION 2 — Canyon Lake — STRIPPED per [[deferred-canyon-lake]].
-- The amlegal URL fetched by the agent tripped Franz's AV checker; the
-- INSERT was removed to keep that URL out of production. Indian Beach
-- (fid 6076) is HOA-private anyway. Re-introduce via a safer source.


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 3 — Palos Verdes Estates Municipal Code §6.08.070
--             (Dogs on beach or in ocean prohibited)
-- Applies to: Rat Beach (fid 8267).
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Palos Verdes Estates Municipal Code §6.08.070 (Dogs on beach or in ocean prohibited) — Title 6 Ch 6.08 Regulations Pertaining to Dogs',
       63,
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/49086030',
       'Palos Verdes Estates Municipal Code §6.08.070 Dogs on beach or in ocean prohibited: "No person shall bring onto any beach or into the waters of the Pacific Ocean adjacent to any beach any dog. ''Beach,'' as used in this section, is defined as follows: that portion of Lot F, Tract 10624, between the water''s edge and the base of the bluffs." (Ord. 678 § 2, 2007; Ord. 701 § 2 (Exh. 1), 2012) Companion §6.08.050 Dogs – Running at large prohibited: (A) No person owning or having charge, care, custody, or control of any dog shall cause, permit, or allow the same to be or to run at large upon any street, sidewalk, parkland, or other public place, or upon any private property or premises other than that of the person owning or having charge, care, custody, or control of such dog, unless such dog be restrained by a substantial chain or leash not exceeding six feet in length and is in the charge, care, custody, or control of a competent person. [Hosted on ecode360 (General Code); custid PA4580. The legacy codepublishing URL (www.codepublishing.com/CA/PalosVerdesEstates/) returns a redirect stub pointing to ecode360.com/PA4580. Fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Palos Verdes Estates Municipal Code §6.08.070%'
);

-- fid 8267 Rat Beach (33.8037) — sits within Lot F, Tract 10624 (the PVE
-- beachfront defined by §6.08.070). Rule = not_allowed.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8267, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'PVE Municipal Code §6.08.070: "No person shall bring onto any beach or into the waters of the Pacific Ocean adjacent to any beach any dog." Definition: "that portion of Lot F, Tract 10624, between the water''s edge and the base of the bluffs." Rat Beach is the PVE beachfront sitting within this defined boundary.',
       ps.source_url,
       'Input tier 2_on-leash UNDER-states the prohibition; PVE §6.08.070 broadly bars dogs from the city''s sole beach. Rat Beach straddles the PVE/Torrance city boundary at its south end; the Torrance-side beach is governed by Torrance city code (not in scope here). On-site signage typically notes the PVE dog prohibition.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Palos Verdes Estates Municipal Code §6.08.070%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;


-- ─────────────────────────────────────────────────────────────────────
-- SECTION 4 — Port Hueneme Municipal Code §4008 (Animals)
-- Applies to: Port Hueneme Beach (fid 8598).
-- ─────────────────────────────────────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Port Hueneme Municipal Code §4008 (Animals) — Article IV Parks, Beaches and Community Recreation Facilities, Ch 1 General Regulations',
       67,
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/ca/port_hueneme/codes/code_of_ordinances?nodeId=ARTIVPABECOREFA_CH1GERE_4008AN',
       'Port Hueneme Municipal Code §4008 Animals: (a) No person shall bring, maintain, ride or lead any animal as defined in Chapter 1 of Article III of this Code, except a guide dog used by the physically handicapped, onto any sandy beach area of the City or on the seaward side of the shorefront paved pathways of Hueneme Beach Park or into the waters of the ocean. (b) An off-leash dog area within Bubbling Springs Park as depicted on the park construction drawings maintained in the Department of Public Works is hereby designated. Such area shall be fenced and posted for off-leash activity pursuant to the following regulations: (1) Persons shall use a suitable container or instrument to remove their dog''s feces and dispose of it in a sanitary manner in designated waste containers; (2) No person shall harass, tease, or incite any dog in any way [...]. §4001 Definitions: (a) "Beach" means all that portion of land adjacent to the Pacific Ocean within the City limits including the bicycle path and all public improvements thereon. (f) "City parks" means all of the following: Bolker Park, Bubbling Springs Park, Beach Park, Moranda Park, Recreation Greenbelt improvements, Seaview Apartments Playground. [Hosted on Municode at library.municode.com/ca/port_hueneme/codes/code_of_ordinances. The off-leash carve-out in §4008(b) applies to an inland city park (Bubbling Springs Park), NOT to the beach. Fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Port Hueneme Municipal Code §4008%'
);

-- fid 8598 Port Hueneme Beach (34.1417) — Hueneme Beach Park, the city's
-- sole beach. Rule = not_allowed (sandy beach area + seaward paved path +
-- ocean waters all barred per §4008(a)); guide-dog/service-dog exception.
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8598, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'Port Hueneme Municipal Code §4008(a): "No person shall bring, maintain, ride or lead any animal as defined in Chapter 1 of Article III of this Code, except a guide dog used by the physically handicapped, onto any sandy beach area of the City or on the seaward side of the shorefront paved pathways of Hueneme Beach Park or into the waters of the ocean."',
       ps.source_url,
       'Input tier 2_on-leash UNDER-states the prohibition; §4008(a) bars all dogs except guide dogs (service-dog use under ADA reads in) from the sandy beach, the seaward side of the paved shorefront pathways, and the ocean. The §4008(b) off-leash dog area is at Bubbling Springs Park (inland city park), NOT the beach. The landward (inland) side of the paved bike/pedestrian path is not covered by the §4008(a) prohibition; leashed access on the landward sidewalk may be tolerated subject to citywide leash law.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Port Hueneme Municipal Code §4008%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
