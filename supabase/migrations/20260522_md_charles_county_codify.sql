-- MD Charles County codify — Chapter 230 Dogs and Other Animals
--
-- Source: Charles County, Maryland Code, Division 2 Code of Ordinances and
--   Resolutions, Part II General Legislation, Chapter 230 DOGS AND OTHER
--   ANIMALS, Article I Animal Regulations (adopted 6-19-2006 by Bill No.
--   2006-06; amended in its entirety 1-10-2012 by Bill No. 2011-07).
--
-- URL (chapter deep-link via ecode360, custid CH0836):
--   https://ecode360.com/26905390
--
-- ─── Operative rule (§ 230-12 Animal at large — verbatim) ───────────
--
-- A. It shall be unlawful for any person, partnership, or corporation or
--    other legal entity to allow an animal to be at large.
-- B. This section shall not apply to a dog undergoing supervised obedience
--    training or while actually engaged in the sport of hunting in an
--    authorized area while supervised by a competent person.
-- C. Without permission of the proper authority, the owner or custodian of
--    any animal may not permit the animal to be on school grounds on a day
--    when school is in session, in a public recreation area, any public
--    property or thoroughfare or private property without the property
--    owner's permission unless:
--    (1) The animal is controlled by a leash or similar restraining device.
--    (2) The presence of the animal is in an organized activity such as a
--        dog show.
-- D. No animal accidentally at large with a person capable of controlling
--    the animal in immediate physical pursuit shall be deemed at large.
--
-- And § 230-2 definition (operative scope):
--   ANIMAL AT LARGE — Any animal off the premises of the owner or
--   custodian, and not under the restraint of a person capable of
--   controlling the animal.
--
-- Rule classification: on_leash. § 230-12(A) establishes the countywide
-- at-large prohibition; § 230-12(C)(1) explicitly requires a "leash or
-- similar restraining device" in any public recreation area / public
-- property. Per [[citywide-leash-inference]] (territorial leash rule →
-- beaches by default), all Charles County public-access beach areas
-- (Potomac shore, river access) inherit on_leash.
--
-- ─── Per-beach mapping ──────────────────────────────────────────────
--
-- 4 scoreable beaches in Charles County MD (state='MD', county_name='Charles',
-- scoring_tier ∈ {daily, hourly}). All Potomac-shore river beaches; none
-- inside any incorporated Maryland municipality (Charles County's incorporated
-- towns — Indian Head, La Plata, Port Tobacco Village — are all inland, not
-- on the Potomac shoreline). Uniform unincorporated-county code application.
--
--   fid 12944  Clifton Beach    — Nanjemoy Natural Resource Management
--                                 Area (state SDNR) + Mallows Bay-Potomac
--                                 NMS overlay (NOAA marine — landside is
--                                 Charles County, county code is operative
--                                 for terrestrial dog rule; the NMS
--                                 jurisdiction is water-column only).
--   fid 12952  Wicomico Beach   — Wicomico River shore at Charles/St.
--                                 Mary's county boundary; county PIP
--                                 returns BOTH counties (river-edge
--                                 ambiguity). Charles County code applied
--                                 as primary; St. Mary's § 212-8
--                                 (20260522_md_st_marys_county_codify.sql)
--                                 did NOT enroll fid 12952 in its per-beach
--                                 list, so no layering conflict.
--   fid 14626  The Sands        — Wicomico River shore at MD/VA waterline.
--                                 PIP returns Westmoreland State Park (VA
--                                 SDC) due to cross-state polygon spillover;
--                                 actual location is Maryland (Charles
--                                 County, Potomac/Wicomico river edge).
--                                 Treat as a regular Charles County beach
--                                 under county code; DO NOT attach to
--                                 Westmoreland VA SP.
--   fid 425640 Colonial Beach   — county PIP returns Charles County MD;
--                                 address geocoder put it as Colonial
--                                 Beach VA (the centroid sits on the VA
--                                 shore of the Potomac). Cross-river
--                                 polygon spillover analogous to The
--                                 Sands. Authoritative jurisdictional
--                                 assignment is Charles County MD per
--                                 spatial join; apply county code with
--                                 status_note flagging the centroid quirk
--                                 for walkthrough verification.
--
-- ─── Judgment calls ────────────────────────────────────────────────
--
-- 1. No incorporated-municipality overlay. Charles County's three
--    incorporated towns (Indian Head, La Plata, Port Tobacco Village)
--    are all inland; none of the 4 Potomac-shore beaches sit inside
--    any incorporated-town corporate limits. Uniform county-code
--    application is correct.
-- 2. § 230-12(B) hunting + obedience-training exceptions preserved in
--    full_text but do NOT override the on_leash classification —
--    § 230-12(C)(1) public-recreation-area leash requirement controls
--    for public beach use.
-- 3. Mallows Bay-Potomac NMS (Clifton Beach overlay) is a marine
--    sanctuary; its 15 CFR Part 922 subpart-equivalent regulations
--    govern water-column activities (vessels, anchoring, artifacts).
--    Landside dog rule remains Charles County's. Per task header:
--    "Clifton's land rules ARE Charles County's." Confirmed.
-- 4. The Sands + Colonial Beach are flagged as cross-state polygon
--    spillover (MD/VA Potomac waterline). Per task header: treat both
--    as regular Charles County beaches; DO NOT cross-attach to VA
--    jurisdictions. Status_note recommends walkthrough verification of
--    actual on-the-ground location vs. the centroid quirk.
-- 5. Wicomico Beach (fid 12952) has dual Charles + St. Mary's county
--    PIP membership (river-edge ambiguity). St. Mary's § 212-8
--    migration (already committed today) did NOT enroll fid 12952;
--    Charles County is the canonical primary authority here. If future
--    walkthrough indicates the beach is actually on the St. Mary's
--    bank, swap rather than layer (both county codes yield the same
--    on_leash rule, so consumer surface is unaffected either way).
--
-- ─── Agency provisioning ────────────────────────────────────────────
--
-- No "Charles County" type=county agency row exists yet (verified
-- 2026-05-22). Create canonical bare-name row before inserting the
-- policy_source row.

-- ─── 1. Provision Charles County agency (canonical bare-name) ───────

INSERT INTO public.agency (name, type, web_url)
SELECT 'Charles County', 'county'::agency_type, 'https://www.charlescountymd.gov/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency WHERE name='Charles County' AND type='county'::agency_type
);

-- ─── 2. Insert codified-ordinance policy_source ─────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Charles County, Maryland Code, Part II General Legislation, Chapter 230 Dogs and Other Animals, § 230-12 (Animal at large — countywide at-large prohibition and public-recreation-area leash requirement)',
       (SELECT id FROM public.agency WHERE name='Charles County' AND type='county'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/26905390',
       $body$Charles County, Maryland Code, Division 2 Code of Ordinances and Resolutions, Part II General Legislation, Chapter 230 DOGS AND OTHER ANIMALS, Article I Animal Regulations. [Adopted 6-19-2006 by Bill No. 2006-06; amended in its entirety 1-10-2012 by Bill No. 2011-07.] § 230-1 Purpose. These animal regulations are hereby established by the County Commissioners of Charles County, Maryland, to provide for the safety of the public, the humane care and treatment of animals and to encourage responsible pet ownership. § 230-2 Definitions. ANIMAL AT LARGE — Any animal off the premises of the owner or custodian, and not under the restraint of a person capable of controlling the animal. CUSTODIAN — Any person, partnership, corporation or other legal entity that harbors, takes care or custody of, or takes responsibility for another person's animal(s); or allows the animal(s) to remain on their premises. OWNER — Any person, partnership, corporation or other legal entity that owns, keeps or harbors one or more animals; or allows the animal(s) to remain on their premises. § 230-12 Animal at large. A. It shall be unlawful for any person, partnership, or corporation or other legal entity to allow an animal to be at large. B. This section shall not apply to a dog undergoing supervised obedience training or while actually engaged in the sport of hunting in an authorized area while supervised by a competent person. C. Without permission of the proper authority, the owner or custodian of any animal may not permit the animal to be on school grounds on a day when school is in session, in a public recreation area, any public property or thoroughfare or private property without the property owner's permission unless: (1) The animal is controlled by a leash or similar restraining device. (2) The presence of the animal is in an organized activity such as a dog show. D. No animal accidentally at large with a person capable of controlling the animal in immediate physical pursuit shall be deemed at large. E. A person who is aware of an animal being at large or who finds a stray animal shall report the condition to the Tri-County Animal Shelter, an Animal Control Officer, or other law enforcement official. F. An Animal Control Officer or authorized representative of Animal Control who observes an animal at large may pursue that animal on public and/or private property. § 230-12.3 Allowing animal to defecate on public property prohibited. It shall be unlawful for any owner or custodian to allow their animal to defecate on public property unless the owner or custodian of the animal immediately thereafter removes and disposes of it in a sanitary manner. § 230-12.4 Public nuisance. A public nuisance includes when an owner or custodian allows an animal to: (1) Be at large; (2) Damage the property of anyone other than its owner; (3) Molest pedestrians, neighbors or passersby [...]. [Hosted on ecode360 (General Code platform, custid CH0836) at ecode360.com/26905390. Fetched verbatim via Playwright 2026-05-22.]$body$
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Charles County, Maryland Code%§ 230-12%'
);

-- ─── 3. Per-beach BPS rows — 3 unambiguous Charles County beaches ───

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       $ev$Charles County Code § 230-12(A): "It shall be unlawful for any person, partnership, or corporation or other legal entity to allow an animal to be at large." § 230-12(C)(1): "Without permission of the proper authority, the owner or custodian of any animal may not permit the animal to be ... in a public recreation area, any public property or thoroughfare ... unless: (1) The animal is controlled by a leash or similar restraining device." § 230-2: "ANIMAL AT LARGE — Any animal off the premises of the owner or custodian, and not under the restraint of a person capable of controlling the animal." (Bill No. 2011-07, 1-10-2012)$ev$,
       ps.source_url, NULL, NULL
FROM (values (12944),(12952),(14626)) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Charles County, Maryland Code%§ 230-12%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule) DO NOTHING;

-- ─── 4. Clifton Beach — Mallows Bay NMS landside-rule note ──────────
-- Re-emitted as a UPDATE pattern? No — single insert above already
-- covered fid 12944 with NULL status_note. The Mallows Bay context is
-- captured in the header for auditability; no separate bps row needed
-- because the NMS regulation does not govern terrestrial dog rule.

-- ─── 5. Colonial Beach (fid 425640) — cross-state polygon quirk ─────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT 425640, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       $ev$Charles County Code § 230-12(A) at-large prohibition + § 230-12(C)(1) public-recreation-area leash requirement apply countywide. (Bill No. 2011-07, 1-10-2012)$ev$,
       ps.source_url,
       'Cross-state polygon spillover: county PIP assigns this beach to Charles County MD, but the centroid sits on the Virginia shore of the Potomac (Colonial Beach VA town address from geocoder). Treating as a regular Charles County MD beach per spatial-join authority. Walkthrough recommended to confirm actual on-the-ground jurisdiction; if it is genuinely the Colonial Beach VA town shoreline, reattach under Westmoreland County VA + Town of Colonial Beach code.',
       NULL
FROM public.policy_source ps
WHERE ps.citation LIKE 'Charles County, Maryland Code%§ 230-12%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule) DO NOTHING;
