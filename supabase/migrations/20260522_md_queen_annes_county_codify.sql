-- MD Queen Anne's County codify — Ch 9 (Animal Control) + Ch 22 (Parks)
--
-- Source: Queen Anne's County Code, Part III Local Regulations.
--   Chapter 9 Animal Control, Article III Registration; Commercial Animal
--     Establishment License; Animal Care, § 9-17 Animals at large.
--     [Adopted 4-9-2024 by Ord. No. 24-08; repealed and replaced prior
--      Ch 9 adopted 9-9-2014 by Ord. No. 14-13.]
--   Chapter 22 Parks and Recreation, Article III Use of County Parks,
--     § 22-21 Animal control provisions.
--
-- URLs (deep-linked, ecode360 custid QU1770):
--   § 9-17 Animals at large:          https://ecode360.com/45162679
--   § 22-21 Animal control (parks):   https://ecode360.com/35472191
--   Ch 9 Art III container:           https://ecode360.com/45162609
--   Ch 22 Art III container:          https://ecode360.com/35472224
-- (Both fetched verbatim via Playwright 2026-05-22.)
--
-- ─── Operative rules (verbatim, key subsections) ────────────────────
--
-- § 9-17 Animals at large:
--   "A. Prohibited. Animals at large are prohibited.
--     (1) Includes any animal off the property of the owner or custodian
--         without being under restraint.
--       (a) Domestic animals. Under restraint means:
--         [1] Secured by a leash or lead; or
--         [2] Under the effective control of its owner, custodian, or
--             other responsible person (at heel and directly beside) by
--             way of training of the animal and handler; or
--         [3] Within a vehicle being driven, standing, or parked in a
--             manner that does not endanger the animal's health or safety.
--     (2) Does not include:
--       (a) An animal at large with a person in immediate pursuit.
--       (b) An animal that is unrestrained in a permitted or appropriate
--           area.
--       (c) Community cats.
--       (d) An animal engaging in supervised obedience training or
--           hunting activities in an authorized area and supervised by
--           a competent person.
--   B. School and County property.
--     (1) An animal may not be on school grounds on a day when school is
--         in session without the permission of the proper authority.
--     (2) If in a public recreation area, the animal must be controlled
--         by a leash or other similar restraining device.
--     (3) An animal may not be on any County property that is properly
--         posted against such animals."
--
-- § 22-21 Animal control provisions (County parks):
--   "A. All animals must be controlled in accordance with the Queen
--       Anne's County Department of Parks and Recreation Rules and
--       Regulations and abide by all provisions as set forth within the
--       Queen Anne's County Animal Control Ordinance (Chapter 9 of the
--       Code of Queen Anne's County).
--    B. Pets are strictly prohibited from Romancoke Pier, and Matapeake
--       Pier (Class e offense).
--    C. Nondomesticated animals. Grazing of animals is not allowed on
--       County parklands without special permission (Class e offense).
--    D. Domesticated animals.
--      (1) All pets, except for documented service animals, are
--          prohibited from bathing, picnic or other areas that are
--          specifically restricted and posted (Class e offense).
--      (2) All animals must be controlled by a leash or other similar
--          restraining device at all times (Class e offense).
--      (3) The owner or custodian of an animal may not allow ... to
--          defecate on public property, unless the owner or custodian
--          ... immediately thereafter removes and disposes of any and
--          all waste in a sanitary manner (Class e offense).
--      (4) At no time may an animal injure, molest or intimidate another
--          individual or animal, chase vehicles or bicycles (Class b
--          offense)."
--
-- ─── Layered authority decision ─────────────────────────────────────
--
-- Per [[feedback_citywide_leash_inference]]: territorial leash / at-large
-- rules apply to beaches by default. Queen Anne's County § 9-17 is a
-- countywide restraint requirement (any animal off the owner's property
-- must be under restraint = leash, voice-control at heel, or vehicle).
-- § 22-21 is the more-specific overlay for County-owned/operated park
-- properties and explicitly requires leashing "at all times."
--
-- Both authorities yield the same operative rule for these 6 beaches:
-- on_leash. ONE policy_source row covers the layered citation; one bps
-- row per beach with section='sand', rule='on_leash'.
--
-- ─── Matapeake State Park — ownership vs operation ──────────────────
--
-- Matapeake State Park is MARYLAND-OWNED but COUNTY-LEASED/OPERATED
-- (per Maryland DNR ↔ Queen Anne's County lease). Operating authority
-- = Queen Anne's County Parks & Recreation; the county-park §§ 22-21
-- + 9-17 apply, NOT COMAR 08.07.06.17 (which the MD state baseline
-- migration limits to genuinely state-MANAGED parks: Sandy Point,
-- Greenwell, Janes Island, Point Lookout, Calvert Cliffs, Assateague
-- — explicitly excluding county-leased units).
--
-- ─── Kent Island Dog Beach — off-leash carve-out is OPERATOR-POSTED ─
--
-- Kent Island Dog Beach (fid 16231547) sits inside the Matapeake
-- complex. The off-leash designation is published by Queen Anne's
-- County Parks & Recreation on qac.org ("the dog beach... off-leash
-- play is allowed on the beach itself"), NOT in the codified ordinance.
-- § 22-21(D)(2) of the COUNTY CODE still says "All animals must be
-- controlled by a leash ... at all times," with no codified carve-out
-- for the dog-beach area. Per playbook tenet 2 ("defer > fabricate")
-- and the operator-vs-codified distinction:
--   - This codify migration encodes the CODIFIED baseline (on_leash)
--     for Kent Island Dog Beach.
--   - The off-leash overlay is an OPERATOR-POSTED rule and will land
--     via the Operate pipeline as an operator_posted_policy row
--     (region_name='Kent Island Dog Beach', rule=off_leash) attached
--     to operator='Queen Anne's County Department of Parks and
--     Recreation', source = the qac.org Matapeake page.
-- Consensus engine will then layer the operator-posted off-leash rule
-- over the codified on_leash baseline at run time.
--
-- ─── Per-beach mapping (6 scoreable Queen Anne's County MD beaches) ─
--
-- All 6 beaches sit on county-operated land (5 inside Kent Island /
-- Matapeake complex + Conquest Preserve on Spaniard Neck). None have
-- CPAD/PAD-US unit attribution (cpad_unit_id IS NULL for all 6) —
-- consistent with state-land that's county-leased (PAD-US doesn't
-- always populate for leased state parcels).
--
-- Public-access county-park / preserve beaches in the set:
--   - Conquest Beach (fid 12959) — Conquest Preserve, 758-acre county
--     waterfront preserve NW of Centreville on Spaniard Neck, on
--     Chester / Corsica River. County-acquired 1998-2003.
--   - North Beach (fid 13576) — Kent Island south end (Romancoke
--     area, lat 38.878 / lon -76.364). Small unincorporated waterfront.
--     [NOTE: distinct from Town of North Beach in Calvert County which
--     is codified separately in 20260522_md_calvert_county_codify.sql.]
--   - South Beach (fid 13577) — Kent Island south end (Romancoke
--     area, lat 38.869 / lon -76.367). Small unincorporated waterfront
--     just south of North Beach.
--   - Matapeake Clubhouse and Beach (fid 8535748) — county-operated
--     unit of Matapeake State Park (state-owned, county-leased).
--   - Kent Island Dog Beach (fid 16231547) — county-operated, inside
--     Matapeake. CODIFIED rule = on_leash (§ 22-21(D)(2)); off-leash
--     overlay deferred to Operate pipeline per note above.
--   - Matapeake Clubhouse and Public Beach (fid 17462491) — duplicate-
--     name unit of Matapeake State Park complex (state-owned,
--     county-leased).

-- ─── 1. Provision Queen Anne's County agency (canonical bare-name) ──

INSERT INTO public.agency (name, type, web_url)
SELECT 'Queen Anne''s County', 'county'::agency_type, 'https://www.qac.org/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency WHERE name='Queen Anne''s County' AND type='county'::agency_type
);

-- ─── 2. Insert codified-ordinance policy_source ─────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Queen Anne''s County Code Ch 9 Animal Control, Art III § 9-17 (Animals at large; § 9-17B(2) — public recreation areas must control animal by leash or other restraining device) + Ch 22 Parks and Recreation, Art III § 22-21 (Animal control provisions — § 22-21(D)(2) animals must be leashed at all times on County park properties)',
       (SELECT id FROM public.agency WHERE name='Queen Anne''s County' AND type='county'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/45162679',
       $body$Queen Anne's County Code, Part III Local Regulations, Chapter 9 Animal Control [adopted 4-9-2024 by Ord. No. 24-08; repealed and replaced prior Ch 9 adopted 9-9-2014 by Ord. No. 14-13], Article III Registration; Commercial Animal Establishment License; Animal Care, § 9-17 Animals at large.

A. Prohibited. Animals at large are prohibited.
(1) Includes any animal off the property of the owner or custodian without being under restraint.
  (a) Domestic animals. Under restraint means:
    [1] Secured by a leash or lead; or
    [2] Under the effective control of its owner, custodian, or other responsible person (at heel and directly beside) by way of training of the animal and handler; or
    [3] Within a vehicle being driven, standing, or parked in a manner that does not endanger the animal's health or safety.
  (b) Unattended or domesticated livestock. Under restraint means confined within a fence or enclosure of suitable material capable of holding the animal.
(2) Does not include:
  (a) An animal at large with a person in immediate pursuit.
  (b) An animal that is unrestrained in a permitted or appropriate area.
  (c) Community cats.
  (d) An animal engaging in supervised obedience training or hunting activities in an authorized area and supervised by a competent person.
B. School and County property.
(1) An animal may not be on school grounds on a day when school is in session without the permission of the proper authority.
(2) If in a public recreation area, the animal must be controlled by a leash or other similar restraining device.
(3) An animal may not be on any County property that is properly posted against such animals.
C. Duty to report. A person who observes an animal at large shall report the finding to the Animal Control Office.
D. Pursuit. An Animal Control Officer who observes an animal at large may pursue that animal on public or private property.
E. Animal pursuing livestock ... A person may kill any animal which he or she sees in the act of pursuing, attacking, wounding, or killing any poultry, livestock, ... or a human being.
---
Chapter 22 Parks and Recreation, Article III Use of County Parks, § 22-21 Animal control provisions.

A. All animals must be controlled in accordance with the Queen Anne's County Department of Parks and Recreation Rules and Regulations and abide by all provisions as set forth within the Queen Anne's County Animal Control Ordinance (Chapter 9 of the Code of Queen Anne's County).
B. Pets are strictly prohibited from Romancoke Pier, and Matapeake Pier (Class e offense).
C. Nondomesticated animals. Grazing of animals is not allowed on County parklands without special permission (Class e offense).
D. Domesticated animals.
(1) All pets, except for documented service animals, are prohibited from bathing, picnic or other areas that are specifically restricted and posted (Class e offense).
(2) All animals must be controlled by a leash or other similar restraining device at all times (Class e offense).
(3) The owner or custodian of an animal may not allow his or her animal or any animal under his or her care to defecate on public property, unless the owner or custodian of the animal immediately thereafter removes and disposes of any and all waste in a sanitary manner (Class e offense).
(4) At no time may an animal injure, molest or intimidate another individual or animal, chase vehicles or bicycles (Class b offense).

[Hosted on ecode360 (General Code) at ecode360.com/45162679 (§ 9-17 deep link) and ecode360.com/35472191 (§ 22-21 deep link); chapter container ecode360.com/45162609 (Ch 9 Art III) + ecode360.com/35472224 (Ch 22 Art III); custid QU1770. Fetched verbatim via Playwright 2026-05-22.]$body$
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Queen Anne''s County Code Ch 9%§ 9-17%'
);

-- ─── 3. Per-beach BPS rows — on_leash baseline for all 6 ────────────
--
-- Countywide § 9-17 restraint + park-specific § 22-21(D)(2) leash
-- requirement both yield rule=on_leash. Section='sand', operative.
-- Kent Island Dog Beach gets the codified on_leash baseline here; the
-- operator-posted off-leash carve-out (qac.org) lands separately via
-- the Operate pipeline.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       $ev$Queen Anne's County Code § 9-17A(1)(a)[1]: "Under restraint means: Secured by a leash or lead..." § 9-17B(2): "If in a public recreation area, the animal must be controlled by a leash or other similar restraining device." § 22-21(D)(2) reinforces for county park properties: "All animals must be controlled by a leash or other similar restraining device at all times."$ev$,
       ps.source_url,
       v.note,
       NULL
FROM (values
        (12959,    'Conquest Beach — Conquest Preserve, county-owned waterfront on Spaniard Neck (Chester/Corsica River). § 22-21 county park rule applies; § 9-17 countywide restraint also binds.'),
        (13576,    'North Beach (Kent Island, Queen Anne''s County) — small unincorporated Kent Island south-end waterfront near Romancoke (NOT the Town of North Beach in Calvert County). § 9-17 countywide restraint applies; if on county-posted park property, § 22-21 also applies.'),
        (13577,    'South Beach (Kent Island, Queen Anne''s County) — small unincorporated Kent Island south-end waterfront near Romancoke. § 9-17 countywide restraint applies; if on county-posted park property, § 22-21 also applies.'),
        (8535748,  'Matapeake Clubhouse and Beach — state-owned, county-leased/operated (QAC Parks & Rec). § 22-21(D)(2) leash-at-all-times applies as operator/county code; COMAR 08.07.06.17 deliberately NOT attached because per [[md-state-baseline-codify]] COMAR only governs state-MANAGED parks, not county-leased units. § 22-21(B) bans pets from Matapeake Pier specifically — not the beach.'),
        (16231547, 'Kent Island Dog Beach — inside Matapeake complex (state-owned, county-operated). Codified baseline = on_leash per § 22-21(D)(2) "All animals must be controlled by a leash ... at all times." Off-leash designation is OPERATOR-POSTED on qac.org (not codified) and will land via the Operate pipeline as a separate operator_posted_policy row with region_name=''Kent Island Dog Beach'' + rule=off_leash; consensus engine will then layer that operator overlay over this codified baseline.'),
        (17462491, 'Matapeake Clubhouse and Public Beach — duplicate-name unit of Matapeake State Park complex (state-owned, county-leased/operated). Same authority as fid 8535748: § 22-21(D)(2) leash-at-all-times; COMAR not attached per county-leased rationale above.')
     ) AS v(fid, note)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Queen Anne''s County Code Ch 9%§ 9-17%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule) DO NOTHING;
