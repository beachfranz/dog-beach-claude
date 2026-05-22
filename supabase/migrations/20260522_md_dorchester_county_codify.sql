-- MD Dorchester County codify — Ch 78 Dog Control (countywide at-large/restraint)
--
-- Source: Dorchester County, MD Code of Public Local Laws, Part II
--   Administrative and General Legislation, Chapter 78 Dog Control.
--   [Adopted by County Council 8-15-2017 by Bill No. 2017-7. Repealed and
--    replaced former Ch. 78 adopted 9-9-2003 by Bill No. 2003-14.]
-- URL (deep-linked chapter):    https://ecode360.com/10453380
-- Definitions article:          https://ecode360.com/10456482
-- Jurisdiction root:            https://ecode360.com/DO0950 (custid DO0950)
-- (Fetched verbatim via Playwright 2026-05-22.)
--
-- ─── Operative rule derivation ──────────────────────────────────────
--
-- Dorchester County Ch 78 does NOT contain an explicit "leash" mandate
-- worded as a positive restraint requirement. Instead, the operative
-- mechanism is the AT-LARGE PROHIBITION via § 78-2 + § 78-5(A):
--
--   § 78-2 Definitions: "AT LARGE — A dog shall be deemed to be 'at large'
--   when off the property of the owner and not under restraint."
--
--   § 78-2 Definitions: "RESTRAINTS — Secure chain, cable or trolley,
--   invisible containment system, or other tether of sufficient strength
--   to prevent escape. If a dog is confined on a tether, excepting periods
--   of time that are brief and incidental, the tether shall be sufficient
--   in length and positioned to prevent tangling and hanging."
--
--   § 78-5(A) Seizure and impoundment: "The Dog Control Officer shall,
--   whenever possible, seize and impound any stray dog or dog at large,
--   any diseased or vicious dog or any public nuisance dog. ... The Dog
--   Control Officer may pursue any dog running at large onto public
--   property or on the unenclosed exterior premises of private property."
--
-- Per [[feedback_citywide_leash_inference]] (territorial at-large rules
-- apply to beaches by default) this yields rule=on_leash for any beach
-- inside the chapter's jurisdictional reach: off-owner-property dogs
-- must be under restraint (leash/cable/tether) on pain of seizure.
--
-- Note: § 78-2 RESTRAINTS allows "invisible containment system" — i.e.
-- the rule is restraint-with-tether, not strictly a 6-foot leash like
-- COMAR 08.07.06.17. This is closer to "restraint required" than
-- "physical leash required," but for downstream beach-rule purposes
-- on_leash is the correct mapping (no beach has invisible-fence
-- infrastructure; the practical compliance path is a leash).
--
-- ─── Jurisdictional reach — § 78-4 ──────────────────────────────────
--
-- § 78-4 Applicability: "The provisions of this chapter shall apply
-- throughout the UNINCORPORATED AREA of Dorchester County, and services
-- provided hereunder are available to any incorporated area upon request;
-- subject, however, to the availability and appropriation in the annual
-- budget of the County Council."
--
-- Both in-scope beaches are unincorporated:
--   - fid 12954  Cedar Grove Beach (Nanticoke River near Vienna, no
--     incorporated city overlap).
--   - fid 19519452 Cherry Beach (Nanticoke River; beaches_gold canonical
--     county_name='Dorchester' / county_fips=24019; the Sharptown +
--     Galestown C1 polygon proximity hits are CDP/town outlines on the
--     opposite Wicomico shore — neither town's incorporated boundary
--     covers Cherry Beach).
--
-- Both beaches inherit § 78-4 reach → § 78-2 + § 78-5(A) at-large rule.
--
-- ─── Federal-overlap triage (Blackwater NWR) ────────────────────────
--
-- Per [[feedback_dont_classify_unknowns_for_deletion]] + Franz' Blackwater
-- NWR note: checked beach_polygon_membership for pad_us_unit FED matches.
--
--   fid 12954  Cedar Grove Beach: NO FED matches. Only Higgins/Daly NGO
--     conservation polygons at 1.5-1.8 km (match_strength=1, NOT contained).
--     Not in Blackwater NWR. PROCEED with county codify.
--
--   fid 19519452 Cherry Beach: Blackwater NWR (FED/FWS) appears as
--     match_strength=1 nearby polygons at 527 m and 1799 m — i.e. NOT
--     containing the beach, just within the 2 km neighborhood radius.
--     The actual containing polygon (match_strength=2, dist 247 m) is
--     "Cherry Beach Park" (LOC/UNKL — a local park, NOT a refuge unit).
--     Cherry Beach is on the upper Nanticoke River north of the Blackwater
--     NWR core (Blackwater is centered ~30 km south near the Blackwater
--     River). PROCEED with county codify; no federal override applies.
--
-- ─── Per-beach disposition ──────────────────────────────────────────
--
-- fid 12954     Cedar Grove Beach  → Ch 78 at-large rule → on_leash
-- fid 19519452  Cherry Beach       → Ch 78 at-large rule → on_leash
--                                    (status_note flags Cherry Beach Park
--                                     as likely-local-park; operator-posted
--                                     park-specific rule may layer later
--                                     via Operate pipeline.)
--
-- ─── No state-park or federal layer here ────────────────────────────
--
-- Neither beach sits inside the MD state-park system (so COMAR 08.07.06.17
-- does NOT attach via the 20260522_md_state_baseline_codify migration).
-- The honest state_dogs_policy MD fallback (no statewide leash law) is
-- superseded for these two beaches by this codified county at-large rule.

begin;

-- ─── 1. Provision Dorchester County agency (canonical bare-name) ────

INSERT INTO public.agency (name, type, web_url)
SELECT 'Dorchester County', 'county'::agency_type, 'https://www.dorchestercountymd.com/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency WHERE name='Dorchester County' AND type='county'::agency_type
);

-- ─── 2. Insert codified-ordinance policy_source ─────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Dorchester County, MD Code Ch 78 Dog Control § 78-2 (Definitions: AT LARGE, RESTRAINTS) + § 78-4 (Applicability — unincorporated area) + § 78-5(A) (Seizure of dogs at large)',
       (SELECT id FROM public.agency WHERE name='Dorchester County' AND type='county'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/10453380',
       $body$Dorchester County, MD Code of Public Local Laws, Part II Administrative and General Legislation, Chapter 78 Dog Control [Adopted by the County Council of Dorchester County 8-15-2017 by Bill No. 2017-7; repealed and replaced former Ch. 78 adopted 9-9-2003 by Bill No. 2003-14, as amended].

§ 78-1 Short title. This chapter shall be known as and may be cited as the "Dorchester County Dog Control Ordinance."

§ 78-2 Definitions.
AT LARGE — A dog shall be deemed to be "at large" when off the property of the owner and not under restraint.
RESTRAINTS — Secure chain, cable or trolley, invisible containment system, or other tether of sufficient strength to prevent escape. If a dog is confined on a tether, excepting periods of time that are brief and incidental, the tether shall be sufficient in length and positioned to prevent tangling and hanging.
OWNER — A person having the right of property or custody of a dog or who keeps or harbors a dog or knowingly permits a dog to remain on or about any premises occupied by the person.
PUBLIC NUISANCE — A dog shall be deemed a public nuisance when it is a danger to any person or when it engages in activities which disturb the peace and quiet of any neighborhood, including, but not limited to, excessive barking, whining or howling, chasing vehicles, attacking other domestic animals or damaging property.

§ 78-3 Administration; enforcement responsibilities. A. This chapter shall be administered and enforced by the Dorchester County Sheriff's Office (the "Department").

§ 78-4 Applicability of provisions generally. The provisions of this chapter shall apply throughout the unincorporated area of Dorchester County, and services provided hereunder are available to any incorporated area upon request; subject, however, to the availability and appropriation in the annual budget of the County Council. The County Council is not under any legal obligation to provide animal control services to any incorporated city or town within Dorchester County.

§ 78-5(A) Seizure and impoundment. The Dog Control Officer shall, whenever possible, seize and impound any stray dog or dog at large, any diseased or vicious dog or any public nuisance dog. Every dog so seized and impounded shall be cared for and fed by the Department until disposition is made thereof as directed. The Dog Control Officer may pursue any dog running at large onto public property or on the unenclosed exterior premises of private property.

§ 78-11 Civil infractions; violations and penalties — at-large violations are enforced via the County's civil infractions schedule (Ch 147).

[Hosted on ecode360 (General Code) at ecode360.com/10453380 (Chapter 78 deep link); definitions article at ecode360.com/10456482; custid DO0950. Fetched verbatim via Playwright 2026-05-22.]$body$
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Dorchester County, MD Code Ch 78 Dog Control%'
);

-- ─── 3. Per-beach BPS rows — on_leash for both Dorchester beaches ──

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       $verb$Dorchester County Code § 78-2: "AT LARGE — A dog shall be deemed to be 'at large' when off the property of the owner and not under restraint." § 78-5(A): the Dog Control Officer "shall ... seize and impound any stray dog or dog at large ... and may pursue any dog running at large onto public property...". RESTRAINTS defined § 78-2 as "Secure chain, cable or trolley, invisible containment system, or other tether of sufficient strength to prevent escape." Applies to all unincorporated-area beaches per § 78-4.$verb$,
       ps.source_url,
       v.note,
       NULL
FROM (values
        (12954,    'Cedar Grove Beach — Nanticoke River shoreline near Vienna; unincorporated Dorchester County, no city overlap. § 78-4 reach + § 78-2/§ 78-5(A) at-large prohibition apply directly.'),
        (19519452, 'Cherry Beach — Nanticoke River near the Wicomico County boundary; canonical county=Dorchester per beaches_gold (county_fips 24019). PAD-US containing polygon is "Cherry Beach Park" (LOC/UNKL — local park, NOT Blackwater NWR; Blackwater hits in polygon_membership are nearby-only, match_strength=1, 527-1799 m distance). The C1-city Sharptown/Galestown overlaps are CDP/proximity hits across the river, not municipal-boundary containment. § 78-4 unincorporated-area reach applies; on_leash via § 78-2 + § 78-5(A) at-large rule. Operator-posted park-specific rules from the local park (if any) may layer later via Operate pipeline.')
     ) AS v(fid, note)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Dorchester County, MD Code Ch 78 Dog Control%'
ON CONFLICT (beach_fid, policy_source_id, section, COALESCE(region_name, '__default__'::text), rule) DO NOTHING;

commit;
