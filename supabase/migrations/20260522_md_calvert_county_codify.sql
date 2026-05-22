-- MD Calvert County codify — Chapter 7 (Animals) + Chapter 82 (Parks and
-- Recreation) + Town of North Beach Chapter 26 (Animals)
--
-- Western Shore Chesapeake — 11 scoreable beaches.
--
-- ─── Sources ────────────────────────────────────────────────────────
--
-- (a) Calvert County Code, Chapter 7 ANIMALS, Part 6 Prohibited Acts,
--     Article IV "At-Large; Trespass" — § 7-6-401 (Animals at-large).
--     Adopted by the Board of Commissioners of Calvert County 3-25-2008
--     by Ord. No. 07-08.
--     URL (section deep-link): https://ecode360.com/15518812
--
-- (b) Calvert County Code, Chapter 82 PARKS AND RECREATION, Article 2
--     Park Regulation — § 82-2-106 (Animals). Adopted by the Board of
--     County Commissioners of Calvert County 10-22-2019 by Ord. No.
--     32-19.
--     URL (chapter deep-link): https://ecode360.com/36414814
--     §82-2-106(B)(2) EXPLICITLY BANS dogs (non-service) at Breezy
--     Point Beach.
--
-- (c) Town of North Beach Code, Part II General Legislation, Chapter 26
--     ANIMALS, Article II Dogs — § 26-3 (Dogs running at large and dogs
--     on the sand beach). Amended 7-31-2012 by Ord. No. 12-04.
--     URL (chapter deep-link): https://ecode360.com/9917831
--     §26-3(A) EXPRESSLY PROHIBITS dogs (at large or on leash) on the
--     sand of the Town beach.
--
-- Chesapeake Beach (Town) adopts Calvert County Chapter 7 by reference
-- via §104-1 (no Chesapeake-Beach beaches in this list — only North
-- Beach has incorporated-town overlay among the 11 in scope).
--
-- ─── Operative rules ────────────────────────────────────────────────
--
-- § 7-6-401 (Calvert County baseline — applies to all dogs off-owner-
-- property anywhere in the unincorporated county; per § 26-5 of North
-- Beach + § 104-1 of Chesapeake Beach the county chapter ALSO applies
-- within incorporated town limits):
--   A. Except as provided in Subsection B below:
--      (2) When a dog is off the real property of the owner, keeper or
--          custodian, the dog shall be on a leash, accompanied by and
--          under the control of a person who is capable of controlling
--          and physically restraining the dog.
--   B. Exceptions.
--      (1) Pursuit of accidentally at-large animal.
--      (2) Animals participating in a qualified activity.
--      (3) Dogs located within the designated fenced area of a public
--          park or dog park are not required to be on a leash.
--   C. At-large + threatening = dangerous animal.
--
-- § 82-2-106(A)(1) (Calvert County Parks — leash):
--   Domestic animals must be under restraint or control of a competent
--   person by means of a chain, leash, or other similar device, or is
--   in a secure cage or other secure enclosure.
--
-- § 82-2-106(B)(2) (Calvert County Parks — prohibition at named units):
--   Notwithstanding the provisions of § 82-2-106A, above, it is
--   unlawful for any person on park property: ... (2) To bring animals,
--   with the exception of service or law enforcement animals, to pool
--   facilities, within indoor facilities, Battle Creek Cypress Swamp
--   Sanctuary, Breezy Point Beach, or Chesapeake Hills Golf Course
--   without express written authorization from the Director.
--
-- North Beach § 26-3(A):
--   No person shall own or harbor within the Town of North Beach any
--   dog unless such dog is licensed as provided by the ordinances and
--   regulations of Calvert County. All ordinances and regulations of
--   Calvert County concerning dogs shall apply within the corporate
--   limits of the Town of North Beach. In addition, this includes the
--   provisions of the Calvert County Code which call for all dogs to be
--   on a leash when in the company of their owner outside the confines
--   of the owner's property. In addition, it is expressly prohibited
--   for any dog to be on the Town beach, whether said dog be at large
--   or on a leash. It is the intention of the Council that dogs are
--   prohibited on the sand beach under all circumstances.
--
-- Rule classification per beach:
--   * § 7-6-401(A)(2) sets the COUNTYWIDE on_leash baseline (territorial
--     rule → applies to beaches by default per
--     [[citywide-leash-inference]]).
--   * § 82-2-106(B)(2) override → not_allowed at Breezy Point Beach.
--   * North Beach § 26-3(A) override → not_allowed at North Beach
--     (incorporated-town sand).
--
-- ─── Per-beach mapping (11 scoreable) ───────────────────────────────
--
--   Calvert County baseline (§ 7-6-401, on_leash):
--     fid 17118831  Beach on the Chesapeake Bay  (Huntingtown,
--                   unincorporated)
--     fid 12961     Cove Point Beach             (Lusby, unincorporated;
--                   Cove Point Light area — not a county park, no
--                   Chapter 82 overlay)
--     fid 17681520  Dares Beach                  (Prince Frederick,
--                   unincorporated)
--     fid 8398580   Flag Harbor Beach            (St Leonard,
--                   unincorporated; private waterfront community,
--                   county code is the public floor)
--     fid 6363129   Flag Ponds                   (Lusby — Flag Ponds
--                   Nature Park, county-operated; § 82-2-106(A)(1)
--                   leash + § 7-6-401 BOTH apply; same on_leash result)
--     fid 13420     Long Beach                   (Long Beach CDP,
--                   unincorporated)
--     fid 14896     Matoaka Beach                (Calvert Beach CDP,
--                   unincorporated; private community access, county
--                   code is the public floor)
--
--   Calvert County Parks prohibition (§ 82-2-106(B)(2), not_allowed):
--     fid 14895     Breezy Point Beach           (Calvert County Parks
--                   property; § 82-2-106(B)(2) names "Breezy Point
--                   Beach" verbatim. Note: campground/camper-only sand
--                   carve-out per operator posting — out of scope here;
--                   public beach = no dogs.)
--
--   Town of North Beach (§ 26-3(A), not_allowed):
--     fid 10890720  North Beach                  (incorporated Town of
--                   North Beach; sand beach)
--     fid 7595112   North Beach Boardwalk        (incorporated Town of
--                   North Beach; the boardwalk strip itself is paved
--                   right-of-way where leashed dogs are permitted under
--                   § 7-6-401 baseline, BUT § 26-3(A) bans dogs on the
--                   sand. This beach record sits on the sand fronting
--                   the boardwalk → not_allowed.)
--
--   DEFER (HOA-private, operator authority):
--     fid 1519331   Lake Lariat Beach            (Chesapeake Ranch
--                   Estates HOA — privately-owned recreational lake in
--                   a gated community; access restricted to members.
--                   Per playbook defer rubric: HOA-private = no public
--                   bps row; operator posts its own rules. County code
--                   does not extend to private HOA waterbodies.)
--
-- ─── Judgment calls ────────────────────────────────────────────────
--
-- 1. Lake Lariat Beach (fid 1519331) DEFERRED. Chesapeake Ranch Estates
--    is a private HOA community on the southern tip of Calvert; the
--    lake is a private amenity, not a public beach. Pattern matches
--    Canyon Lake / Indian Beach deferrals. No bps row written; consider
--    operator_posted_policy when CRE HOA rules become available.
-- 2. North Beach Boardwalk (fid 7595112) classified not_allowed. The
--    code distinguishes the boardwalk (paved town ROW, leashed-dog OK
--    under § 7-6-401) from the sand beach (banned under § 26-3(A)).
--    Since this beach record is the SAND fronting the boardwalk, the
--    operative rule is § 26-3(A) ban. Status_note flags the boardwalk
--    distinction for the consumer surface to render.
-- 3. Breezy Point Beach (fid 14895) classified not_allowed via the
--    chapter-deep-link Ch. 82 URL (section page wasn't surfaced via
--    web search; the chapter contains § 82-2-106 verbatim and is the
--    deepest stable link available — better than the agency catalog).
--    Per-section IDs in ecode360 are opaque and not derivable.
-- 4. Flag Ponds (fid 6363129) is a county Natural Resources Park; both
--    § 82-2-106(A)(1) (parks leash) and § 7-6-401 (countywide leash)
--    apply. Single bps row cites § 7-6-401 as primary (countywide
--    territorial leash); status_note records the layered park rule.
-- 5. Beaches in private waterfront communities (Flag Harbor, Matoaka,
--    Cove Point) inherit the countywide § 7-6-401 baseline as the
--    public-law floor. If/when operator HOA rules surface, they'd
--    layer as operator_posted_policy on top.
-- 6. Chesapeake Beach (Town) has no in-scope beaches in this list but
--    its § 104-1 adoption-by-reference is documented for future ps
--    rows in that town.
--
-- ─── Agency provisioning ────────────────────────────────────────────
--
-- No "Calvert County" type=county or "North Beach" type=city agency
-- row exists yet (verified 2026-05-22). Create canonical bare-name
-- rows.

-- ─── 1. Provision Calvert County + North Beach agencies ────────────

INSERT INTO public.agency (name, type, web_url)
SELECT 'Calvert County', 'county'::agency_type, 'https://www.calvertcountymd.gov/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency WHERE name='Calvert County' AND type='county'::agency_type
);

INSERT INTO public.agency (name, type, web_url)
SELECT 'North Beach', 'city'::agency_type, 'https://www.northbeachmd.org/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency WHERE name='North Beach' AND type='city'::agency_type
);

-- ─── 2. Insert codified-ordinance policy_source rows ────────────────

-- (a) Calvert County Chapter 7 § 7-6-401 (countywide at-large/leash)

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Calvert County Code, Chapter 7 Animals, Part 6 Prohibited Acts, Article IV At-Large; Trespass, § 7-6-401 (Animals at-large)',
       (SELECT id FROM public.agency WHERE name='Calvert County' AND type='county'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/15518812',
       E'Calvert County Code, Chapter 7 ANIMALS [Adopted by the Board of Commissioners of Calvert County 3-25-2008 by Ord. No. 07-08], Part 6 Prohibited Acts, Article IV At-Large; Trespass. '
       '§ 7-6-401 Animals at-large. A. Except as provided in Subsection B below: (1) When an animal is off the real property of the owner, keeper or custodian, the animal shall be accompanied by and under the control of a person who is capable of controlling and physically restraining the animal. (2) When a dog is off the real property of the owner, keeper or custodian, the dog shall be on a leash, accompanied by and under the control of a person who is capable of controlling and physically restraining the dog. '
       'B. Exceptions. (1) When a person is in physical pursuit of an animal that is accidentally at-large, the animal shall not be deemed at-large. (2) Animals participating in a qualified activity are not at-large unless the animal leaves the qualified activity. (3) Dogs located within the designated fenced area of a public park or dog park are not required to be on a leash. '
       'C. An at-large animal that is threatening a human being, other animal or livestock without provocation shall be deemed a dangerous animal. If the Animal Control Officer(s) is unable to catch the animal, after exhausting all other means to capture the animal, the Animal Control Officer or a law enforcement officer is authorized to shoot the animal. '
       '§ 7-6-402 Animal trespass. It shall be unlawful for an owner, keeper or custodian to permit an animal to enter private property without the property owner''s permission. '
       '§ 7-6-403 Violations and penalties. A violation of this article is a Class A violation. '
       '[Hosted on ecode360 at ecode360.com/15518812 (Calvert County custid CA1802). Adopted 3-25-2008 by Ord. No. 07-08. Fetched verbatim via Playwright 2026-05-22.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Calvert County Code, Chapter 7%§ 7-6-401%'
);

-- (b) Calvert County Chapter 82 § 82-2-106 (parks animals — Breezy Point ban)

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Calvert County Code, Chapter 82 Parks and Recreation, Article 2 Park Regulation, § 82-2-106 (Animals — Breezy Point Beach prohibition)',
       (SELECT id FROM public.agency WHERE name='Calvert County' AND type='county'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/36414814',
       E'Calvert County Code, Chapter 82 PARKS AND RECREATION [Adopted by the Board of County Commissioners of Calvert County 10-22-2019 by Ord. No. 32-19], Article 2 Park Regulation. '
       '§ 82-2-106 Animals. A. Domestic animals are generally permitted in and on park property during park hours of operation with the following restrictions: (1) Domestic animals must be under restraint or control of a competent person by means of a chain, leash, or other similar device, or is in a secure cage or other secure enclosure. This provision shall not apply to working and hunting animals that are working or hunting on park property with the approval of the Director; and are under their handlers'' direct and immediate voice control, and are responsive to their handlers'' commands. '
       'B. Notwithstanding the provisions of § 82-2-106A, above, it is unlawful for any person on park property: (1) To bring animals, with the exception of service or law enforcement animals, on athletic fields; (2) To bring animals, with the exception of service or law enforcement animals, to pool facilities, within indoor facilities, Battle Creek Cypress Swamp Sanctuary, Breezy Point Beach, or Chesapeake Hills Golf Course without express written authorization from the Director; (3) To have a horse or horseback ride outside of designated areas, with the exception of service or law enforcement animals; (4) To intentionally molest, harm, hunt, kill, trap, frighten, or chase any animal, unless authorized in writing by the Board of County Commissioners, State of Maryland, or Calvert County Sheriff''s Office to do so. '
       '[Hosted on ecode360 at ecode360.com/36414814 (Calvert County custid CA1802, chapter deep-link — per-section opaque IDs not derivable). Adopted 10-22-2019 by Ord. No. 32-19. Fetched verbatim via Playwright 2026-05-22.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Calvert County Code, Chapter 82%§ 82-2-106%'
);

-- (c) Town of North Beach Chapter 26 § 26-3 (dogs banned on Town sand)

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Town of North Beach Code, Part II General Legislation, Chapter 26 Animals, Article II Dogs, § 26-3 (Dogs running at large and dogs on the sand beach)',
       (SELECT id FROM public.agency WHERE name='North Beach' AND type='city'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/9917831',
       E'Town of North Beach Code, Part II General Legislation, Chapter 26 ANIMALS [Adopted by the Mayor and Town Council of the Town of North Beach], Article II Dogs [Adopted as Art. XII of the 1982 Code]. '
       '§ 26-3 Dogs running at large and dogs on the sand beach; dangerous animals. [Amended 3-8-2000 by Ord. No. 00-3; 8-2-2000 by Ord. No. 10-00; 7-31-2012 by Ord. No. 12-04.] '
       'A. No person shall own or harbor within the Town of North Beach any dog unless such dog is licensed as provided by the ordinances and regulations of Calvert County. All ordinances and regulations of Calvert County concerning dogs shall apply within the corporate limits of the Town of North Beach. In addition, this includes the provisions of the Calvert County Code which call for all dogs to be on a leash when in the company of their owner outside the confines of the owner''s property. In addition, it is expressly prohibited for any dog to be on the Town beach, whether said dog be at large or on a leash. It is the intention of the Council that dogs are prohibited on the sand beach under all circumstances. '
       'B. Dangerous animals. ... it is prohibited for a dangerous animal to be on any Town right-of-way, including streets, the boardwalk and/or the beach, at any time, whether with its owner or not. '
       '§ 26-4 Sanitation. A. No person owning, harboring, keeping or in charge of any dog shall cause, suffer or allow such dog to soil, defile or defecate on any public property or upon any thoroughfare, sidewalk, passageway, boardwalk, bikepath, beach or play area or any place where people congregate or walk or upon any private property other than that of the owner or custodian, unless such person immediately removes and disposes of all feces. '
       '§ 26-5 Calvert County licenses and regulations. No person shall own or harbor within the Town of North Beach any dog unless such dog is licensed as provided by the ordinances and regulations of Calvert County. All ordinances and regulations of Calvert County concerning dogs shall apply within the corporate limits of North Beach. '
       '§ 26-8 Violations and penalties. Violations of this article shall be a municipal infraction. '
       '[Hosted on ecode360 at ecode360.com/9917831 (Town of North Beach chapter deep-link — per-section opaque IDs not derivable). § 26-3(A) last amended 7-31-2012 by Ord. No. 12-04. Fetched verbatim via Playwright 2026-05-22.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Town of North Beach Code%§ 26-3%'
);

-- ─── 3. Per-beach BPS rows ──────────────────────────────────────────

-- (a) Calvert County baseline § 7-6-401 → on_leash (7 beaches)

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       E'Calvert County Code § 7-6-401(A)(2): "When a dog is off the real property of the owner, keeper or custodian, the dog shall be on a leash, accompanied by and under the control of a person who is capable of controlling and physically restraining the dog." (Ord. No. 07-08, 3-25-2008.) Countywide territorial leash rule — applies to beaches by default.',
       ps.source_url, NULL, NULL
FROM (values (17118831),(12961),(17681520),(8398580),(13420),(14896)) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Calvert County Code, Chapter 7%§ 7-6-401%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule) DO NOTHING;

-- (b) Flag Ponds (county Nature Park) → on_leash, layered Ch. 7 + Ch. 82

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT 6363129, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       E'Calvert County Code § 7-6-401(A)(2) countywide leash rule applies. § 82-2-106(A)(1) also applies (county park property): "Domestic animals must be under restraint or control of a competent person by means of a chain, leash, or other similar device." (Ord. No. 32-19, 10-22-2019.)',
       ps.source_url,
       'Flag Ponds Nature Park is a Calvert County Natural Resources park. Both Ch. 7 countywide leash and Ch. 82 county-park leash rules apply (same on_leash result). Battle Creek Cypress Swamp (which is excluded from dogs) is a separate park; Flag Ponds is dog-allowed-on-leash per operator posting.',
       NULL
FROM public.policy_source ps
WHERE ps.citation LIKE 'Calvert County Code, Chapter 7%§ 7-6-401%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule) DO NOTHING;

-- (c) Breezy Point Beach (county park named in § 82-2-106(B)(2)) → not_allowed

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT 14895, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       E'Calvert County Code § 82-2-106(B)(2): "Notwithstanding the provisions of § 82-2-106A, above, it is unlawful for any person on park property ... (2) To bring animals, with the exception of service or law enforcement animals, to pool facilities, within indoor facilities, Battle Creek Cypress Swamp Sanctuary, Breezy Point Beach, or Chesapeake Hills Golf Course without express written authorization from the Director." (Ord. No. 32-19, 10-22-2019.)',
       ps.source_url,
       'Public beach = no dogs (service/law-enforcement animals only). Operator-posted camper-only carve-out: leashed dogs are permitted at the campers'' beach inside Breezy Point Campground (members/campers only). Public-access beach record stays not_allowed.',
       NULL
FROM public.policy_source ps
WHERE ps.citation LIKE 'Calvert County Code, Chapter 82%§ 82-2-106%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule) DO NOTHING;

-- (d) Town of North Beach sand → not_allowed (2 beaches)

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT v.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       E'Town of North Beach Code § 26-3(A): "it is expressly prohibited for any dog to be on the Town beach, whether said dog be at large or on a leash. It is the intention of the Council that dogs are prohibited on the sand beach under all circumstances." (Amended 7-31-2012 by Ord. No. 12-04.)',
       ps.source_url,
       'Town of North Beach incorporated limits. Sand beach = no dogs at all (no leashed exception). The paved boardwalk strip itself is town right-of-way where leashed dogs are permitted under Calvert County § 7-6-401, but this beach record represents the SAND fronting the boardwalk — § 26-3(A) ban controls.',
       NULL
FROM (values (10890720),(7595112)) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Town of North Beach Code%§ 26-3%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule) DO NOTHING;

-- (e) Lake Lariat Beach (fid 1519331) DEFERRED.
--   Chesapeake Ranch Estates HOA — private gated-community lake, not a
--   public beach. County code does not extend to private HOA water-
--   bodies. No bps row written. Reclassify or add operator_posted_policy
--   when CRE HOA rules are documented.
