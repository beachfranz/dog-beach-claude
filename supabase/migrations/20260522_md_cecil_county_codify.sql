-- MD Cecil County codify — Ch 142 (Animal Care and Control) + Ch 261 (Parks and Rec)
--
-- Source: Cecil County Code, Part II General Legislation.
--   Chapter 142 Animal Care and Control, Article VI Standards for Care and
--     Keeping of Animals, § 142-20 Restraint.
--   Chapter 261 Parks and Recreation, § 261-5(C)(3) Park rules — Animals.
-- URLs (deep-linked):
--   § 142-20 Restraint:           https://ecode360.com/16162996
--   § 261-5 Park rules:           https://ecode360.com/16189200
-- Article-level container:        https://ecode360.com/16162931 (Art VI)
-- Chapter root:                   https://ecode360.com/16189181 (Ch 261)
-- (Both fetched verbatim via Playwright 2026-05-22; custid CE0748.)
--
-- Adoption:
--   Ch 142: adopted 7-17-2012 by Ord. No. 2011-02.
--   Ch 261: adopted 3-6-2012 by Ord. No. 2012-02 (Ch. 57, § 57-13, of the
--     1990 Code of Public Local Laws).
--
-- ─── Operative rules ────────────────────────────────────────────────
--
-- § 142-20 Restraint (verbatim, key subsections):
--   "A. Dogs shall be kept under restraint or effective control at all
--   times, unless the animal is engaged in supervised hunting or another
--   activity where restraint might endanger the animal's life or safety.
--   B. All dogs shall be restrained by leash, chain, kennel or fence.
--   C. The owner of an animal, other than a cat, shall prevent the animal
--   from leaving the owner's property unattended or unrestrained.
--   ...
--   G. Violations of this § 142-20 shall result in the assessment of fines
--   as follows: (1) Lack of restraint: up to $500. (2) At large: up to $500."
--
-- § 261-5(C)(3) Park rules — Animals (verbatim):
--   "No person shall: ... (3) Cause or permit any dog, cat or other
--   domesticated animal to run at large (must be leashed in public parks
--   and facilities) or to create a nuisance."
--
-- ─── Layered authority decision ─────────────────────────────────────
--
-- Per [[feedback_citywide_leash_inference]]: territorial leash/at-large
-- rules apply to beaches by default. Cecil County's § 142-20 is a
-- countywide restraint requirement (dogs MUST be on leash / under
-- effective control whenever off owner's property). The § 261-5(C)(3)
-- park rule is a more-specific overlay for County-owned parks and
-- explicitly requires leashing in "public parks and facilities."
--
-- Both authorities yield the same operative rule for these beaches:
-- on_leash. We write ONE policy_source row (the chapter-spanning
-- citation) and one bps row per beach with section='sand', rule='on_leash'.
-- If a beach is later confirmed to be a designated Cecil County park,
-- a supplementary bps row citing § 261-5 specifically can be added.
--
-- ─── Per-beach mapping (14 scoreable Cecil County MD beaches) ───────
--
-- All 14 beaches sit on upper Chesapeake / Elk River / Bohemia River
-- shoreline in Cecil County, MD. None have CPAD/PAD-US unit attribution
-- (cpad_unit_id IS NULL across the board → no federal/state agency
-- overrides apply). All fall under the countywide § 142-20 restraint rule
-- regardless of public/HOA ownership: the at-large prohibition binds the
-- animal owner whenever the dog is off the owner's own property.
--
-- Several beaches sit inside known HOA/community-association waterfronts
-- (West View Shores, Chesapeake Isle, Buttonwood Beach, Elkview Shores,
-- Indian Acres, Knights Island Preserve, Red Point Beach). Those community
-- beaches are open to homeowners + guests on association land; § 142-20
-- still applies because (a) the dog owner does not own the community
-- beach parcel and (b) any community beach falls outside "the owner's
-- property" for individual dog owners. Operator-posted HOA rules may
-- override later (those would land via the Operate pipeline as
-- operator_posted_policy rows; this migration ships the codified baseline).
--
-- Public-access beaches in the set: North East Beach Area (Town of North
-- East, but town has no separate codified animal control beyond the
-- county — Town of North East is in Cecil County and the County code
-- applies absent municipal override), Garrett Island Beach (Garrett
-- Island is a Susquehanna NWR USFWS unit — note below), Roachs Shore,
-- Elk River Beach, Hollywood Beach, Sandy Hill Beach, Grove Point Beach.
--
-- Judgment call — Garrett Island (fid 14021): Garrett Island is part of
-- Susquehanna National Wildlife Refuge (USFWS Region 5, Eastern Neck NWR
-- complex). Federal authority typically overrides county code. Since
-- cpad_unit_id is NULL (PAD-US attribution did not populate), the
-- countywide leash baseline applies as a floor; a future federal codify
-- pass for the NWR will layer on top via separate ps row. INCLUDED here.
--
-- Judgment call — Knights Island Preserve (fid 18350029): name "Preserve"
-- suggests conservation/nature trust ownership. Treated like other
-- non-CPAD private waterfronts: § 142-20 binds the dog owner; operator
-- overlay deferred.

-- ─── 1. Provision Cecil County agency (canonical bare-name) ─────────

INSERT INTO public.agency (name, type, web_url)
SELECT 'Cecil County', 'county'::agency_type, 'https://www.ccgov.org/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency WHERE name='Cecil County' AND type='county'::agency_type
);

-- ─── 2. Insert codified-ordinance policy_source ─────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Cecil County Code Ch 142 Animal Care and Control, Art VI § 142-20 (Restraint) + Ch 261 Parks and Recreation § 261-5(C)(3) (Park rules — Animals; leash required in public parks and facilities)',
       (SELECT id FROM public.agency WHERE name='Cecil County' AND type='county'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/16162996',
       E'Cecil County Code, Part II General Legislation, Chapter 142 Animal Care and Control [adopted 7-17-2012 by Ord. No. 2011-02], Article VI Standards for Care and Keeping of Animals, § 142-20 Restraint. '
       'A. Dogs shall be kept under restraint or effective control at all times, unless the animal is engaged in supervised hunting or another activity where restraint might endanger the animal''s life or safety. '
       'B. All dogs shall be restrained by leash, chain, kennel or fence. '
       'C. The owner of an animal, other than a cat, shall prevent the animal from leaving the owner''s property unattended or unrestrained. '
       'D. The owner of an animal(s) that is fenced shall erect fencing normally considered suitable for the species and type of animal to be contained. ... In the case of a dog, if the owner utilizes an "invisible fence" to restrain a dog it shall only be used if it consistently and effectively restrains the dog and is appropriate to be accessible for the dog in question. No vicious or dangerous dog shall be restrained behind an "invisible fence." '
       'E. Any dog at large that attacks another animal or person causing injury documented by a veterinarian or human doctor or causes property damage shall result in the dog''s owner being subject to penalties as set in Subsection G below. '
       'F. In addition to, or in lieu of, impounding an animal found at large, the Animal Care and Control Officer or law enforcement officer may issue to the known owner of such animal a citation. ... '
       'G. Violations of this § 142-20 shall result in the assessment of fines as follows: (1) Lack of restraint: up to $500. (2) At large: up to $500. '
       '--- '
       'Chapter 261 Parks and Recreation [adopted 3-6-2012 by Ord. No. 2012-02], § 261-5 Park rules and regulations. C. Animals. No person shall: (1) Pursue, catch, or kill any wildlife. (2) Disturb any nest, den or burrow of any animal or fowl. (3) Cause or permit any dog, cat or other domesticated animal to run at large (must be leashed in public parks and facilities) or to create a nuisance. '
       '[Hosted on ecode360 (General Code) at ecode360.com/16162996 (§ 142-20 deep link) and ecode360.com/16189200 (§ 261-5 deep link); chapter root ecode360.com/16162739 (Ch 142) + ecode360.com/16189181 (Ch 261); custid CE0748. Fetched verbatim via Playwright 2026-05-22.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Cecil County Code Ch 142%§ 142-20%'
);

-- ─── 3. Per-beach BPS rows — on_leash baseline for all 14 ───────────
--
-- Countywide § 142-20 restraint + park-specific § 261-5(C)(3) leash
-- requirement both yield rule=on_leash. Section='sand', operative.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       E'Cecil County Code § 142-20(B): "All dogs shall be restrained by leash, chain, kennel or fence." § 142-20(A) requires dogs to be "kept under restraint or effective control at all times" off the owner''s own property. § 261-5(C)(3) reinforces: "Cause or permit any dog ... to run at large (must be leashed in public parks and facilities)..." Applies to all upper-Chesapeake / Elk River beaches in Cecil County.',
       ps.source_url,
       v.note,
       NULL
FROM (values
        (12956,    'Roachs Shore — Cecil County waterfront. § 142-20 countywide leash applies.'),
        (14021,    'Garrett Island Beach — Garrett Island is part of Susquehanna NWR (USFWS). County leash baseline applies; future federal codify may layer a USFWS-specific rule on top.'),
        (14110,    'North East Beach Area — Town of North East within Cecil County. No municipal animal-control override identified; county § 142-20 applies.'),
        (3431650,  'Elk River Beach — Cecil County waterfront. § 142-20 countywide leash applies.'),
        (3550497,  'Buttonwood Beach — known RV resort / community waterfront. § 142-20 binds dog owner whenever off owner''s own property; operator-posted overlay deferred to Operate pipeline.'),
        (6802001,  'West View Shores — HOA community waterfront. § 142-20 binds dog owner; operator-posted HOA rules may override via separate operator_posted_policy row.'),
        (7218218,  'Hollywood Beach — Cecil County waterfront. § 142-20 countywide leash applies.'),
        (8911500,  'Chesapeake Isle — HOA community waterfront. § 142-20 binds dog owner; operator-posted HOA rules may override.'),
        (9240837,  'Sandy Hill Beach — Cecil County waterfront. § 142-20 countywide leash applies.'),
        (13883357, 'Grove Point Beach — Cecil County waterfront. § 142-20 countywide leash applies.'),
        (15955939, 'Elkview Shores — HOA community waterfront. § 142-20 binds dog owner; operator-posted HOA rules may override.'),
        (16986388, 'Indian Acres — RV / community waterfront resort. § 142-20 binds dog owner; operator-posted overlay deferred.'),
        (18350029, 'Knights Island Preserve — likely conservation preserve. § 142-20 binds dog owner; operator-posted overlay deferred pending operator identification.'),
        (19693342, 'Red Point Beach — community waterfront. § 142-20 binds dog owner; operator-posted overlay deferred.')
     ) AS v(fid, note)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Cecil County Code Ch 142%§ 142-20%'
ON CONFLICT (beach_fid, policy_source_id, section, COALESCE(region_name, '__default__'::text), rule) DO NOTHING;
