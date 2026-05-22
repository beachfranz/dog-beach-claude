-- MD Garrett County codify — Ch 91 Animal Control § 91.40 (Animals
-- Running at Large) + § 91.20 definition of "Animal Under Restraint"
--
-- Inland mountain county in westernmost MD (Allegheny Plateau). 1
-- in-scope beach in scope of this migration (Little Brown Lake, fid
-- 2646649). Two other Garrett County beaches — Deep Creek Lake State
-- Park (fid 5773745) + Herrington Manor State Park (fid 10346736) —
-- are already attached to COMAR (state-park system regulation) and are
-- EXPLICITLY EXCLUDED from this migration's bps inserts.
--
-- ─── Sources ─────────────────────────────────────────────────────────
--
-- (1) Garrett County, MD Code of Ordinances, Title IX General
--     Regulations, Chapter 91 Animal Control, REQUIREMENTS AND
--     REGULATIONS, § 91.40 Animals Running at Large [Ord. passed
--     2-3-2004].
--     Section deep-link:  https://codelibrary.amlegal.com/codes/garrettcounty/latest/garrettco_md/0-0-0-1773
--     Container article:  https://codelibrary.amlegal.com/codes/garrettcounty/latest/garrettco_md/0-0-0-1772
--     Chapter 91 root:    https://codelibrary.amlegal.com/codes/garrettcounty/latest/garrettco_md/0-0-0-1
--     (Hosted on amlegal — codelibrary.amlegal.com — General Code-class
--     property; jurisdiction slug "garrettcounty", doc slug
--     "garrettco_md". Per-section IDs use amlegal "0-0-0-N" stable
--     numeric form; § 91.40 = -1773. Fetched verbatim via Playwright
--     2026-05-22.)
--
-- (2) Garrett County Sheriff's Office Animal Control Ordinance page
--     (operator-administrative summary; cited for cross-reference of
--     the same Ord. text):
--     https://sheriff.garrettcountymd.gov/animal-control-shelter/animal-control-ordinance
--     (Not the canonical source; the amlegal codified URL above is the
--     citable codified record. Sheriff page used as verification cross-
--     check 2026-05-22.)
--
-- ─── Operative rule — verbatim ───────────────────────────────────────
--
-- § 91.40 ANIMALS RUNNING AT LARGE.
--   It shall be unlawful for the owner of any animal to allow the
--   animal to be at large in Garrett County, meaning to be off the
--   premises or property of the owner, unless under restraint, except
--   dogs accompanied by the owner or trainer and being used or trained
--   for hunting being followed by person on foot, horseback or in
--   vehicles.
--   (A) Impoundment procedure — animal found at large is impounded for
--       not less than 3 days...
--   (B) Owner of unlicensed dog/cat with complaint filed assessed fine
--       not less than $25 plus license costs.
-- (Ord. —, passed 2-3-2004)
--
-- § 91.20 ANIMAL UNDER RESTRAINT (definition, verbatim):
--   "An animal secured by a leash or lead, or under the control of a
--   responsible person and obedient to that person's commands, or
--   confined within a vehicle or within the real property limits of its
--   owner and/or its custodian."
--
-- § 91.99 PENALTY: civil fines per § 91.44(D)(2): first offense $50,
--   second $100, all others $250.
--
-- ─── Rule classification ─────────────────────────────────────────────
--
-- § 91.40 + § 91.20 definition is a COUNTYWIDE TERRITORIAL restraint
-- requirement: any animal off the owner's real property must be "under
-- restraint" — leash/lead OR voice control by a responsible person.
-- Per [[feedback_citywide_leash_inference]], territorial restraint
-- rules apply to beaches by default; explicit beach mention not
-- required.
--
-- Restraint definition mirrors Kent County § 64-1 (voice-control
-- alternative). Same dominant-reading call as Kent: classify as
-- on_leash with status_note documenting the voice-control alternative.
-- Coding this as off_leash_voice_control would overstate permission —
-- the operative noun is "restraint," and the dominant lay reading is
-- "leashed." Honest call: on_leash + carve-out narrative.
--
-- ─── Per-beach mapping (1 scoreable, this migration) ────────────────
--
--   fid 2646649  Little Brown Lake  (Oakland area, inland recreational
--                lake in Garrett County mountain region; no operator
--                overlay identified; county § 91.40 baseline binds)
--
-- EXCLUDED (already COMAR-attached, NO bps row written here):
--   fid 5773745   Deep Creek Lake State Park    (MD DNR — COMAR
--                 09.20.07 / 08.01.07 state-park rules)
--   fid 10346736  Herrington Manor State Park   (MD DNR — same)
--
-- ─── Judgment calls ─────────────────────────────────────────────────
--
-- 1. Garrett County is on amlegal (codelibrary.amlegal.com), NOT
--    ecode360 like most other MD counties this batch. Slug
--    "garrettcounty"; doc "garrettco_md". Per-section IDs use amlegal
--    stable numeric form ("0-0-0-1773" for § 91.40). Recorded for
--    future MD codify work.
-- 2. Garrett is an inland mountain county with no coastline. "Beaches"
--    in scope are inland recreational lake shorelines (Deep Creek
--    Lake, Herrington Manor, Little Brown Lake). For Little Brown Lake
--    specifically, no operator overlay was identified during discovery
--    — the county § 91.40 baseline is what binds.
-- 3. on_leash classification follows the Kent County precedent for
--    "restraint" + voice-control definitional language: the
--    territorial requirement is "restraint", the dominant lay reading
--    is leashed; voice-control alternative captured in status_note
--    rather than upgrading the rule to off_leash_voice_control.
-- 4. Hunting carve-out in § 91.40 (dogs accompanied by owner/trainer
--    being used for hunting) is upland-game-specific and does not
--    apply to recreational beach use — not material for Little Brown
--    Lake.
-- 5. No agency.web_url exists yet for "Garrett County" county-type;
--    canonical bare-name created with the county's official .gov
--    domain.
--
-- ─── Agency provisioning ────────────────────────────────────────────
--
-- Create canonical bare-name "Garrett County" type=county row if not
-- already present (per [[feedback_url_resolution_field_guide]] tenet 4
-- canonical bare-name).

-- ─── 1. Provision Garrett County agency ────────────────────────────

INSERT INTO public.agency (name, type, web_url)
SELECT 'Garrett County', 'county'::agency_type, 'https://www.garrettcountymd.gov/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency WHERE name='Garrett County' AND type='county'::agency_type
);

-- ─── 2. Insert codified-ordinance policy_source ─────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Garrett County, MD Code of Ordinances, Title IX Ch 91 Animal Control, § 91.40 (Animals Running at Large) + § 91.20 (Definition: Animal Under Restraint)',
       (SELECT id FROM public.agency WHERE name='Garrett County' AND type='county'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://codelibrary.amlegal.com/codes/garrettcounty/latest/garrettco_md/0-0-0-1773',
       $body$Garrett County, MD Code of Ordinances, Title IX General Regulations, Chapter 91 ANIMAL CONTROL [Ord. passed 2-3-2004, am. Ord. passed 6-3-2014], REQUIREMENTS AND REGULATIONS.

§ 91.20 DEFINITIONS — ANIMAL UNDER RESTRAINT: "An animal secured by a leash or lead, or under the control of a responsible person and obedient to that person's commands, or confined within a vehicle or within the real property limits of its owner and/or its custodian."

§ 91.40 ANIMALS RUNNING AT LARGE. It shall be unlawful for the owner of any animal to allow the animal to be at large in Garrett County, meaning to be off the premises or property of the owner, unless under restraint, except dogs accompanied by the owner or trainer and being used or trained for hunting being followed by person on foot, horseback or in vehicles.
(A) An animal found at large with or without a valid license tag shall, except as provided above, be impounded by the Animal Control Warden and taken to the Animal Control Shelter and there confined in a humane manner for a period of not less than 3 days unless sooner claimed and redeemed by its owner. The Animal Control Officer shall use whatever humane means necessary to catch the animal and is relieved from any liability from harm or injury to the animal at the end of 3 days as indicated above, unclaimed animals shall be deemed abandoned and become the property of the Board of County Commissioners of Garrett County and shall be disposed of in the manner prescribed by the Animal Control Officer and approved by the Board of County Commissioners of Garrett County.
(B) The owner of any dog or cat against which a complaint has been filed that does not have a current valid license shall be assessed a fine of not less than $25 in addition to any other cost, including the purchase of a current valid license.
(Ord. —, passed 2-3-2004)

§ 91.42 CONFINEMENT OF FIERCE, DANGEROUS OR VICIOUS ANIMALS. (A) Every fierce, dangerous or vicious dog, including an animal that has a history of biting a human, shall be confined by the owner within a building or secure enclosure. The animal shall not be taken out of the building or secure enclosure unless securely muzzled.

§ 91.44(D)(2) Schedule of fines: First offense $50; Second offense $100; All other offenses $250.

§ 91.99 PENALTY: see § 91.44 civil fines schedule.

[Hosted on amlegal (codelibrary.amlegal.com); jurisdiction slug "garrettcounty"; doc slug "garrettco_md"; § 91.40 stable section ID "0-0-0-1773"; container article "0-0-0-1772"; chapter root "0-0-0-1". Ord. originally passed 2-3-2004; amended 6-3-2014 (§ 91.44 procedural). Fetched verbatim via Playwright 2026-05-22. Cross-checked vs Garrett County Sheriff's Office Animal Control Ordinance page (sheriff.garrettcountymd.gov/animal-control-shelter/animal-control-ordinance).]$body$
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Garrett County, MD Code of Ordinances%§ 91.40%'
);

-- ─── 3. Per-beach BPS row — on_leash for Little Brown Lake ──────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT 2646649, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       $verb$Garrett County Code § 91.40: "It shall be unlawful for the owner of any animal to allow the animal to be at large in Garrett County, meaning to be off the premises or property of the owner, unless under restraint..." § 91.20 ANIMAL UNDER RESTRAINT: "An animal secured by a leash or lead, or under the control of a responsible person and obedient to that person's commands, or confined within a vehicle or within the real property limits of its owner and/or its custodian." (Ord. passed 2-3-2004.) Countywide territorial restraint rule — applies to beaches by default.$verb$,
       ps.source_url,
       'Little Brown Lake — inland recreational lake in Garrett County mountain region (no coastline; westernmost MD, Allegheny Plateau). Countywide § 91.40 baseline applies; no operator overlay identified. Note: § 91.20 definition includes a voice-control alternative ("under the control of a responsible person and obedient to that person''s commands") — classified on_leash here per the dominant lay reading of "under restraint"; voice control is a permissible alternative under the ordinance text but not promoted to off_leash_voice_control in the consensus, per Kent County precedent for the same definitional language.',
       NULL
FROM public.policy_source ps
WHERE ps.citation LIKE 'Garrett County, MD Code of Ordinances%§ 91.40%'
ON CONFLICT (beach_fid, policy_source_id, section, COALESCE(region_name, '__default__'::text), rule) DO NOTHING;

-- ─── 4. EXCLUSIONS ──────────────────────────────────────────────────
--
-- fid 5773745  Deep Creek Lake State Park    — already COMAR-attached
-- fid 10346736 Herrington Manor State Park   — already COMAR-attached
--
-- These two beaches are administered by MD DNR State Park Service under
-- COMAR Title 08 Subtitle 01 (state-park rules) / Title 09 Subtitle 20
-- and were attached via the MD state park baseline migration. No bps
-- row is written here for them — state-park rule controls within park
-- boundaries (per layered-authority pattern: state regulation over
-- county code where they overlap on state-owned land).

commit;
