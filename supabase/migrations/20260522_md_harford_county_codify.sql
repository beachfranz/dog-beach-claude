-- MD Harford County codify — Ch 64 Art III (Animal Control) + Ch 185 (Parks)
--
-- ─── Sources ─────────────────────────────────────────────────────────
--
-- Harford County Code of Public Local Laws, Part II General Legislation:
--
-- (1) Chapter 64 Animals, Article I General Provisions [§ 64-1
--     Definitions] — establishes "DOG AT LARGE" definition that anchors
--     the operative restraint rule.
--       § 64-1 Definitions:                      https://ecode360.com/9370562
--
-- (2) Chapter 64 Animals, Article III Animal Control [§ 64-7 Dogs at
--     large; § 64-8 Impoundment generally; § 64-9 Redemption; § 64-10
--     Impoundment fees] — operative countywide leash/at-large rule.
--       § 64-7 Dogs at large (Art III root):     https://ecode360.com/9370590
--     Chapter 64 root:                            https://ecode360.com/9370561
--     TOC (custid):                               https://ecode360.com/HA0904
--
-- (3) Chapter 185 Parks and Recreation [adopted by Bill No. 75-7;
--     formerly Ch. 15 Art. I of the 1978 Code], § 185-6 Animals —
--     reinforces the at-large prohibition on County parkland.
--       Ch 185 root (§ 185-6 listed in TOC):     https://ecode360.com/9373618
--
-- (All sections fetched verbatim via Playwright 2026-05-22; ecode360
-- custid HA0904.)
--
-- ─── Operative rules — verbatim ──────────────────────────────────────
--
-- § 64-1 DOG AT LARGE (definition, verbatim):
--   "Any dog will be deemed to be 'at large' when it is upon the
--   property of a person other than the owner or within the traveled
--   portion of any public road and not leashed or under the control of
--   the owner and obedient to the owners command."
--
-- § 64-7 Dogs at large (verbatim, full text):
--   "No owner of any dog shall permit it to be at large."
--
-- § 64-8 Impoundment generally [Amended by Bill No. 15-011] establishes
-- impoundment procedures for any dog "found at large" (96 hours
-- unlicensed; 192 hours licensed; owner-redemption mechanism).
--
-- § 185-6 Animals (verbatim, full text):
--   "No person shall:
--    A. Pursue, catch, molest or kill any wildlife.
--    B. Disturb any nest, den or burrow of any animal or fowl.
--    C. Cause or permit any dog, cat or other domesticated animal to
--       run at large or to create a nuisance."
-- § 185-2 Applicability: "The provisions of this chapter shall apply in
-- and upon all department parklands and facilities within the county."
--
-- ─── Rule classification ─────────────────────────────────────────────
--
-- Per [[feedback_citywide_leash_inference]]: territorial leash/at-large
-- rules apply to beaches by default. Harford County § 64-7 +
-- § 64-1 definition together make "at large" the operative concept —
-- a dog is "at large" whenever off its owner's property AND not
-- "leashed or under the control of the owner and obedient to the
-- owner's command." This is the same hybrid restraint pattern seen in
-- Cecil County § 142-20 and Kent County § 64-5: leash OR demonstrably
-- obedient voice control are both forms of "restraint." § 185-6(C)
-- reinforces the same rule on County parkland.
--
-- Consensus classification = on_leash (the dominant lay reading of the
-- "leashed or under the control of the owner and obedient to commands"
-- standard, consistent with sibling MD county migrations). Voice
-- control is an affirmative defense, not a baseline permission;
-- coding as off_leash_voice_control would overstate the codified
-- permission. status_note documents the voice-control alternative.
--
-- ─── Per-beach mapping (1 scoreable Harford County MD beach) ─────────
--
-- Belcamp Beach (fid 14277) — Bush River shoreline in the Belcamp/
-- Riverside unincorporated community near Aberdeen Proving Ground (NW
-- of APG, not on APG land per coord 39.466 N / -76.235 W). cpad_unit_id
-- IS NULL → no federal/state agency overrides apply. The Belcamp /
-- Riverside community sits on a private development (The Shores at
-- Water's Edge / Riverside Community Association) but Bush River
-- shoreline access in this area is a mix of private waterfront and
-- community-association common areas.
--
-- Per [[feedback_citywide_leash_inference]] + the Cecil/Kent precedent:
-- § 64-7 binds the dog owner whenever the dog is off the owner's own
-- property (the "dog at large" definition keys on "property of a person
-- other than the owner"). On any community waterfront, the individual
-- dog owner does not own the beach parcel, so the at-large prohibition
-- attaches regardless of public vs. HOA ownership. If the beach turns
-- out to be (a) inside a County Department of Parks & Recreation unit,
-- § 185-6(C) reinforces the same rule. If it turns out to be a fully
-- private HOA common area, an operator-posted overlay (HOA rules) may
-- layer on top later via the Operate pipeline as an
-- operator_posted_policy row.
--
-- INCLUDED: fid 14277 → on_leash via § 64-7 countywide baseline.
--
-- ─── Judgment calls ──────────────────────────────────────────────────
--
-- (a) Voice-control alternative in § 64-1 DOG AT LARGE definition.
--     Coded as on_leash (consistent with Cecil and Kent sibling
--     migrations). The plain operative § 64-7 ("No owner of any dog
--     shall permit it to be at large") plus impoundment enforcement
--     makes on_leash the practical rule.
--
-- (b) ONE policy_source row covers Ch 64 Art III + Ch 185 Art § 185-6
--     because both authorities yield the same operative rule on the
--     single in-scope beach and both are Harford County legislative
--     code. source_url deep-links to § 64-7 (Art III root) as the
--     primary operative section; full_text includes the § 185-6
--     reinforcement quote and citation.
--
-- (c) No Town of Belcamp — Belcamp is unincorporated. No municipal
--     overlay. Harford County has only one incorporated municipality
--     (Town of Bel Air) and no incorporated coastal towns; no town
--     code preempts § 64-7 for Belcamp Beach.

-- ─── 1. Provision Harford County agency (canonical bare-name) ───────

INSERT INTO public.agency (name, type, web_url)
SELECT 'Harford County', 'county'::agency_type, 'https://www.harfordcountymd.gov/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency WHERE name='Harford County' AND type='county'::agency_type
);

-- ─── 2. Insert codified-ordinance policy_source ─────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Harford County Code of Public Local Laws, Chapter 64 Animals, Article III Animal Control, § 64-7 (Dogs at large) + § 64-1 Definitions (DOG AT LARGE); reinforced by Chapter 185 Parks and Recreation, § 185-6(C) (Animals)',
       (SELECT id FROM public.agency WHERE name='Harford County' AND type='county'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/9370590',
       $body$Harford County Code of Public Local Laws, Part II General Legislation, Chapter 64 Animals, Article III Animal Control. § 64-7 Dogs at large: "No owner of any dog shall permit it to be at large." --- § 64-8 Impoundment generally [Amended by Bill No. 15-011]. A. Dogs not wearing a current Harford County license tag found at large shall be taken to the Humane Society of the County or such other appropriate place as may be designated by the Department and confined in a humane manner for a period of not less than 96 hours, excluding Sundays and County holidays. Thereafter, the dogs become the property of the Humane Society and shall be disposed of by return to their owner, adoption or euthanasia. B. Dogs wearing a current Harford County license tag and found at large shall be taken to the Humane Society of the County or such other appropriate place as may be designated by the Department and confined in a humane manner for a period of not less than 192 hours, excluding Sundays and holidays. The owner of the licensed dog shall be notified of the impoundment by the Sheriff by certified mail to the address of the owner indicated upon the license application. The notice shall be sent within 72 hours of the impoundment and shall give the owner 120 hours from the date of the notice in which to redeem the dog. Thereafter, the dog becomes the property of the Humane Society and shall be disposed of by return to its owner, adoption or euthanasia. C. When dogs are found at large, such dogs may be returned to the owner, who shall also be served a violation notice. --- § 64-9 Redemption of impounded dogs. A. The owner shall be entitled to resume possession of any impounded dog upon compliance with the license provisions of this chapter and the payment of the fees set forth in Chapter 157, § 157-24, of the Harford County Code, as amended. --- § 64-10 Impoundment fees [Amended by Bill Nos. 92-31; 15-011]. Any animal impounded under this Article may be reclaimed as herein provided upon payment to the County, by the owner, of the sum of $20. An additional care charge of $10 for each calendar day or portion thereof during which the animal was impounded shall be added to the impoundment fee. --- Chapter 64 Article I General Provisions, § 64-1 Definitions. DOG AT LARGE — "Any dog will be deemed to be 'at large' when it is upon the property of a person other than the owner or within the traveled portion of any public road and not leashed or under the control of the owner and obedient to the owners command." OWNER — "Any person owning, keeping, harboring or acting as custodian of an animal. Any animal owned by a minor shall be deemed to be owned, for the purposes of this section, by the parents or guardians with whom the minor resides." --- Chapter 185 Parks and Recreation [HISTORY: Adopted by the Harford County Council by Bill No. 75-7; formerly included as Ch. 15, Art. I of the 1978 Code]. § 185-2 Applicability: "The provisions of this chapter shall apply in and upon all department parklands and facilities within the county." § 185-6 Animals: "No person shall: A. Pursue, catch, molest or kill any wildlife. B. Disturb any nest, den or burrow of any animal or fowl. C. Cause or permit any dog, cat or other domesticated animal to run at large or to create a nuisance." [Hosted on ecode360 (General Code) at ecode360.com/9370590 (§ 64-7 / Art III root) + ecode360.com/9370562 (§ 64-1 Definitions / Art I root) + ecode360.com/9373618 (Ch 185 Parks and Recreation root, § 185-6 deep-linked from TOC); chapter root ecode360.com/9370561 (Ch 64); custid HA0904. Fetched verbatim via Playwright 2026-05-22.]$body$
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Harford County Code of Public Local Laws, Chapter 64%§ 64-7%'
);

-- ─── 3. Per-beach BPS rows — on_leash baseline for Belcamp Beach ────
--
-- Single in-scope beach: Belcamp Beach (fid 14277) on Bush River in the
-- Belcamp/Riverside unincorporated community.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       $body$Harford County Code § 64-7: "No owner of any dog shall permit it to be at large." § 64-1 defines DOG AT LARGE as "Any dog ... upon the property of a person other than the owner or within the traveled portion of any public road and not leashed or under the control of the owner and obedient to the owners command." Countywide territorial at-large prohibition — applies to beaches by default. Reinforced for County parkland by § 185-6(C): "No person shall ... Cause or permit any dog, cat or other domesticated animal to run at large or to create a nuisance." Voice control by an owner with a dog "obedient to the owner's command" is a statutorily recognized alternative form of restraint; on_leash is the practical baseline rule but voice control is an affirmative defense.$body$,
       ps.source_url,
       v.note,
       NULL
FROM (values
        (14277, 'Belcamp Beach — Bush River shoreline in the unincorporated Belcamp/Riverside community (Harford County). Coord -76.235 / 39.466 places the beach west of Aberdeen Proving Ground on community waterfront, not on federal land (cpad_unit_id IS NULL). § 64-7 binds the dog owner whenever the dog is off the owner''s own property; on any community / HOA waterfront the individual dog owner does not own the beach parcel, so the at-large prohibition applies. If a future operator-posted HOA rule for the Riverside Community Association or The Shores at Water''s Edge is identified, it would layer on top via the Operate pipeline as an operator_posted_policy row. No Town overlay (Belcamp is unincorporated; Harford County has no incorporated coastal towns).')
     ) AS v(fid, note)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Harford County Code of Public Local Laws, Chapter 64%§ 64-7%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule) DO NOTHING;
