-- MD Kent County codify — Ch 64 Art I (Domestic Animals) + Betterton Beach overlay
--
-- ─── Sources ─────────────────────────────────────────────────────────
--
-- (1) Kent County Code of Public Local Laws, Part II General Legislation,
--     Chapter 64 Animals, Article I Domestic Animals [adopted 3-16-2010
--     by Bill No. 1-2010].
--     Section deep-links:
--       § 64-1 Definitions:                       https://ecode360.com/11474304
--       § 64-5 Restraint; public nuisance:        https://ecode360.com/11474330
--       § 64-10.3 Enforcement / penalties:        https://ecode360.com/11474381
--     Chapter root:                               https://ecode360.com/11474296
--     TOC:                                        https://ecode360.com/KE1414
--     (Hosted on ecode360, General Code; custid KE1414. Fetched verbatim
--     via Playwright 2026-05-22. Note: § 64-5 chapter-root URL is used for
--     the ps row; per-section opaque IDs verified at chapter level.)
--
-- (2) Kent County Parks & Recreation Department — Betterton Beach
--     "Know Before You Go" operator posting (operative beach-specific
--     overlay for Betterton Beach only).
--     URL: https://kentparksandrec.org/pool-beach-regulations
--     PDF: https://kentparksandrec.org/images/pdf/Betterton_Beach_-_know_before_you_go_2.pdf
--     (Verified verbatim via WebFetch 2026-05-22.)
--
-- (3) Town of Betterton Ordinance 2013-06 (Animal Control) — confirms
--     that Chapter 64 of the Kent County Code is adopted by reference
--     within the corporate limits of Betterton. The town does NOT have
--     an independent leash/restraint rule; § 64-5 controls inside town
--     limits via Town Code § 6-301. Layered authority is therefore
--     county-as-baseline + operator-posted Betterton Beach overlay.
--     (No separate town-issued bps row needed — same rule, same source.)
--
-- ─── Operative rules — verbatim ──────────────────────────────────────
--
-- § 64-5 Restraint; public nuisance.
--   A. All dogs shall be kept under restraint.
--   B. Public nuisance.
--     (1) Every owner shall exercise proper care and control of their
--     animal(s) to prevent it (them) from becoming a public nuisance.
--     (2) Upon being declared a public nuisance, any unaltered animal
--     found running at large three times will be required to be spayed
--     or neutered within seven days of the third offense...
--   C. Every female animal in heat shall be confined or supervised...
--   D. Every dangerous animal shall be confined by the owner within a
--     building or secure enclosure and shall be securely muzzled or caged
--     whenever off the premises of its owner.
--
-- § 64-1 RESTRAINT (definition, verbatim):
--   "An animal is under restraint when it is secured by a leash or lead
--   and/or under the control of a responsible person and obedient to
--   that person's commands or within the real property limits of its
--   owner."
--
-- § 64-1 ANIMAL AT LARGE (definition, verbatim):
--   "Any animal not under the restraint of a person capable of
--   controlling the animal and off the premises of the owner."
--
-- § 64-10.3 Enforcement:
--   "Violations of this article shall be deemed a code county
--   infraction... shall be fined not more than $400."
--
-- ─── Rule classification ─────────────────────────────────────────────
--
-- § 64-5(A) "All dogs shall be kept under restraint" + § 64-1 definition
-- (leash/lead OR voice control + obedient OR on owner's property) is a
-- countywide territorial restraint requirement that applies to any dog
-- off the owner's own real property. Per [[feedback_citywide_leash_inference]],
-- territorial restraint rules apply to beaches by default.
--
-- The § 64-1 RESTRAINT definition explicitly allows voice control by a
-- "responsible person and obedient to that person's commands" as an
-- alternative to physical leash. This is broader than a strict leash
-- requirement; consensus classification = on_leash (the dominant lay
-- reading) with status_note documenting the voice-control alternative.
-- Codifying this as off_leash_voice_control would overstate permission;
-- on_leash with the carve-out narrative is the honest call.
--
-- ─── Betterton Beach overlay (operator-posted, seasonal not_allowed) ──
--
-- The Kent County Parks & Recreation "Know Before You Go" posting for
-- Betterton Beach states verbatim:
--   "NO PETS PERMITTED ON BEACH (SAND) DURING PUBLIC BEACH PERMIT
--   SEASON (MAY-SEPTEMBER)"
--   "ALL PETS MUST BE LEASHED" (outside permit season / off-sand)
--
-- The kentparksandrec.org pool-and-beach regulations page additionally
-- summarizes: "Dogs are not allowed on the beach" under the Betterton
-- Beach section. Both sources confirm a SEASONAL (May–September)
-- prohibition on the SAND; outside permit season the county Ch 64
-- leash rule controls.
--
-- Per playbook §6 + [[feedback_dogpark_rules_are_operator_posted]] +
-- [[feedback_operate_two_table_finding]]: this is an operator-posted
-- overlay that REPLACES the county leash rule on the sand during permit
-- season. Encoding:
--   - Base bps row: not_allowed (operator overlay; ps subtype
--     operator_posted_policy attached to Kent County Parks & Recreation
--     as a Department of Kent County). The seasonal narrowing
--     (May–September) is captured in evidence_verbatim + status_note;
--     temporal extractor (§9.5) will lift the (May 1 – September 30)
--     window into beach_policy_source_temporal so that off-season the
--     bdp reverts to the county Ch 64 on_leash baseline.
--   - Country code Ch 64 row STILL applies to Betterton Beach
--     (year-round leash requirement outside the sand-during-permit-
--     season carve-out, and applies to the bathhouse / parking / paved
--     pavilion areas at all times). Both bps rows coexist; layered
--     authority resolution via consensus engine + temporal layer.
--
-- ─── Per-beach mapping (5 scoreable Kent County MD beaches) ─────────
--
-- Excluded from this migration:
--   - Wickes Beach (fid 12951) — already covered by Eastern Neck NWR
--     USFWS rule (ps id 319). The NWR rule governs Wickes Beach (which
--     sits on Eastern Neck Island within the NWR boundary); county Ch 64
--     would be a redundant baseline. NOT touched here.
--
-- Per-beach disposition:
--   - 12947  Mitchell Bluff               → on_leash (county baseline)
--   - 14611  Betterton Beach              → not_allowed (Betterton overlay; seasonal May–Sep on sand)
--   - 14611  Betterton Beach              → on_leash (county Ch 64 baseline, second row)
--   - 5417044 Tolchester Estates Beach    → on_leash (county baseline)
--   - 9105527 Gregg Neck Beach            → on_leash (county baseline)
--   - 14056191 Quaker Neck Public Landing → on_leash (county baseline; Kent County public landing)
--
-- Notes on community/HOA character:
--   - Tolchester Estates: the Tolchester Community Association is a
--     defunct/dormant 1946 stock corporation per local press; no
--     enforceable HOA rules identified. § 64-5 countywide rule binds
--     dog owners on community waterfront.
--   - Mitchell Bluff: unincorporated waterfront near Rock Hall;
--     county code applies.
--   - Gregg Neck Beach: unincorporated upper Chesapeake / Sassafras
--     waterfront; county code applies.
--   - Quaker Neck Public Landing: Kent County public boat landing
--     (kentcounty.com/government/...public_landings); county code
--     applies + this is Kent County-owned property.
--
-- ─── Judgment calls ─────────────────────────────────────────────────
--
-- (a) Voice-control alternative in § 64-1 RESTRAINT definition. Coded
--     as on_leash (with status_note noting the voice-control reading)
--     rather than off_leash_voice_control. The plain operative § 64-5
--     ("kept under restraint") plus public-nuisance enforcement makes
--     on_leash the practical rule; voice-control is a defense, not a
--     baseline permission. Consistent with how Cecil County (sibling
--     migration 20260522_md_cecil_county_codify) handled its similar
--     "restraint or effective control" definition.
--
-- (b) Betterton Beach earlier "success_auto_commit" cascade artifact
--     left NO bps row in the DB. Verified via:
--       SELECT * FROM beach_policy_source WHERE beach_fid=14611;
--     → zero rows. Either the auto-commit was emitted but the bps INSERT
--     was filtered (likely because Step 6 had no codified text matching
--     fid 14611) OR the pipeline marked the jurisdiction as resolved
--     before per-beach materialization. This migration emits the
--     Betterton rows EXPLICITLY (both baseline + overlay) to close the
--     gap. No DELETE / cleanup needed — the prior cascade left no row.
--
-- (c) Layered authority for Betterton: TWO bps rows (overlay
--     not_allowed + baseline on_leash) coexist. The consensus engine
--     ranks the operator overlay above county code per existing
--     source-class ordering. The seasonal carve-out via the temporal
--     extractor will downgrade the not_allowed to on_leash off-season
--     (Oct–Apr).
--
-- (d) Town of Betterton has its own town code (Ord 2013-06) but it
--     ADOPTS Kent County Ch 64 by reference (§ 6-301: "The Kent County
--     Animal Control Ordinance contained in Chapter 64...is hereby
--     adopted...and made part of this Code by reference"). No
--     independent town-issued bps row needed; the ps cites Kent County
--     as issuing agency with town-limits applicability noted in
--     full_text.

-- ─── 1. Provision Kent County + Kent County Parks & Rec agencies ────

INSERT INTO public.agency (name, type, web_url)
SELECT 'Kent County', 'county'::agency_type, 'https://www.kentcounty.com/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency WHERE name='Kent County' AND type='county'::agency_type
);

INSERT INTO public.agency (name, type, web_url, parent_agency_id)
SELECT 'Kent County Parks and Recreation', 'special_district'::agency_type,
       'https://kentparksandrec.org/',
       (SELECT id FROM public.agency WHERE name='Kent County' AND type='county'::agency_type)
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency
   WHERE name='Kent County Parks and Recreation' AND type='special_district'::agency_type
);

-- ─── 2a. Insert codified-ordinance policy_source (Kent County Ch 64) ─

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Kent County Code of Public Local Laws, Chapter 64 Animals, Article I Domestic Animals, § 64-5 (Restraint; public nuisance) + § 64-1 Definitions (RESTRAINT, ANIMAL AT LARGE)',
       (SELECT id FROM public.agency WHERE name='Kent County' AND type='county'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/11474330',
       $body$Kent County Code of Public Local Laws, Part II General Legislation, Chapter 64 Animals [Adopted by the County Commissioners of Kent County as indicated in article histories], Article I Domestic Animals [adopted 3-16-2010 by Bill No. 1-2010]. § 64-5 Restraint; public nuisance. A. All dogs shall be kept under restraint. B. Public nuisance. (1) Every owner shall exercise proper care and control of their animal(s) to prevent it (them) from becoming a public nuisance. (2) Upon being declared a public nuisance, any unaltered animal found running at large three times will be required to be spayed or neutered within seven days of the third offense. A time deferment or waiver may be granted for pets too old or sick to be altered. A fine will be levied against the owner in an amount to cover the cost of surgery. Upon the owner submitting evidence of the surgery having been completed, the fine will be nolle prosequi. C. Every female animal in heat shall be confined or supervised in such a manner that such female animal cannot come into contact with a male animal except for planned breeding. D. Every dangerous animal shall be confined by the owner within a building or secure enclosure and shall be securely muzzled or caged whenever off the premises of its owner. --- § 64-1 Definitions. RESTRAINT — An animal is under restraint when it is secured by a leash or lead and/or under the control of a responsible person and obedient to that person's commands or within the real property limits of its owner. ANIMAL AT LARGE — Any animal not under the restraint of a person capable of controlling the animal and off the premises of the owner. --- § 64-10.3 Enforcement; violations and penalties. B. Violations of this article shall be deemed a code county infraction. Any person, firm, corporation or other legal entity found to have committed a code county infraction by violating any provision of this article or any amendment thereto shall be fined not more than $400. Each and every day during which such violation occurs or continues may be deemed a separate offense. --- Applicability within incorporated municipalities: Town of Betterton Ordinance 2013-06 § 6-301(a) adopts Chapter 64 by reference: "The Kent County Animal Control Ordinance contained in Chapter 64 of the Kent County Code of Ordinances and as may be amended from time to time, is hereby adopted...and made part of this Code by reference...Chapter 64 shall be applicable and enforceable by Kent County, Kent County's designee, or the Mayor and Council's designee within the boundaries of the Town of Betterton." Betterton is the only incorporated municipality in Kent County with a public beach. [Hosted on ecode360 (General Code) at ecode360.com/11474296 (Ch 64 root) + ecode360.com/11474330 (§ 64-5 deep link); TOC at ecode360.com/KE1414; custid KE1414. Fetched verbatim via Playwright 2026-05-22.]$body$
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Kent County Code of Public Local Laws, Chapter 64%§ 64-5%'
);

-- ─── 2b. Insert Betterton Beach operator-posted overlay ps row ──────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'operator_posted_policy',
       'Kent County Parks & Recreation — Betterton Beach Regulations (Know Before You Go): seasonal prohibition on sand during public beach permit season (May–September)',
       (SELECT id FROM public.agency WHERE name='Kent County Parks and Recreation' AND type='special_district'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://kentparksandrec.org/pool-beach-regulations',
       $body$Kent County Parks & Recreation Department — Betterton Beach "Know Before You Go" operator posting (operative beach-specific overlay). Posted regulations: "NO PETS PERMITTED ON BEACH (SAND) DURING PUBLIC BEACH PERMIT SEASON (MAY-SEPTEMBER)." "ALL PETS MUST BE LEASHED." The pool-and-beach regulations page (kentparksandrec.org/pool-beach-regulations) additionally states under the Betterton Beach section: "Dogs are not allowed on the beach." Read together: dogs are PROHIBITED on the sand portion of Betterton Beach during the May–September permit season; outside the permit season (October–April) dogs are permitted on leash per Kent County Ch 64 § 64-5. Leash requirement applies year-round to non-sand areas of the park (bathhouse, picnic pavilion, parking, jetty path). --- Authority basis: Kent County Parks & Recreation Department promulgates park-specific rules under the County Park Regulations document (adopted by the Kent County Parks & Recreation Advisory Board 2-10-2020; amended 5-3-2021 and 9-6-2021). The General Provisions Rule 19 (Animals) of the County Park Regulations states: "Per Kent County Public Law (Section 64 - Domestic Animals), all dogs shall be kept under restraint. Animal owners shall be responsible for the removal of any excrement deposited by the animal. Violations will result in fines not to exceed $400." Per-park additional rules (such as the Betterton Beach seasonal sand prohibition) are published in the Parks Department "Know Before You Go" postings and on kentparksandrec.org. [Sources: kentparksandrec.org/pool-beach-regulations (primary) + kentparksandrec.org/images/pdf/Betterton_Beach_-_know_before_you_go_2.pdf (PDF posting); County Park Regulations PDF at kentparksandrec.org/images/County_Park_Regulations.pdf. Verified verbatim via WebFetch 2026-05-22.]$body$
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Kent County Parks & Recreation — Betterton Beach Regulations%'
);

-- ─── 3a. Per-beach BPS rows — county baseline on_leash (5 beaches) ──
--
-- All 5 scoreable Kent County beaches get the § 64-5 baseline. Betterton
-- (14611) ALSO gets the operator overlay below; both rows coexist.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       $body$Kent County Code § 64-5(A): "All dogs shall be kept under restraint." § 64-1 defines RESTRAINT as "secured by a leash or lead and/or under the control of a responsible person and obedient to that person's commands or within the real property limits of its owner." Countywide territorial restraint rule — applies to beaches by default. Voice control by a responsible person obedient to commands is recognized as an alternative form of restraint; on_leash is the practical baseline rule but voice control is an affirmative defense.$body$,
       ps.source_url,
       v.note,
       NULL
FROM (values
        (12947,    'Mitchell Bluff — unincorporated Kent County waterfront near Rock Hall. § 64-5 applies; no municipal or operator overlay identified.'),
        (14611,    'Betterton Beach — within the incorporated Town of Betterton; Town Ord 2013-06 adopts Kent County Ch 64 by reference. The county leash rule controls non-sand park areas (bathhouse, pavilion, parking, jetty path) year-round. The sand portion has a seasonal not_allowed overlay (May–September) via Kent County Parks & Recreation posting — see separate bps row.'),
        (5417044,  'Tolchester Estates Beach — community waterfront in unincorporated Kent County. Tolchester Community Association is a dormant 1946 stock corporation per local press; no enforceable HOA leash overlay identified. § 64-5 binds dog owners off their own property.'),
        (9105527,  'Gregg Neck Beach — unincorporated upper Chesapeake / Sassafras waterfront. § 64-5 applies.'),
        (14056191, 'Quaker Neck Public Landing — Kent County-owned public boat landing on the Chester River near Chestertown. § 64-5 applies; this is County property where Parks & Recreation Rule 19 reinforces "all dogs shall be kept under restraint."')
     ) AS v(fid, note)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Kent County Code of Public Local Laws, Chapter 64%§ 64-5%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule) DO NOTHING;

-- ─── 3b. Betterton Beach (fid 14611) operator overlay — not_allowed ──
--
-- Operator overlay on the SAND during permit season (May–September).
-- Temporal extractor (§9.5) will lift the May 1 – September 30 window
-- into beach_policy_source_temporal so the consensus engine downgrades
-- to the on_leash baseline outside permit season.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT 14611, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       $body$Kent County Parks & Recreation Betterton Beach posting: "NO PETS PERMITTED ON BEACH (SAND) DURING PUBLIC BEACH PERMIT SEASON (MAY-SEPTEMBER)." Reinforced on kentparksandrec.org/pool-beach-regulations under the Betterton Beach section: "Dogs are not allowed on the beach." Seasonal sand prohibition by operator (Kent County Parks & Rec); outside permit season (October–April) Kent County Ch 64 § 64-5 on_leash baseline controls. Year-round, "ALL PETS MUST BE LEASHED" in non-sand park areas (bathhouse, pavilion, parking, jetty).$body$,
       ps.source_url,
       'Seasonal SAND-only prohibition: May 1 – September 30. Off-season (Oct–Apr), dogs are permitted on the sand on leash per Kent County Ch 64 § 64-5. Run scripts/extract_temporal_from_policy_source.py on this ps row to lift the May–September window into beach_policy_source_temporal.',
       NULL
FROM public.policy_source ps
WHERE ps.citation LIKE 'Kent County Parks & Recreation — Betterton Beach Regulations%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule) DO NOTHING;
