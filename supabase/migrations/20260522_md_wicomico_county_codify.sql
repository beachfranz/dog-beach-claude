-- MD Wicomico County codify — Chapter 133 Animal Control (Bill 2018-09)
--
-- Source: Wicomico County, Maryland Code of Public Local Laws, Chapter 133
--   Animal Control. Adopted 10-16-2018 by Legislative Bill No. 2018-09 (in
--   effect 1-19-2019), repealing the prior dog laws and replacing them with
--   the current animal-welfare regulations. Amended 6-6-2023 by Bill No.
--   2023-06 (§ 133-2 definitions and § 133-18 nuisances).
-- Custid (ecode360):           WI0638
-- Jurisdiction root:           https://ecode360.com/WI0638
-- Chapter 133 root:            https://ecode360.com/10170030
-- Operative URLs (deep-linked, per [[feedback_page_level_over_agency_level]]):
--   § 133-14 (Animals at large prohibited):                https://ecode360.com/10170126
--   § 133-15 (Animals on public recreation areas — leash): https://ecode360.com/10170128
--   § 133-18 (Nuisances — public-property cleanup):        https://ecode360.com/10170131
--   § 133-2  (Definitions of "at large"/"owner"/"animal"): https://ecode360.com/10170035
-- (Fetched verbatim via Playwright + ecode360 General Code platform 2026-05-22.)
--
-- ─── Operative rules (verbatim from Bill 2018-09 / Bill 2023-06) ────
--
-- § 133-14 Animals at large prohibited.
--   A. It is unlawful to allow an animal to run at large.
--   B. Definition of "at large." "At large" means when an unleashed animal is:
--      (1) On the property of a person other than the owner; or
--      (2) Within the traveled portion of a public road.
--   C. Exceptions. An animal will not be considered at large if the animal is:
--      (1) Under the control of a responsible person;
--      (2) Securely confined in a vehicle;
--      (3) On the owner's real property; or
--      (4) Hunting, herding, or tracking under the control of the owner and
--          on land that the owner is permitted to be on.
--
-- § 133-15 Animals on school grounds and public recreation areas.
--   B. At any part of a public recreation area, an animal must be on a leash
--      no longer than six feet or confined or otherwise restrained in a
--      vehicle.
--   C. The Board or other agency that manages school grounds or public
--      recreation areas may waive or suspend the provisions in Subsections A
--      and B of this section, and in the event of conflict with those
--      subsections, the rules or regulations adopted by such agency shall
--      govern; provided, however, that animals may not be prohibited in
--      public parking areas unless the prohibition is prominently and clearly
--      posted in such areas in both English and Spanish or the specific area
--      is unavailable for public parking at the particular time.
--   D. This section does not apply to an animal being used for the following
--      purposes: (1) As a service animal, if appropriately restrained.
--      (2) Law enforcement or public safety, if under the control or
--      supervision of a person duly authorized to perform the particular
--      function.
--
-- § 133-18 Nuisances. [Amended 6-6-2023 by Bill No. 2023-06]
--   A. It is unlawful to allow an animal to become a public nuisance,
--      including to allow: ... (3) An animal to repeatedly soil, defile,
--      defecate, or commit another nuisance upon public property, recreation
--      areas, or private property other than the owner's property; ...
--
-- ─── Layered authority decision ─────────────────────────────────────
--
-- Per [[feedback_citywide_leash_inference]]: territorial leash rules apply
-- to beaches by default. Wicomico Ch 133 is unusually direct — § 133-15(B)
-- is an EXPLICIT positive 6-foot leash mandate keyed to "public recreation
-- area," not merely a derivative at-large prohibition. Both in-scope beaches
-- are unincorporated public-access waterfronts → squarely within § 133-15(B).
-- § 133-14 at-large prohibition provides the parallel countywide backstop
-- (unleashed animal off owner's property = at large).
--
-- Cleanup obligation flows from § 133-18(A)(3) — "soil, defile, defecate ...
-- upon public property, recreation areas" — encoded via status_note (the
-- operative rule for beach surface is on_leash; cleanup is a co-rule on the
-- same authority that downstream consensus will surface via prose).
--
-- The § 133-15(C) waiver power belongs to "the Board or other agency that
-- manages ... public recreation areas." Neither in-scope beach has an
-- identified managing operator that has published a contrary rule (cpad_unit_id
-- NULL on both; no operator row attached). Default rule stands.
--
-- Subtype: municipal_code (county-level codified ordinance, mirrors
-- Worcester/Cecil/Somerset/Dorchester County treatment 2026-05-22).
--
-- ─── Per-beach mapping (2 in-scope Wicomico County MD beaches) ──────
--
-- Source query (per user dispatch — fids explicitly named in agent prompt):
--   SELECT fid, name, cpad_unit_id FROM beaches_gold
--    WHERE state='MD' AND county_name='Wicomico' AND is_active
--    ORDER BY fid;
-- Returns 2 rows (both active; cpad_unit_id IS NULL → no federal/state
-- agency overrides; both currently scoring_tier='none' — agent task accepts
-- the user-supplied fids as the in-scope set).
--
-- In scope (2 beaches, both lower Eastern Shore Wicomico River shoreline):
--
--   fid 12960   — Sandy Hill Beach  (~-75.854,38.355 — Wicomico River near
--                 Mardela / Sandy Hill area, unincorporated Wicomico County)
--   fid 1376688 — Cove Road Beach   (~-75.902,38.288 — Wicomico River near
--                 Whitehaven, unincorporated Wicomico County)
--
-- Both are unincorporated Wicomico County public waterfronts; Ch 133
-- (§ 133-14 + § 133-15(B)) binds every dog owner walking a dog on either.
--
-- Confused-name guard (per dispatch note): NOT Charles County's "Wicomico
-- Beach" on the Potomac/Wicomico River near Cobb Neck — that is in Charles
-- County and is covered by 20260522_md_charles_county_codify.sql, not here.
--
-- ─── Agency provisioning ────────────────────────────────────────────
--
-- No "Wicomico County" type=county agency row exists yet (verified 2026-05-22
-- via SELECT * FROM agency WHERE name ILIKE '%wicomico%'). Create canonical
-- bare-name row before inserting the policy_source row.

-- ─── 1. Provision Wicomico County agency (canonical bare-name) ──────

INSERT INTO public.agency (name, type, web_url)
SELECT 'Wicomico County', 'county'::agency_type, 'https://www.wicomicocounty.org/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency WHERE name='Wicomico County' AND type='county'::agency_type
);

-- ─── 2. Insert codified-ordinance policy_source ─────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Wicomico County, MD Code Ch 133 (Animal Control) §§ 133-14 (Animals at large prohibited) + 133-15(B) (6-ft leash on public recreation areas) + 133-18(A)(3) (nuisance cleanup on public property)',
       (SELECT id FROM public.agency WHERE name='Wicomico County' AND type='county'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/10170128',
       $body$Wicomico County, Maryland Code of Public Local Laws, Chapter 133 Animal Control. Adopted by the County Council of Wicomico County 10-16-2018 by Legislative Bill No. 2018-09; effective 1-19-2019 (repealed and replaced the prior dog-control chapter). § 133-2 definitions and § 133-18 nuisances amended 6-6-2023 by Bill No. 2023-06. ---
§ 133-2 DEFINITIONS (selected). ANIMAL — An animate being (other than a human being) capable of voluntary movement. OWNER — A person who owns or is the custodian of an animal. An animal owned by a minor will be considered to be owned by the parents or guardians with whom the minor resides. OWNER'S REAL PROPERTY — Property of which the animal's owner, as herein defined, is the actual owner or tenant or present as the guest or licensee of the owner, but does not include a public right-of-way or common area of a condominium, apartment complex or townhouse development or premises when the animal's owner is present as a customer or business or commercial invitee, including hotels and motels. SERVICE ANIMAL — Any dog that is individually trained to do work or perform tasks for the benefit of an individual with a disability ... ---
§ 133-14 ANIMALS AT LARGE PROHIBITED. A. It is unlawful to allow an animal to run at large. B. Definition of "at large." "At large" means when an unleashed animal is: (1) On the property of a person other than the owner; or (2) Within the traveled portion of a public road. C. Exceptions. An animal will not be considered at large if the animal is: (1) Under the control of a responsible person; (2) Securely confined in a vehicle; (3) On the owner's real property; or (4) Hunting, herding, or tracking under the control of the owner and on land that the owner is permitted to be on. ---
§ 133-15 ANIMALS ON SCHOOL GROUNDS AND PUBLIC RECREATION AREAS. B. At any part of a public recreation area, an animal must be on a leash no longer than six feet or confined or otherwise restrained in a vehicle. C. The Board or other agency that manages school grounds or public recreation areas may waive or suspend the provisions in Subsections A and B of this section, and in the event of conflict with those subsections, the rules or regulations adopted by such agency shall govern; provided, however, that animals may not be prohibited in public parking areas unless the prohibition is prominently and clearly posted in such areas in both English and Spanish or the specific area is unavailable for public parking at the particular time. D. This section does not apply to an animal being used for the following purposes: (1) As a service animal, if appropriately restrained. (2) Law enforcement or public safety, if under the control or supervision of a person duly authorized to perform the particular function. ---
§ 133-18 NUISANCES. [Amended 6-6-2023 by Bill No. 2023-06] A. It is unlawful to allow an animal to become a public nuisance, including to allow: (1) An animal to be a danger to a person; (2) An animal to disturb the peace and quiet of a neighborhood with excessive barking, whining, howling, crowing, chasing vehicles, attacking other domestic animals, or damaging property; (3) An animal to repeatedly soil, defile, defecate, or commit another nuisance upon public property, recreation areas, or private property other than the owner's property; (4) An animal to enter private property without the property owner's permission; (5) A female dog or cat in heat outside of a building or other proper enclosure while not in the control or supervision of a person; or (6) An animal to cause an unsanitary, dangerous, or offensive condition because of the size or number of animals in a single location or because a facility is not appropriately or properly maintained. ---
§ 133-37 CIVIL PENALTY (Article XII). Any person who violates a provision of this chapter is guilty of a civil infraction and may be fined up to $1,000 per violation; each day a violation continues is a separate violation. ---
[Hosted on ecode360 General Code platform under custid WI0638; chapter root https://ecode360.com/10170030. Wicomico County, MD operates animal control via the Wicomico County Sheriff's Office (Animal Control Authority per § 133-2). Fetched via Playwright 2026-05-22.]$body$
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Wicomico County, MD Code Ch 133 (Animal Control)%'
);

-- ─── 3. Per-beach BPS rows — on_leash for both ──────────────────────
--
-- Both Wicomico County in-scope beaches are unincorporated public waterfronts
-- on the Wicomico River. § 133-15(B) explicit 6-ft leash mandate ("at any
-- part of a public recreation area, an animal must be on a leash no longer
-- than six feet") + § 133-14 countywide at-large prohibition jointly bind
-- → rule=on_leash. Section='sand', operative.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       $body$Wicomico County Code Ch 133, § 133-15(B): "At any part of a public recreation area, an animal must be on a leash no longer than six feet or confined or otherwise restrained in a vehicle." § 133-14(A): "It is unlawful to allow an animal to run at large." § 133-14(B) defines "at large" to include any unleashed animal on the property of a person other than the owner. § 133-18(A)(3) further prohibits allowing an animal to "repeatedly soil, defile, defecate, or commit another nuisance upon public property, recreation areas, or private property other than the owner's property" — owners must clean up. Applies to unincorporated Wicomico County public waterfronts on the Wicomico River.$body$,
       ps.source_url,
       v.note,
       NULL
FROM (values
        (12960,   $note$Sandy Hill Beach — public-access waterfront on the Wicomico River in unincorporated Wicomico County (Sandy Hill / Mardela vicinity). Countywide Ch 133 § 133-15(B) 6-ft leash mandate on public recreation areas + § 133-14 at-large prohibition apply. Owner-cleanup obligation per § 133-18(A)(3). Not in Charles County (distinct from the Potomac-tributary "Wicomico Beach" near Cobb Neck).$note$),
        (1376688, $note$Cove Road Beach — public-access waterfront on the Wicomico River in unincorporated Wicomico County (Whitehaven vicinity, off Cove Rd). Countywide Ch 133 § 133-15(B) 6-ft leash mandate on public recreation areas + § 133-14 at-large prohibition apply. Owner-cleanup obligation per § 133-18(A)(3).$note$)
     ) AS v(fid, note)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Wicomico County, MD Code Ch 133 (Animal Control)%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule) DO NOTHING;
