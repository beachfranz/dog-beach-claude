-- MD Somerset County codify — Ordinance No. 924 (Somerset County Dog Control Ordinance)
--
-- Source: Somerset County, Maryland Dog Control Ordinance, Ordinance No. 924.
--   Repealed and re-enacted Ord. #444 (1990). Effective 15 July 2006; passed
--   by the Board of County Commissioners for Somerset County, Maryland.
-- URL (deep-linked PDF, the canonical hosted copy):
--   https://cms7files1.revize.com/somersetcountymd/document_center/Government/Ordinances/Dog%20Control%20Ordinance%20924.pdf
-- Linked from: https://www.somersetmd.us/services/dog_control.php
-- (Fetched verbatim via curl 2026-05-22; pypdf text extraction.)
--
-- Platform note: Somerset County MD is NOT on ecode360 / Municode / amlegal /
-- codepublishing. The County does not publish a chaptered web code; the
-- operative animal-control authority is the standalone PDF ordinance hosted
-- on the County's revize.com document center. Per [[feedback_url_resolution_field_guide]]
-- "PDF county-codes class" pattern: the deep-linked PDF is the page-level
-- detail document (deep-link satisfies [[feedback_page_level_over_agency_level]]).
-- Earlier pipeline cascade flagged Somerset as defer_stubborn because Phase A
-- platform probes (ecode360/Municode/etc.) all returned NONE; Step 6.8 web
-- search surfaced the County dog-control page → ordinance PDF directly.
--
-- ─── Operative rules (verbatim from Ord. 924) ────────────────────────
--
-- Article III, Section 1. DOGS AT LARGE PROHIBITED:
--   "It shall be unlawful for any person or owner to permit a dog to run at
--   large, regardless of whether the dog is licensed or unlicensed."
--
-- Article II, Section 1, Definition (3) AT LARGE/STRAY DOG(S):
--   "Any dog upon the property of a person other than the owner or within
--   the traveled portion of any public road and not leashed, or when used
--   for hunting, training, or field trials and not under the control of a
--   responsible person and obedient to that person's command."
--
-- Article II, Section 3. APPLICABILITY OF ORDINANCE:
--   "The provisions of this Ordinance shall apply throughout the County."
--
-- Article II, Section 24(B). NUISANCES — GENERAL:
--   "No owner of a dog shall allow it to soil, defile, defecate or commit
--   other nuisances upon any public property, recreation area, or private
--   property other than the owner's. The owner must take immediate steps
--   to eliminate any such nuisance, including feces caused by the dog, in
--   an appropriate and sanitary manner."
--
-- ─── Layered authority decision ─────────────────────────────────────
--
-- Per [[feedback_citywide_leash_inference]]: territorial leash/at-large rules
-- apply to beaches by default. Somerset County's Article III §1 is a
-- countywide at-large prohibition; the Article II §1(3) definition
-- explicitly defines "at large" to include any dog off the owner's own
-- property that is "not leashed" (or otherwise not under responsible-person
-- control via the limited hunting/training/field-trial carve-out). Combined,
-- the operative rule for any waterfront not on the dog owner's own parcel
-- is on_leash.
--
-- The hunting/training/field-trial carve-out does NOT apply to general
-- beach recreation — it is conditioned on active sporting use AND
-- responsible-person voice control. Default rule for beach visits: on_leash.
--
-- Subtype: municipal_code (county-level codified ordinance, equivalent to
-- the Cecil/Worcester County treatment 2026-05-22).
--
-- ─── Per-beach mapping (3 scoreable Somerset County MD beaches) ─────
--
-- Query: SELECT fid, name, cpad_unit_id FROM beaches_gold
--        WHERE state='MD' AND county_name='Somerset'
--          AND is_active AND scoring_tier IN ('daily','hourly')
--        ORDER BY fid;
-- Returns 4 rows; Crisfield Beach (fid 18941056) EXCLUDED here — it sits
-- inside Janes Island State Park and is already attached to COMAR via the
-- state-park baseline migration (20260522_md_state_park_baseline_v2.sql).
--
-- In scope (3 beaches, all on lower Eastern Shore Chesapeake / Tangier
-- Sound shoreline; cpad_unit_id IS NULL across the board → no federal/state
-- agency overrides):
--
--   fid 12948 — Mount Vernon Beach (community waterfront on the Wicomico
--               River / Monie Bay reach, near the village of Mount Vernon
--               in unincorporated Somerset County)
--   fid 12950 — Sound Shore (waterfront on Tangier Sound)
--   fid 12955 — Langford Sand (waterfront on Tangier Sound / Pocomoke
--               Sound, unincorporated Somerset County)
--
-- All three are unincorporated Somerset County waterfronts. Article II §3
-- ("shall apply throughout the County") + Article III §1 (at-large
-- prohibition) bind every dog owner walking a dog on any of these beaches
-- regardless of land-ownership classification.
--
-- ─── Agency provisioning ────────────────────────────────────────────
--
-- No "Somerset County" type=county agency row exists yet (verified 2026-05-22
-- via SELECT * FROM agency WHERE name ILIKE '%somerset%'). Create canonical
-- bare-name row before inserting the policy_source row.

-- ─── 1. Provision Somerset County agency (canonical bare-name) ──────

INSERT INTO public.agency (name, type, web_url)
SELECT 'Somerset County', 'county'::agency_type, 'https://www.somersetmd.us/'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency WHERE name='Somerset County' AND type='county'::agency_type
);

-- ─── 2. Insert codified-ordinance policy_source ─────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Somerset County, Maryland Dog Control Ordinance (Ord. No. 924) — Art III §1 (Dogs At Large Prohibited) + Art II §1(3) (AT LARGE definition requires leash off owner property) + Art II §24(B) (cleanup)',
       (SELECT id FROM public.agency WHERE name='Somerset County' AND type='county'::agency_type),
       ARRAY['dog_policy']::text[],
       'https://cms7files1.revize.com/somersetcountymd/document_center/Government/Ordinances/Dog%20Control%20Ordinance%20924.pdf',
       $body$Somerset County, Maryland — Ordinance No. 924, "An Ordinance to repeal Ordinance #444 [Dog Control] and re-enact the Dog Control Ordinance for Somerset County, Maryland." Effective 15 July 2006; approved and passed by the Board of County Commissioners for Somerset County, Maryland. Authorized under ART 24 Section 11-504 of the Annotated Code of Maryland and Subtitle 2-411 of The Code of Public Laws of Somerset County. ---
ARTICLE II - IN GENERAL, SECTION 1. DEFINITIONS. (3) AT LARGE/STRAY DOG(S) - Any dog upon the property of a person other than the owner or within the traveled portion of any public road and not leashed, or when used for hunting, training, or field trials and not under the control of a responsible person and obedient to that person's command. (8) PUBLIC NUISANCE - A dog shall be deemed a "Public Nuisance" when it is (A) a danger to any person or (B) when it engages in activities which disturb the peace and quiet of any neighborhood including but not limited to: excessive barking, whining, howling or chasing of vehicles, attacking other domestic animals or damaging of property. ---
ARTICLE II, SECTION 3. APPLICABILITY OF ORDINANCE. The provisions of this Ordinance shall apply throughout the County. ---
ARTICLE II, SECTION 24. NUISANCES - GENERAL. (A) It shall be unlawful for any owner to allow his/her dog to become a public nuisance, as defined in Article II, Section 1, Definition (8) herein. (B) No owner of a dog shall allow it to soil, defile, defecate or commit other nuisances upon any public property, recreation area, or private property other than the owner's. The owner must take immediate steps to eliminate any such nuisance, including feces caused by the dog, in an appropriate and sanitary manner. ---
ARTICLE III - ANIMAL WELFARE, SECTION 1. DOGS AT LARGE PROHIBITED. It shall be unlawful for any person or owner to permit a dog to run at large, regardless of whether the dog is licensed or unlicensed. ---
ARTICLE V - PENALTIES AND FINES, GENERAL, SECTION 1. CIVIL PENALTIES. A. Any person who shall violate any provisions of this Ordinance shall be guilty of a civil violation and shall be subject to a fine not to exceed one thousand dollars ($1,000.00) per violation. Each day the violation continues shall constitute a separate violation. ---
[Hosted on Somerset County document center (revize.com CMS) at cms7files1.revize.com/somersetcountymd/document_center/Government/Ordinances/Dog%20Control%20Ordinance%20924.pdf; linked from somersetmd.us/services/dog_control.php. Somerset County MD is NOT on ecode360 / Municode / amlegal / codepublishing — the PDF ordinance is the canonical hosted authority. Fetched via curl + pypdf 2026-05-22.]$body$
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Somerset County, Maryland Dog Control Ordinance (Ord. No. 924)%'
);

-- ─── 3. Per-beach BPS rows — on_leash baseline for all 3 ────────────
--
-- All 3 Somerset County scoreable beaches (excluding Crisfield Beach inside
-- Janes Island SP, handled by COMAR baseline). Countywide at-large
-- prohibition + "not leashed" definition yields rule=on_leash.
-- Section='sand', operative.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note, region_name)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       $body$Somerset County Ord. 924, Art III §1: "It shall be unlawful for any person or owner to permit a dog to run at large, regardless of whether the dog is licensed or unlicensed." Art II §1(3) defines "AT LARGE" to include any dog off the owner's own property that is "not leashed" (the only exceptions are active hunting/training/field-trial use under responsible-person voice control — N/A to general beach recreation). Art II §3: "The provisions of this Ordinance shall apply throughout the County." Applies to all unincorporated Somerset County waterfronts on lower Eastern Shore Chesapeake / Tangier Sound.$body$,
       ps.source_url,
       v.note,
       NULL
FROM (values
        (12948, $note$Mount Vernon Beach — community waterfront on the Wicomico River / Monie Bay reach near unincorporated village of Mount Vernon. Countywide Ord. 924 Art III §1 + Art II §1(3) at-large/leash binding apply.$note$),
        (12950, $note$Sound Shore — Tangier Sound shoreline, unincorporated Somerset County. Countywide Ord. 924 Art III §1 + Art II §1(3) at-large/leash binding apply.$note$),
        (12955, $note$Langford Sand — Tangier Sound / Pocomoke Sound shoreline, unincorporated Somerset County. Countywide Ord. 924 Art III §1 + Art II §1(3) at-large/leash binding apply.$note$)
     ) AS v(fid, note)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Somerset County, Maryland Dog Control Ordinance (Ord. No. 924)%'
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__'::text)), rule) DO NOTHING;
