-- MD federal lands codify — per jurisdiction_policy_source_playbook.md (Federal codify path §5)
-- Date: 2026-05-22
--
-- Scope: Maryland federal-land beaches (NPS/FWS/NOAA). Discovered via
-- beaches_gold × pad_us_units ST_Contains() + manual containment audit.
-- DOD-managed beaches (Naval Academy / Aberdeen / Patuxent NAS) excluded —
-- not publicly accessible to dog-beach use cases.
-- NRCS / NOAA-Marine polygons excluded (easement / marine-only — no land authority).
--
-- ============================================================================
-- IN-SCOPE BEACHES (containment-verified):
-- ============================================================================
--   fid 14485  Sycamore Beach          — inside George Washington Memorial Pkwy (NPS, dist 0)
--   fid 14949  Purplehorse Beach       — inside George Washington Memorial Pkwy (NPS, dist 0)
--   fid 12951  Wickes Beach            — adjacent to Eastern Neck NWR (FWS, dist 8.4m, tideline edge)
--
-- ============================================================================
-- DEFERS (per playbook defer-rubric — defer > fabricate):
-- ============================================================================
--   Assateague Island National Seashore — Ocean City beaches (fids 16500596,
--   19712160, 14910035) PAD-US-tagged at match_strength=1 (dist 685m+); they
--   are downtown Ocean City beaches NORTH of the Assateague Island bridge,
--   not actually inside the NS boundary. The one truly inside-Assateague beach
--   (Little Egging Beach fid 12946) has scoring_tier='none' — out of scope.
--   DEFER until Assateague gets a scoreable beach. Assateague pet rule (NPS):
--   leashed on MD side; prohibited on VA side. Source:
--   https://www.nps.gov/asis/planyourvisit/pets.htm
--
--   Blackwater National Wildlife Refuge — No in-scope MD beach is contained
--   in the Blackwater polygon. The two PAD-US matches (Cherry Beach fid
--   19519452 in Seaford, Garrett Island Beach fid 14021 in Cecil County) are
--   GEOGRAPHIC DATA ANOMALIES — both beaches sit 80+ miles from Cambridge MD
--   where the actual refuge is located. The PAD-US Blackwater multipolygon
--   has stale geometry extending implausibly far north (ymax=39.57).
--   DEFER all "Blackwater"-tagged matches as suspect; revisit if PAD-US is
--   refreshed. Refuge rule (for the record): leashed pets allowed at refuge
--   trails per https://www.fws.gov/refuge/blackwater/visit-us/rules-policies
--
--   Patuxent Research Refuge — Naval Air Station Patuxent River (FED/DOD,
--   fid 3672326 Cedar Cove Community Beach) is unrelated to the FWS-managed
--   Patuxent Research Refuge in Laurel/Beltsville (inland, not coastal). No
--   in-scope MD beaches inside Patuxent Research Refuge polygons. No codify
--   needed.
--
--   Mallows Bay-Potomac National Marine Sanctuary (NOAA) — Clifton Beach
--   (fid 12944) is inside the sanctuary's MARINE polygon. NOAA NMS regulates
--   marine resources under 15 CFR Part 922 Subpart S but does not address
--   pets; landside pet rules at Mallows Bay Park come from Charles County MD
--   (the parcel owner/operator). DEFER federal codify; route via county-level
--   operate channel.
--
--   C&O Canal NHP overlay — Sycamore + Purplehorse are also adjacent to
--   C&O Canal NHP (dist 269m) but contained in GWMP at dist 0. GWMP authority
--   is operative; C&O Canal overlay deferred (separate compendium; same NPS
--   baseline applies via 36 CFR §2.15 ps id=1).
--
-- ============================================================================
-- LAYERED AUTHORITY:
-- ============================================================================
-- NPS baseline (36 CFR §2.15) already covers Sycamore + Purplehorse via
-- existing ps id=1. This migration ADDS the GWMP Superintendent's Compendium
-- as a per-unit override that imposes a STRICTER rule (water access prohibited).
-- Per playbook §6: layered authority writes ONE bps row per (beach, section,
-- override). GWMP Compendium row gets section='sand' with rule='not_allowed'
-- (Potomac River entry prohibition is the operative rule for the shoreline
-- beach). The 36 CFR §2.15 baseline rule applies to LAND areas of the parkway
-- (trails / picnic areas) where dogs ARE allowed leashed — but those areas
-- are not the "sand" section we're modeling.
--
-- Per playbook tenet #5: bps INSERT auto-fires trigger cascade. No two-pass
-- refire (data-only migration, not function-body change).
--
-- ============================================================================
BEGIN;

-- ============================================================================
-- 1. GWMP Superintendent's Compendium (NPS, agency id=307)
-- ============================================================================
-- Subtype: superintendents_compendium (per Fort Funston walkthrough taxonomy)
-- Affects: Sycamore Beach (14485), Purplehorse Beach (14949)
-- Rule:    not_allowed  (Potomac River entry prohibition trumps general §2.15)

INSERT INTO public.policy_source (
    subtype, citation, issuing_agency_id, scope, source_url, full_text,
    effective_date, last_verified, metadata
)
SELECT 'superintendents_compendium',
       'George Washington Memorial Parkway Superintendent''s Compendium (approved Dec 4, 2025) — Pets / Potomac River entry prohibition',
       307,
       ARRAY['dog_policy']::text[],
       'https://www.nps.gov/gwmp/learn/management/superintendent-s-compendium.htm',
       'GWMP Superintendent''s Compendium (NPS unit GWMP, approved Dec 4, 2025)
================================================================================

OPERATIVE RULES FOR PETS (verbatim extracts from 2025 GWMP Compendium):

36 CFR §2.15 — Pets — Designated areas where pets are prohibited:
"Possessing a pet in a public building, public transportation vehicle, or
location designated as a swimming beach" applies to:
  - Arlington House, The Robert E. Lee Memorial
  - Clara Barton House

Potomac River entry prohibition (compendium-specific addition):
"Pets are prohibited from entering the Potomac River from within GWMP managed
areas."

Pet waste:
"Solid pet waste must be collected and disposed of by placing in a trash
receptacle or removing it from the park."

Service-animal exemption:
"Guide dogs for visually impaired persons and hearing ear dogs for hearing-
impaired persons are excluded from the building/facility prohibitions."

NOTE: The compendium does not establish a specific leash-length standard; the
36 CFR §2.15 baseline (6-ft leash on NPS lands) applies on parkway trails and
picnic grounds. The river-entry prohibition is the operative rule for shoreline
beaches inside GWMP managed areas (Sycamore Beach fid 14485, Purplehorse Beach
fid 14949).

Citation: 2025 GWMP Superintendent''s Compendium (NPS Doc Ref:
2025-GWMP-Compendium-converted-from-pdf-12-5-25.docx)
URL: https://www.nps.gov/gwmp/learn/management/superintendent-s-compendium.htm
Compendium file: https://www.nps.gov/gwmp/learn/management/upload/2025-GWMP-Compendium-converted-from-pdf-12-5-25.docx

Layered authority: this compendium operationalizes 36 CFR §2.15 (existing
ps id=1) by adding a unit-specific water-entry prohibition. Per playbook
"Layered authority": the compendium is the operative override for shoreline
beaches; §2.15 baseline applies to land areas.

[Hosted on nps.gov. Fetched via WebFetch 2026-05-22.]',
       '2025-12-04'::date,
       NOW(),
       jsonb_build_object('codify_source', 'md_federal_codify_2026_05_22',
                          'nps_unit_code', 'gwmp',
                          'compendium_approval_date', '2025-12-04',
                          'compendium_file_url', 'https://www.nps.gov/gwmp/learn/management/upload/2025-GWMP-Compendium-converted-from-pdf-12-5-25.docx')
WHERE NOT EXISTS (
    SELECT 1 FROM public.policy_source
    WHERE source_url = 'https://www.nps.gov/gwmp/learn/management/superintendent-s-compendium.htm'
);

INSERT INTO public.beach_policy_source (
    beach_fid, policy_source_id, section, rule, operative_status,
    evidence_verbatim, evidence_url, status_note, extracted_at, last_verified
)
SELECT v.fid, ps.id, 'sand', 'not_allowed',
       'operative'::operative_status,
       'GWMP Compendium (2025): "Pets are prohibited from entering the Potomac River from within GWMP managed areas." Shoreline beach inside GWMP managed area; river-entry prohibition is the operative rule for sand/shoreline section. NPS 36 CFR §2.15 baseline (leash on land) remains applicable to adjacent parkway trails / picnic grounds.',
       ps.source_url,
       'GWMP Compendium overrides 36 CFR §2.15 for shoreline beaches by prohibiting river entry. Land-area rule (leashed) still applies to parkway trails outside the sand section.',
       NOW(), NOW()
FROM (VALUES (14485::bigint)  /* Sycamore Beach    — GWMP contained, dist=0 */,
             (14949::bigint)  /* Purplehorse Beach — GWMP contained, dist=0 */) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.source_url = 'https://www.nps.gov/gwmp/learn/management/superintendent-s-compendium.htm'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ============================================================================
-- 2. Eastern Neck National Wildlife Refuge — Pet Policy (USFWS, agency id=308)
-- ============================================================================
-- Subtype: agency_administrative_policy (USFWS public-use rules page, per
--          Oregon Islands NWR precedent ps id=275)
-- Affects: Wickes Beach (12951) — 8.4m off polygon edge at the shoreline
-- Rule:    on_leash  (6-foot leash)

INSERT INTO public.policy_source (
    subtype, citation, issuing_agency_id, scope, source_url, full_text,
    effective_date, last_verified, metadata
)
SELECT 'agency_administrative_policy',
       'Eastern Neck National Wildlife Refuge — Pet Policy (USFWS; 6-foot handheld leash required)',
       308,
       ARRAY['dog_policy']::text[],
       'https://www.fws.gov/refuge/eastern-neck/visit-us/rules-policies',
       'USFWS Eastern Neck National Wildlife Refuge — Pet Policy
================================================================================

OPERATIVE RULE: Pets are permitted on the refuge but must be kept on a handheld
leash no longer than 6 feet.

Verbatim from the USFWS refuge rules & policies page:
"Pet must be on a handheld leash no longer than 6 feet."

Refuge context: 2,094-acre island refuge at the mouth of the Chester River,
Kent County, MD (established 1962). The refuge contains trails, observation
areas, and shoreline along the Chester River and Chesapeake Bay. The standard
USFWS public-use rule applies refuge-wide (50 CFR Part 26 framework + this
refuge-specific public-use policy).

Citation: Eastern Neck NWR Rules & Policies page (USFWS)
URL: https://www.fws.gov/refuge/eastern-neck/visit-us/rules-policies

Note on spatial match: Wickes Beach (fid 12951) sits ~8.4m off the refuge
polygon edge — this is the tideline boundary (the refuge follows the mean-
high-water line; the beach centroid is just offshore on the bay side). The
shoreline beach is within Eastern Neck NWR jurisdiction per USFWS public-use
rules covering the refuge perimeter.

[Hosted on fws.gov. Fetched via WebFetch 2026-05-22.]',
       NULL::date,
       NOW(),
       jsonb_build_object('codify_source', 'md_federal_codify_2026_05_22',
                          'fws_refuge_slug', 'eastern-neck',
                          'leash_length_ft', '6',
                          'spatial_note', 'tideline edge match — beach 8.4m off polygon')
WHERE NOT EXISTS (
    SELECT 1 FROM public.policy_source
    WHERE source_url = 'https://www.fws.gov/refuge/eastern-neck/visit-us/rules-policies'
);

INSERT INTO public.beach_policy_source (
    beach_fid, policy_source_id, section, rule, operative_status,
    evidence_verbatim, evidence_url, status_note, extracted_at, last_verified
)
SELECT v.fid, ps.id, 'sand', 'on_leash',
       'operative'::operative_status,
       'Eastern Neck NWR Rules & Policies: "Pet must be on a handheld leash no longer than 6 feet." Beach centroid 8.4m off refuge polygon edge (tideline boundary); shoreline within refuge jurisdiction.',
       ps.source_url,
       'Tideline-edge spatial match. Refuge perimeter rule applies.',
       NOW(), NOW()
FROM (VALUES (12951::bigint)  /* Wickes Beach — Eastern Neck NWR, dist=8.4m */) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.source_url = 'https://www.fws.gov/refuge/eastern-neck/visit-us/rules-policies'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

COMMIT;
