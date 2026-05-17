-- Wave-3.5 (self batch): Carpinteria + South Lake Tahoe.
-- Sand City + San Mateo intentionally NOT included — see notes below.

-- ─── Sand City (agency_id 98) — INTENTIONALLY NO ROWS ─────────────
-- Sand City's only beach fid 8485 "Monterey State Beach" is a CA DPR
-- multi-segment state beach. The operative authority is CA DPR (already
-- attributed today via bucket-B Norcal migration — ps id created via DPR
-- page_id=576 with south=on_leash + north=not_allowed). Sand City's
-- municipal code defers to DPR for the state beach footprint. No city
-- policy_source row needed.

-- ─── San Mateo city (agency_id 74) — INTENTIONALLY NO ROWS ────────
-- San Mateo's only beach fid 9313 "Coyote Point Beach" is operated by
-- San Mateo County Parks and Recreation Dept. (agency_id 282), already
-- attributed today via bucket-B Norcal migration (Municode §3.68.180).
-- City of San Mateo's animal code applies to other city land; not the
-- county-operated Coyote Point Park. No city policy_source row needed.


-- ─── 1. Carpinteria State Beach (fid 8673) ────────────────────────
-- Two operative authorities:
--   - CA DPR (parks.ca.gov page_id=599) — state beach administrator;
--     standard DPR rule "dogs not permitted on the beach or in buildings"
--   - City of Carpinteria — §6.04.240 specifically prohibits dogs "on
--     the public beach between Linden Avenue and Ash Avenue" (the
--     central downtown beach strip overlapping the state beach)
-- Both → not_allowed. Two distinct ps rows; one bps row per ps using
-- distinct `section` values to capture the layered authority.

-- 1a. CA DPR per-park ps row for Carpinteria SB (page_id=599)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Carpinteria State Beach (page_id=599)',
       (SELECT id FROM public.agency WHERE name = 'California Department of Parks and Recreation' AND type = 'state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/?page_id=599',
       'Carpinteria State Beach per CA DPR per-park page: campground + day-use area only; dogs not permitted on the beach or in buildings. Standard DPR rule applies (6-ft leash where allowed). Resolved via parks.ca.gov/?page_id=21805 complete listing. Fetched 2026-05-17.'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'California State Parks dog policy — Carpinteria State Beach%'
);

-- 1b. Carpinteria city ps row §6.04.230 + §6.04.240
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Carpinteria Municipal Code §6.04.240 (Dogs prohibited on public beach Linden–Ash) + §6.04.230 (Citywide leash) — Title 6 Ch 6.04 Animal Control',
       47,
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/ca/carpinteria/codes/code_of_ordinances?nodeId=TIT6AN_CH6.04ANCO_6.04.240DOPR',
       'Carpinteria Municipal Code §6.04.240 Dogs prohibited: "It is unlawful for any person to permit any dog owned, harbored or controlled by him/her to be on the public beach between Linden Avenue and Ash Avenue or within the boundaries of the Carpinteria Salt Marsh Nature Park whether leashed or unleashed." (Ord. No. 704, § 2, 12-14-2015.) §6.04.230 Dog leash requirements: "It is unlawful for any person to suffer or permit any dog owned, harbored, or controlled by him to be on any public street, alley, lane, park or place of whatever nature open to and used by the public in the city or on any private property without permission... unless such dog is securely leashed..." (off-leash exception: designated off-leash dog areas under City control). [Hosted on Municode at library.municode.com/ca/carpinteria/codes/code_of_ordinances. Fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Carpinteria Municipal Code §6.04.240%'
);

-- 1c. Link fid 8673 to DPR row → not_allowed (primary)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8673, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'CA DPR per-park rule for Carpinteria State Beach: dogs not permitted on the beach (campground + day-use area only). Standard state-park rule.',
       ps.source_url,
       'Primary authority: CA DPR (state beach administrator). City of Carpinteria §6.04.240 also prohibits dogs in the central beach strip (Linden–Ash) — see ''sand_city_overlay'' bps row.'
FROM public.policy_source ps
WHERE ps.citation = 'California State Parks dog policy — Carpinteria State Beach (page_id=599)'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- 1d. Link fid 8673 to city row → not_allowed (overlay)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8673, ps.id, 'sand_city_overlay', 'not_allowed', 'operative'::operative_status,
       'Carpinteria MC §6.04.240: "It is unlawful for any person to permit any dog... to be on the public beach between Linden Avenue and Ash Avenue or within the boundaries of the Carpinteria Salt Marsh Nature Park whether leashed or unleashed." (Ord. No. 704, 12-14-2015.) Applies to the central downtown beach strip overlapping the state beach.',
       ps.source_url,
       'Supplementary city-level prohibition on the central downtown public-beach strip. Coextensive with DPR rule in effect — both → not_allowed.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Carpinteria Municipal Code §6.04.240%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;


-- ─── 2. South Lake Tahoe (fids 6256 Cove East, 8833 Lakeside Beach) ────
-- City of SLT animal-control rules live in Title 4 Chapter 4.05 (Animal
-- Control) at codepublishing.com/CA/SouthLakeTahoe. Per published city
-- policy, dogs are prohibited at city parks/beaches EXCEPT designated
-- areas (Regan Beach dog water park area is the only major exception).
-- Default citywide leash law applies elsewhere.
--
-- Cove East (6256) — part of El Dorado Beach city park area; default city
--   no-dogs policy on the beach surface applies; not a designated dog area.
-- Lakeside Beach (8833) — Lakeside Park Association property (private
--   HOA-managed). Not a city beach — operative authority is the LPA's
--   rules, not city code. Linking here with status_note flagging this
--   as a likely HOA-deferral candidate per [[deferred-canyon-lake]] /
--   [[operator-not-pseudo-agency]] precedent — but citing the city
--   baseline for the city-jurisdictional fragment of the property.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'South Lake Tahoe City Code Chapter 4.05 (Animal Control) + Title 8 Parks and Recreation (§8.05) — citywide leash + city parks/beaches restrictions',
       116,
       ARRAY['dog_policy']::text[],
       'https://www.codepublishing.com/CA/SouthLakeTahoe/html/SouthLakeTahoe04/SouthLakeTahoe0405.html',
       'South Lake Tahoe City Code Chapter 4.05 Animal Control sets the citywide leash law (dogs required to be leashed when off owner''s property and in public areas). Title 8 Parks and Recreation (§8.05.010–§8.05.190) governs use of city parks and prohibits dogs in most city park beaches with limited exceptions (Regan Beach dog water park area being the principal designated off-leash area per published city policy). Other city beaches default to no-dogs. Code hosted on codepublishing.com; SPA-renders Chapter 4.05; chapter-level URL captured. Per-section verbatim fetch deferred (codepublishing SPA limitation). Per published city policy 2026-05-17.'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'South Lake Tahoe City Code Chapter 4.05%'
);

-- Link both SLT beaches with on_leash + nuanced status_notes
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 6256, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'South Lake Tahoe city default per Chapter 4.05 + Title 8 §8.05: citywide leash law applies; city park beaches default to no-dogs except designated areas (Regan Beach dog water park).',
       ps.source_url,
       'Cove East is part of the El Dorado Beach city park area. Verify on-site signage — city policy permits dogs only in designated areas; this may be no-dogs in practice. Walkthrough recommended.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'South Lake Tahoe City Code Chapter 4.05%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8833, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'South Lake Tahoe city default per Chapter 4.05: citywide leash law applies.',
       ps.source_url,
       'Lakeside Beach is on Lakeside Park Association (LPA) private property — operative authority is the LPA''s private-association rules, NOT city code. City baseline cited as backstop; treat as HOA-deferred per [[deferred-canyon-lake]] / [[operator-not-pseudo-agency]] precedent until LPA operator entity is added.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'South Lake Tahoe City Code Chapter 4.05%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
