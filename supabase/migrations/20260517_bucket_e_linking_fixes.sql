-- Bucket E linking fixes — 5 CA beaches whose primary dog_policy
-- authority has policy_source rows but the specific beach wasn't
-- linked during Wave 2.
--
-- 1. fid 561 Klamath Beach (Yurok Tribe) -> existing ps id 52
-- 2. fid 3671 Leo Carrillo State Beach (CA DPR) -> existing ps id 70
-- 3. fid 6433 Royal Palms State Beach (LA City precedence-1, but
--    actually LA County-operated state beach per the 1976+ state-
--    county agreements) -> LA County Title 17 (existing ps id 6)
-- 4. fid 8394 Oceano Dunes State Rec Area -> NEW DPR ps + link
-- 5. fid 9005 Rio Del Mar State Beach -> NEW DPR ps + link

-- ─── Create new DPR policy_source rows for Oceano Dunes + Rio Del Mar ──

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Oceano Dunes State Vehicular Recreation Area',
       234,
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/?page_id=406',
       'California Department of Parks and Recreation — Oceano Dunes State Vehicular Recreation Area (SVRA). Dogs are permitted on-leash (max 6 ft) in the camping and day-use areas; off-leash use is permitted within the open recreation area. This is one of the most permissive CA DPR units for dogs. Standard CA DPR exclusions apply (designated swimming beaches if posted; snowy plover seasonal closures Mar 15–Sep 15 in posted nesting zones). [Source: parks.ca.gov page_id=406. Fetched 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'California State Parks dog policy — Oceano Dunes%'
);

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'California State Parks dog policy — Rio Del Mar State Beach',
       234,
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/?page_id=545',
       'California Department of Parks and Recreation — Rio Del Mar State Beach (Santa Cruz County). Default CA DPR coastal state-beach rule applies: dogs not allowed on the beach sand; allowed in adjacent paved/parking areas on 6-ft leash. [Source: parks.ca.gov page_id=545. Fetched 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'California State Parks dog policy — Rio Del Mar%'
);

-- ─── Link the 5 bucket-E beaches ──────────────────────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
VALUES
  -- Klamath Beach → Yurok Tribal Code
  (561, 52, 'sand', 'on_leash', 'operative'::operative_status,
   'Yurok Tribal Code — animal access provisions apply on tribal land.',
   NULL,
   'Tribal jurisdiction; precise section text not captured. Walkthrough recommended.'),
  -- Leo Carrillo State Beach → existing DPR ps 70
  (3671, 70, 'sand', 'not_allowed', 'operative'::operative_status,
   'CA DPR Leo Carrillo State Park dog policy: dogs not allowed on the beach sand; allowed in camping and trail areas on 6-ft leash.',
   'https://www.parks.ca.gov/?page_id=616',
   NULL),
  -- Royal Palms State Beach → LA County Title 17
  (6433, 6, 'sand', 'not_allowed', 'operative'::operative_status,
   'LA County Code Title 17 (Beaches and Harbors): no animals on the sand of LA County beaches. Royal Palms State Beach is state-owned but LA-County-operated under state agreement.',
   NULL,
   'Governance: beach_agency precedence-1 is LA City (likely incorrect — Royal Palms is in San Pedro near Palos Verdes Estates, not LA City proper, and is operated by LA County under state agreement). Flagged for governance-resolver review.'),
  -- Oceano Dunes SRA → new DPR ps
  (8394, (SELECT id FROM public.policy_source WHERE citation LIKE 'California State Parks dog policy — Oceano Dunes%'),
   'sand', 'on_leash', 'operative'::operative_status,
   'CA DPR Oceano Dunes SVRA: dogs permitted in camping/day-use areas on 6-ft leash; off-leash permitted within open recreation area. Snowy plover seasonal closures may apply.',
   'https://www.parks.ca.gov/?page_id=406',
   'Oceano Dunes is one of the most permissive CA DPR units for dogs; sub-area rules vary (open recreation vs day-use vs nesting closure zones).'),
  -- Rio Del Mar State Beach → new DPR ps
  (9005, (SELECT id FROM public.policy_source WHERE citation LIKE 'California State Parks dog policy — Rio Del Mar%'),
   'sand', 'not_allowed', 'operative'::operative_status,
   'CA DPR Rio Del Mar State Beach: default coastal state-beach rule — dogs not allowed on beach sand; allowed in adjacent paved/parking on 6-ft leash.',
   'https://www.parks.ca.gov/?page_id=545',
   NULL)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
