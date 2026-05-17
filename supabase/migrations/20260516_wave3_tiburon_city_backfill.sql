-- Wave 3: City of Tiburon — 3 beaches (all on Angel Island State Park),
-- dogs not allowed per CA State Parks policy.
--
-- Source: California Department of Parks and Recreation — Angel Island
-- State Park dog policy page.
-- URL: https://www.parks.ca.gov/?page_id=468
--
-- Operative rule (verbatim, parks.ca.gov Angel Island page):
--   "Dogs are NOT allowed on the island, except service animals."
--   Emotional support animals are not covered under the ADA and are not
--   exempt from this prohibition.
--
-- Governance note: The 3 Tiburon-city-polygon beaches in beaches_gold
-- (Perles Beach, Quarry Beach, Sand Springs Beach) are all on Angel
-- Island, which is a CA State Park. The Town of Tiburon Municipal Code
-- (Chapter 20 ANIMALS, hosted on Municode at library.municode.com/ca/
-- tiburon/codes/code_of_ordinances) governs town parks and streets but
-- not state-park land. The operative dog rule on Angel Island is the
-- CA State Parks blanket prohibition.
--
-- 3 Tiburon-city-polygon beaches, all unlinked, all on Angel Island:
--   - fid 8722 Perles Beach        → not_allowed (Angel Island SP)
--   - fid 6131 Quarry Beach        → not_allowed (Angel Island SP)
--   - fid 8778 Sand Springs Beach  → not_allowed (Angel Island SP)
--
-- All 3 → not_allowed.

-- ─── 1. Insert agency_administrative_policy policy_source ────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'California State Parks — Angel Island State Park: dogs not allowed (except service animals)',
       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://www.parks.ca.gov/?page_id=468',
       'California Department of Parks and Recreation — Angel Island State Park visitor information page. Operative rule (verbatim): "Dogs are NOT allowed on the island, except service animals." Emotional support animals are not covered under the Americans with Disabilities Act and are therefore not exempt from this prohibition. Reason given: for the safety of visitors, wildlife, and the park. Angel Island is the entirety of the island in San Francisco Bay; all named beaches (Perles, Quarry, Sand Springs, China Cove, etc.) fall under this blanket prohibition. Town of Tiburon Municipal Code Chapter 20 ANIMALS (hosted on Municode at library.municode.com/ca/tiburon/codes/code_of_ordinances) governs town parks and streets but does NOT govern state-park land on the island. [Fetched verbatim 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE source_url='https://www.parks.ca.gov/?page_id=468' AND issuing_agency_id=(SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department')
);

-- ─── 2. Link all 3 Tiburon (Angel Island) beaches → not_allowed ──

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'CA State Parks — Angel Island SP: "Dogs are NOT allowed on the island, except service animals." Blanket prohibition across the entire island, including all beaches.',
       ps.source_url,
       'Beach is on Angel Island State Park; CA DPR governs (not Town of Tiburon, despite city-polygon overlap). Service animals only.'
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('California','CA') AND j.place_type='C1' AND j.name='Tiburon'
CROSS JOIN public.policy_source ps
WHERE g.state='CA' AND g.is_active=true
  AND ps.source_url='https://www.parks.ca.gov/?page_id=468'
  AND ps.issuing_agency_id=(SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department')
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source bps WHERE bps.beach_fid=g.fid
  )
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
