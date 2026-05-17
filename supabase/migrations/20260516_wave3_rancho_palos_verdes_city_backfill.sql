-- Wave 3: City of Rancho Palos Verdes — 5 beaches, blanket no-dogs.
--
-- Source: City of Rancho Palos Verdes FAQ + Park Use Rules:
--   - https://www.rpvca.gov/Faq.aspx?QID=149  (Are dogs allowed on the
--     city's beaches and parks?)
--   - https://rpvca.gov/DocumentCenter/View/16293/FINAL-Group-Picnic-Rules-Updated-06-01-2023
--   - https://www.rpvca.gov/522/Dog-Behavior
--
-- Operative rules per City sources:
--   - Dogs are PROHIBITED on Rancho Palos Verdes beaches
--   - Dogs in city parks/trails must be on a leash ≤6 feet
--   - Adopts LA County Code Title 10 §10.32.010 by reference for the
--     leash rule (max fine $250)
--   - Residential cap: no more than 3 dogs and 3 cats per residential
--     lot (RPV Municipal Code §6.04.040)
--
-- 5 RPV-city-polygon beaches, all unlinked:
--   - fid 5941 East Beach
--   - fid 6349 Golden Cove Beach          (RPV as precedence-1)
--   - fid 6199 Rancho Palos Verdes Beach
--   - fid 8806 Terranea Cove Beach        (RPV as precedence-1)
--   - fid 8266 Upper Beach
--
-- All 5 linked → not_allowed.

-- ─── 1. Insert policy_source (agency_administrative_policy) ───────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'City of Rancho Palos Verdes — Dogs prohibited on beaches (city FAQ + Park Use Rules; leash law adopts LA County §10.32.010)',
       (SELECT id FROM public.agency WHERE name='Rancho Palos Verdes' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://www.rpvca.gov/Faq.aspx?QID=149',
       'City of Rancho Palos Verdes dog policy. Dogs are PROHIBITED on RPV beaches. Dogs in city parks and on designated trails must be kept on a leash not exceeding six feet in length. Leash requirement enforced per LA County Code Title 10 §10.32.010 (adopted by reference); maximum fine $250. RPV Municipal Code §6.04.040 caps household animals at 3 dogs + 3 cats per residential lot. Designated dog park: Rancho Caninos Dog Park (inland). [Source: rpvca.gov city FAQ (QID=149: dogs and beaches/parks) + Park Use Rules document + Dog Behavior page. Underlying RPV Municipal Code chapter for beach prohibition not directly cited on the FAQ; codified backstop hosted on Municode at library.municode.com/ca/rancho_palos_verdes/codes/code_of_ordinances per Phase A discovery. Fetched 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE source_url='https://www.rpvca.gov/Faq.aspx?QID=149'
);

-- ─── 2. Link all 5 RPV beaches → not_allowed ───────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'City of Rancho Palos Verdes: dogs are prohibited on RPV beaches per city policy (rpvca.gov FAQ). Citywide leash rule (≤6 ft, per LA County §10.32.010) applies elsewhere.',
       ps.source_url, NULL
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('California','CA') AND j.place_type='C1' AND j.name='Rancho Palos Verdes'
CROSS JOIN public.policy_source ps
WHERE g.state='CA' AND g.is_active=true
  AND ps.source_url='https://www.rpvca.gov/Faq.aspx?QID=149'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source bps WHERE bps.beach_fid=g.fid
  )
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
