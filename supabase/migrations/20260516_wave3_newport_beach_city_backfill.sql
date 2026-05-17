-- Wave 3: City of Newport Beach — citywide leash + 10am–4:30pm
-- prohibition for all unlinked NB-polygon beaches.
--
-- Source: City of Newport Beach "Beach Information" page (operative
-- agency policy summary).
-- URL: https://www.newportbeachca.gov/how-do-i-/find/beach-information
--
-- Operative rules (verbatim from the "Dogs on the Beach" section):
--   - "Dogs are never allowed on the beach or any beachfront sidewalk
--     between the hours of 10 a.m. and 4:30 p.m., including the ocean
--     front beaches and bay front beaches."
--   - "Before 10 a.m. and after 4:30 p.m., dogs are allowed, if they
--     are securely restrained by a substantial leash or chain not
--     exceeding six feet in length and controlled by a person who is
--     competent to restrain the dog."
--   - "Off-leash dogs are never allowed in any public spaces including
--     parks and beaches."
--   - "The person controlling the dog must have in their possession an
--     implement, device or bag to immediately remove any feces deposited
--     by the animal."
--
-- Rule is year-round (not seasonal). 6-foot leash max. Time-of-day
-- restriction applies every day.
--
-- 12 Newport Beach polygon beaches in beaches_gold; 4 have NB as
-- precedence-1 dog_policy authority, 8 have no resolved authority.
-- None are currently linked. All 12 get the NB citywide policy
-- applied (idempotent — skips any beach with an existing link).
--
-- Carve-out worth noting: Corona del Mar State Beach (fid 8333) is a
-- CA State Park; the city rule still applies on the sand per "all
-- ocean front beaches" language, but state-park layered rules may
-- supplement.

-- ─── 1. Insert policy_source (agency_administrative_policy) ───────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'City of Newport Beach — Beach Information / Dogs on the Beach (citywide leash + 10am–4:30pm prohibition)',
       (SELECT id FROM public.agency WHERE name='Newport Beach' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://www.newportbeachca.gov/how-do-i-/find/beach-information',
       'City of Newport Beach — Dogs on the Beach (operative city policy). Dogs are never allowed on the beach or any beachfront sidewalk between the hours of 10 a.m. and 4:30 p.m., including the ocean front beaches and bay front beaches. Before 10 a.m. and after 4:30 p.m., dogs are allowed, if they are securely restrained by a substantial leash or chain not exceeding six feet in length and controlled by a person who is competent to restrain the dog. Off-leash dogs are never allowed in any public spaces including parks and beaches. The person controlling the dog must have in their possession an implement, device or bag to immediately remove any feces deposited by the animal. Rule applies year-round (not seasonal). [Fetched from City of Newport Beach Beach Information page via Playwright 2026-05-16. Underlying NBMC citation not specified on the page; Newport Beach''s Municipal Code is hosted on its own city portal and codifies the leash/restraint rule under animal control sections — not yet captured verbatim.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE source_url='https://www.newportbeachca.gov/how-do-i-/find/beach-information'
);

-- ─── 2. Link all unlinked NB-polygon beaches → on_leash w/ time note ──

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Newport Beach citywide rule: dogs prohibited 10am–4:30pm; outside those hours dogs allowed on 6-ft-max leash, never off-leash. Year-round.',
       ps.source_url,
       'Time-of-day restriction: dogs prohibited 10am–4:30pm every day. On-leash before 10am and after 4:30pm. Off-leash never allowed.'
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('California','CA') AND j.place_type='C1' AND j.name='Newport Beach'
CROSS JOIN public.policy_source ps
WHERE g.state='CA' AND g.is_active=true
  AND ps.source_url='https://www.newportbeachca.gov/how-do-i-/find/beach-information'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source bps WHERE bps.beach_fid=g.fid
  )
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
