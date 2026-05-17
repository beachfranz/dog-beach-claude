-- Wave 5 OR: Voice-control city batch — Cannon Beach, Seaside, and
-- Rockaway Beach are explicit OAR §736-030-0010 voice-control
-- jurisdictions for the ocean shore within their city limits.
--
-- Source: OAR §736-030-0010 (Prohibition of Dogs Off Leash) — Oregon
-- state administrative rule that names specific cities.
-- URL: https://oregon.public.law/rules/oar_736-030-0010
--
-- Operative rule (per OAR §736-030-0010 + city dog-on-beach pages):
--   "Dogs are prohibited except on a leash or under voice or signal
--   command on the ocean shore within the city limits of Cannon Beach,
--   Seaside and Rockaway Beach."
--
-- This is the OR equivalent of California's voice-control jurisdictions
-- (e.g., Humboldt County [[walkthrough]]). The named cities allow
-- off-leash dogs provided they're under VOICE OR SIGNAL CONTROL of
-- the owner.
--
-- Other OR coastal cities (Lincoln City, Newport, Bandon, Brookings,
-- Gearhart, Manzanita, Waldport, Yachats, Gold Beach, Port Orford,
-- Warrenton) default to OPRD OAR §736-021-0070 6-ft leash on
-- state-park-operated beaches + county code.
--
-- Note: Haystack Rock Marine Garden (Cannon Beach) requires dogs on
-- leash to protect the tide pool ecosystem — sub-area exception not
-- captured here; flagged for per-beach walkthrough.
--
-- Beaches affected per city polygon containment:
--   - Cannon Beach: 3 beaches
--   - Seaside: 3 beaches
--   - Rockaway Beach: 3 beaches

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'state_regulation',
       'OAR §736-030-0010 (Prohibition of Dogs Off Leash) — voice-control exception cities',
       (SELECT id FROM public.agency WHERE name='Oregon Parks and Recreation Department' AND type='state_department'),
       ARRAY['dog_policy']::text[],
       'https://oregon.public.law/rules/oar_736-030-0010',
       'Oregon Administrative Rules §736-030-0010 Prohibition of Dogs Off Leash. General rule prohibits off-leash dogs in state parks. NAMED EXCEPTION: dogs are prohibited except on a leash OR under voice or signal command on the ocean shore within the city limits of Cannon Beach, Seaside, and Rockaway Beach. These three cities are explicit voice-control jurisdictions. Sub-area exception: Haystack Rock Marine Garden (Cannon Beach) requires dogs on leash to protect tide pool ecosystem. [Fetched 2026-05-16; codified text at oregon.public.law/rules/oar_736-030-0010.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'OAR §736-030-0010%'
);

-- Link Cannon Beach + Seaside + Rockaway Beach polygon beaches as off_leash_voice_control

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'off_leash_voice_control', 'operative'::operative_status,
       'OAR §736-030-0010: dogs prohibited except on leash OR under voice or signal command on the ocean shore within city limits of Cannon Beach, Seaside, Rockaway Beach.',
       ps.source_url,
       CASE
         WHEN j.name = 'Cannon Beach' THEN 'Haystack Rock Marine Garden sub-area requires leashed dogs (tide pool protection). Walkthrough recommended to flag sub-area boundaries.'
         ELSE NULL
       END
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('Oregon','OR') AND j.place_type='C1'
  AND j.name IN ('Cannon Beach','Seaside','Rockaway Beach')
CROSS JOIN public.policy_source ps
WHERE g.state='OR' AND g.is_active=true
  AND ps.citation LIKE 'OAR §736-030-0010%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
