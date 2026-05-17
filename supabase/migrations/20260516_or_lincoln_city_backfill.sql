-- Wave 5 OR: Lincoln City — 8 beaches under Oregon Beach Bill +
-- OPRD layered rules.
--
-- Source: City of Lincoln City Beach Rules and FAQs page citing
-- Oregon state law for the dog policy on Lincoln City's ocean shore.
-- URL: https://www.lincolncity.org/visitors/beach-rules-and-faqs
--
-- Operative interpretation (per city FAQ + multiple guides):
--   Dogs are welcome on Lincoln City beaches; they must be on leash
--   within state park boundaries (OPRD 6-ft leash per OAR §736-021-0070
--   already linked). Outside state-park areas — on the general state
--   ocean shore — dogs may be off-leash under direct voice/visual
--   control. An owner must carry a leash at all times. Per OAR
--   §736-030-0010 the named voice-control exception cities are Cannon
--   Beach, Seaside, and Rockaway Beach. Lincoln City is NOT in that
--   named exception list, but the city interpretation (per the FAQ)
--   permits voice control on the city's ocean shore beyond state-park
--   boundaries.
--
-- This migration treats Lincoln City as voice-control-permissive on
-- the general ocean shore (citing the city's own FAQ as the
-- agency-administrative-policy source). OPRD 6-ft leash already
-- applies on state-park-operated beaches via the existing OPRD
-- baseline (commit e5bd2b9). The consensus engine resolves layering.
--
-- 8 Lincoln City-polygon beaches in beaches_gold.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'City of Lincoln City — Beach Rules and FAQs (voice-control on ocean shore; OPRD 6-ft leash within state park boundaries)',
       (SELECT id FROM public.agency WHERE name='Lincoln City' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://www.lincolncity.org/visitors/beach-rules-and-faqs',
       'City of Lincoln City Beach Rules and FAQs page. Dogs are welcome on Lincoln City ocean beaches. Per Oregon state law (OAR §736-021-0070), dogs must be leashed within state-park boundaries — leash not to exceed six (6) feet, animal under physical control. Outside state-park areas, on the general state ocean shore, dogs may be off-leash provided they are under direct voice/visual control of the owner; the owner must carry a leash at all times. Lincoln City is NOT one of the OAR §736-030-0010 explicitly named voice-control cities (Cannon Beach, Seaside, Rockaway Beach), but the city''s own interpretation per the Beach Rules FAQ permits voice-control off-leash on the general ocean shore. Overnight camping on the ocean shore within Lincoln City limits is prohibited. [Fetched 2026-05-16; city FAQ summarizes Oregon state law application.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE source_url='https://www.lincolncity.org/visitors/beach-rules-and-faqs'
);

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'off_leash_voice_control', 'operative'::operative_status,
       'City of Lincoln City Beach Rules: dogs allowed off-leash on the ocean shore under direct voice/visual control; owner must carry a leash at all times. OPRD 6-ft leash applies on state-park-operated beach sections (separately linked).',
       ps.source_url,
       'Lincoln City NOT explicitly named in OAR §736-030-0010 voice-control list, but city FAQ permits voice control on the general ocean shore. State-park boundary determines layered OPRD rule.'
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('Oregon','OR') AND j.place_type='C1' AND j.name='Lincoln City'
CROSS JOIN public.policy_source ps
WHERE g.state='OR' AND g.is_active=true
  AND ps.source_url='https://www.lincolncity.org/visitors/beach-rules-and-faqs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
