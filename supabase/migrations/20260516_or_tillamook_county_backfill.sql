-- Wave 4 OR: Tillamook County — 21 beaches, on_leash per Ord 64.
--
-- Source: Tillamook County Ordinance #64 (Dog Control) — codified on
-- amlegal as part of the Tillamook municipal code library.
-- URL: https://codelibrary.amlegal.com/codes/tillamookor/latest/tillamook_or/0-0-0-2710
-- Plus: https://www2.co.tillamook.or.us/gov/Parks/Documents/Ordinance43.pdf
--   (Tillamook County Parks rules — Ord 43)
--
-- Operative rule (Tillamook County Ord 64, per amlegal + city
-- summary): all dogs in Tillamook County must be licensed; dogs must
-- be restrained by a leash, tether or other physical control device
-- not to exceed eight (8) feet in length and under the physical
-- control of a person.
--
-- 21 Tillamook County active beaches in beaches_gold (Cape Lookout,
-- Pacific City, Manzanita, Nehalem, Cannon-area south).

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Tillamook County Ordinance #64 (Dog Control) — 8-ft leash',
       (SELECT id FROM public.agency WHERE name='Tillamook County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://codelibrary.amlegal.com/codes/tillamookor/latest/tillamook_or/0-0-0-2710',
       'Tillamook County Ordinance #64 (Dog Control). All dogs in Tillamook County must be licensed; dogs must be restrained by a leash, tether or other physical control device not to exceed eight (8) feet in length and under the physical control of a person. Tillamook County Parks separately publishes Ord 43 governing parks/beaches. Co-applicable: Oregon State Parks (OPRD) 6-ft leash rule via OAR §736-021-0070 controls on state-park-operated beaches (Cape Lookout SP, Oswald West SP, Cape Meares SP, etc.) — many of the 21 county-polygon beaches are state-park-operated. [Hosted on amlegal/tillamookor. Tillamook also has city-level animal regs (Chapter 90) for the City of Tillamook itself. Fetched via web search 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Tillamook County Ordinance #64%'
);

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Tillamook County Ord 64: dogs must be restrained by leash/tether/other physical control device not to exceed 8 feet under physical control of a person.',
       ps.source_url,
       'County 8-ft leash rule. OPRD 6-ft rule (OAR 736-021-0070) is more restrictive and controls on state-park-operated beaches (many Tillamook coast beaches).'
FROM public.beaches_gold g
CROSS JOIN public.policy_source ps
WHERE g.state='OR' AND g.is_active=true AND g.county_name='Tillamook'
  AND ps.citation LIKE 'Tillamook County Ordinance #64%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
