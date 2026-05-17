-- Wave 3: City of Encinitas — 3 beaches, mixed (city beaches no-dogs;
-- Cardiff State Beach DPR-governed on_leash).
--
-- Source: Encinitas Municipal Code §8.08.030 (Dogs and Cats) and the
-- City marine safety beach-rules page.
-- URL: https://ecode360.com/44477391 (Title 8 Beaches, Parks, and
-- Waterways; specifically §8.08.030 inside Chapter 8.08 BEACHES).
-- Also: encinitasca.gov/government/departments/public-safety/
-- marine-safety/beach-rules-and-regulations (411'd by Akamai, but
-- rule corroborated via web search).
--
-- Operative rule (§8.08.030 verbatim, fetched via Playwright):
--   "With the exception of a dog accompanying an unsighted person or
--   hearing impaired person there present, it is unlawful for any
--   person to bring, allow or permit a dog or cat owned or controlled
--   by such person upon a City beach."
--   (Ord. 92-02)
--
-- City marine-safety scope (per City beach rules page, corroborated):
--   No dogs allowed on any beach or beach access operated by the City
--   (beaches north of San Elijo State Beach in Cardiff to Encinitas
--   northern city limits).
--
-- §8.04.160 leash carve-out (citywide context — not applicable on
-- beaches; only applies at Encinitas Viewpoint Park, Sun Vista Park
-- East, Orpheus Park, and Trail Segment 72).
--
-- 3 Encinitas-city-polygon beaches, per-beach mapping:
--
--   fid 8341 Cardiff State Beach  — south of city limits / DPR-operated;
--                                   already linked to CA State Parks
--                                   on_leash policy. SKIP (don't overwrite).
--   fid 8342 Leucadia State Beach — north of San Elijo, city-operated;
--                                   §8.08.030 prohibits dogs → not_allowed.
--                                   Existing on_leash links via SD County
--                                   are SUPPLEMENTED (not replaced) by
--                                   this more-specific city ordinance.
--   fid 8343 Grandview Surf Beach — north of San Elijo, city-operated;
--                                   §8.08.030 prohibits dogs → not_allowed.
--                                   Existing on_leash links via SD County
--                                   are SUPPLEMENTED by this city
--                                   ordinance.
--
-- Net 2 new links (city ordinance specificity overrides county default
-- for the two city-operated beaches; Cardiff stays DPR-governed).

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Encinitas Municipal Code §8.08.030 (Dogs and Cats — prohibited on City beaches) + §8.04.160 (Unleashed Dogs on Public Property — specific parks/trails carve-out)',
       (SELECT id FROM public.agency WHERE name='Encinitas' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/44477391',
       'Encinitas Municipal Code Title 8 Beaches, Parks, and Waterways, Chapter 8.08 BEACHES, §8.08.030 Dogs and Cats (verbatim): With the exception of a dog accompanying an unsighted person or hearing impaired person there present, it is unlawful for any person to bring, allow or permit a dog or cat owned or controlled by such person upon a City beach. (Ord. 92-02). Companion §8.04.160 Unleashed Dogs on Public Property: dogs may be unleashed on public property ONLY at Encinitas Viewpoint Park, Sun Vista Park (East), Orpheus Park (specified hours), and a portion of Trail Segment 72; not applicable to beaches. City marine-safety page (encinitasca.gov/government/departments/public-safety/marine-safety/beach-rules-and-regulations) clarifies scope: dogs prohibited on any beach or beach access operated by the City (beaches north of San Elijo State Beach in Cardiff to Encinitas northern city limits). Cardiff State Beach south of San Elijo is CA DPR-operated and follows state-park dog rules (typically on_leash where designated). [Hosted on ecode360 at ecode360.com/44477391. Fetched verbatim via Playwright 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Encinitas Municipal Code §8.08.030%'
);

-- ─── 2. Link city-operated beaches (Leucadia, Grandview) → not_allowed ──

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'Encinitas MC §8.08.030: "With the exception of a dog accompanying an unsighted person or hearing impaired person there present, it is unlawful for any person to bring, allow or permit a dog or cat owned or controlled by such person upon a City beach." Beach is city-operated (north of San Elijo State Beach within Encinitas city limits).',
       ps.source_url,
       'City ordinance specificity supplements / overrides existing SD County on_leash links for this city-operated beach. Cardiff State Beach (south of city operating area) remains DPR-governed.'
FROM (values (8342),(8343)) as v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Encinitas Municipal Code §8.08.030%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
