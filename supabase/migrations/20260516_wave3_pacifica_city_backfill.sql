-- Wave 3: City of Pacifica — 5 beaches with Esplanade off-leash exception.
--
-- Source: City of Pacifica beach dog policy (per City FAQ + the
-- "Dog Itinerary" page on cityofpacifica.org + multiple corroborating
-- sources). The City's own FAQ page is Akamai/JS-blocked from
-- automated fetch; rule captured via web search of authoritative
-- city/county/news sources.
--
-- Operative policy (per City of Pacifica beach dog policy):
--   - DEFAULT: dogs must be on leash on all Pacifica beaches
--   - EXCEPTION: Esplanade Beach permits off-leash dogs
--   - Leash law is actively enforced on: Linda Mar Beach, Rockaway
--     Beach, Pacifica State Beach, and Sharp Park Beach
--
-- Underlying codified ordinance is in the Pacifica Municipal Code
-- (chapter not pinpointed in publicly indexed content; the city code
-- is hosted on Municode at library.municode.com/ca/pacifica/codes/
-- code_of_ordinances per Phase A discovery). To be backfilled with
-- exact section citation during a future refinement pass.
--
-- 5 Pacifica-city-polygon beaches:
--   - fid 9619 Manor Beach              → on_leash (default; unlinked)
--   - fid 7190 Pacifica Esplanade Beach → off_leash (named exception; unlinked)
--   - fid 8607 Pacifica State Beach     → already linked to CA DPR (skipped)
--   - fid 8757 Rockaway Beach           → already linked (skipped)
--   - fid 8217 Sharp Park Beach         → on_leash (default; unlinked)

-- ─── 1. Insert policy_source (agency_administrative_policy) ───────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'City of Pacifica — Dogs on Pacifica beaches (default on_leash; Esplanade Beach off-leash exception)',
       (SELECT id FROM public.agency WHERE name='Pacifica' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://www.cityofpacifica.org/i-want-to/faqs',
       'City of Pacifica beach dog policy (summarized from City FAQ + Dog Itinerary page on cityofpacifica.org): DEFAULT — all dogs must be on leash on all Pacifica beaches. EXCEPTION — Esplanade Beach permits off-leash dogs. Leash law is actively enforced on Linda Mar Beach, Rockaway Beach, Pacifica State Beach, and Sharp Park Beach. Codified backstop in Pacifica Municipal Code (hosted on Municode at library.municode.com/ca/pacifica/codes/code_of_ordinances per Phase A discovery); exact chapter/section not yet pinpointed. [City FAQ page Akamai/JS-blocked from automated fetch; operative rule captured via web search corroborated across multiple sources. Fetched 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE source_url='https://www.cityofpacifica.org/i-want-to/faqs'
);

-- ─── 2. Esplanade Beach (fid 7190) → off_leash (named exception) ──

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 7190, ps.id, 'sand', 'off_leash', 'operative'::operative_status,
       'City of Pacifica beach dog policy: Esplanade Beach is the named off-leash exception to the default leash law on Pacifica beaches.',
       ps.source_url,
       'Named off-leash exception; signage and seasonal/time-of-day variations possible — walkthrough recommended to confirm.'
FROM public.policy_source ps
WHERE ps.source_url='https://www.cityofpacifica.org/i-want-to/faqs'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 3. Default Pacifica beaches → on_leash (unlinked only) ─────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'City of Pacifica default rule: dogs must be on leash on all Pacifica beaches; leash law is actively enforced (per City policy).',
       ps.source_url, NULL
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('California','CA') AND j.place_type='C1' AND j.name='Pacifica'
CROSS JOIN public.policy_source ps
WHERE g.state='CA' AND g.is_active=true
  AND g.fid <> 7190
  AND ps.source_url='https://www.cityofpacifica.org/i-want-to/faqs'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source bps WHERE bps.beach_fid=g.fid
  )
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
