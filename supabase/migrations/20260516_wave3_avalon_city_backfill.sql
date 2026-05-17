-- Wave 3: City of Avalon (Catalina Island) — 3 beaches, blanket
-- no-dogs prohibition per AMC §6-1.110 / §6-1.111.
--
-- Source: City of Avalon "Dogs in Avalon" page citing Avalon Municipal
-- Code §6-1.110 and §6-1.111.
-- URL: https://www.cityofavalon.gov/1419/Dogs-in-Avalon
--
-- Operative facts (per City summary):
--   - Citywide: leash requirements per AMC §6-1.110 + §6-1.111
--   - Beaches: "All public beaches" prohibit dogs (no exception zones
--     or off-leash designations named)
--
-- 3 Avalon-city-polygon beaches in beaches_gold, all unlinked:
--   - 8807 Descanso Beach   (Descanso Beach Club area — private club
--                            but within city limits; city rule applies
--                            as baseline)
--   - 747  Hamilton Beach
--   - 8802 Middle Beach
--
-- All 3 linked → not_allowed.

-- ─── 1. Insert policy_source (agency_administrative_policy) ───────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'City of Avalon — Dogs in Avalon (AMC §6-1.110 + §6-1.111 leash + beach prohibition)',
       (SELECT id FROM public.agency WHERE name='Avalon' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://www.cityofavalon.gov/1419/Dogs-in-Avalon',
       'City of Avalon (Catalina Island) — Dogs in Avalon policy page. The owner shall comply with the leash requirements established by Avalon Municipal Code (AMC) 6-1.110 and 6-1.111. Dogs are prohibited from all public beaches in Avalon. No designated off-leash or dog-friendly beach areas. [Source: cityofavalon.gov Dogs in Avalon page. Underlying AMC §6-1.110 and §6-1.111 referenced but not reproduced on the city page; verbatim codified text not directly captured. Fetched 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE source_url='https://www.cityofavalon.gov/1419/Dogs-in-Avalon'
);

-- ─── 2. Link all 3 Avalon beaches → not_allowed ─────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'City of Avalon Dogs in Avalon policy: dogs prohibited from all public beaches. Citywide leash requirements established by AMC §6-1.110 and §6-1.111 apply elsewhere.',
       ps.source_url,
       CASE g.name
         WHEN 'Descanso Beach' THEN 'Descanso Beach is operated as a private beach club within Avalon city limits; private rules may layer on top of city baseline.'
         ELSE NULL
       END
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('California','CA') AND j.place_type='C1' AND j.name='Avalon'
CROSS JOIN public.policy_source ps
WHERE g.state='CA' AND g.is_active=true
  AND ps.source_url='https://www.cityofavalon.gov/1419/Dogs-in-Avalon'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source bps WHERE bps.beach_fid=g.fid
  )
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
