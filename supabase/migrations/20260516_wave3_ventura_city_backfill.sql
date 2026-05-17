-- Wave 3: City of Ventura (San Buenaventura) — 6 beaches, citywide
-- on_leash per §8.050.510 (no beach is a designated off-leash area).
--
-- Source: City of Ventura Municipal Code §8.050.510 (Leash law) +
-- §8.050.515 (Designated off-leash areas).
--
-- Operative rules (verbatim, per Franz pointer 2026-05-16):
--
-- §8.050.510 Leash law:
--   "Dogs must be, at all times, when in a public space, securely
--   leashed and the leash is held continuously in the hands of a
--   responsible person capable of controlling such dog."
--
-- §8.050.515 Designated off-leash areas — applies ONLY to:
--   - Camino Real Park (fenced off-leash dog area)
--   - Arroyo Verde Park (off-leash dog hours)
--   No beach is a designated off-leash area in Ventura.
--
-- 6 Ventura-city-polygon beaches in beaches_gold; all get on_leash per
-- the citywide leash law. 5 are already linked (probably to CA DPR
-- since San Buenaventura State Beach is operated by State Parks); 1
-- (South Jetty Beach) has the City as primary authority and is
-- unlinked.

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Ventura Municipal Code §8.050.510 (Leash law) + §8.050.515 (Designated off-leash areas — Camino Real Park + Arroyo Verde Park only)',
       (SELECT id FROM public.agency WHERE name='San Buenaventura (Ventura)' AND type='city'),
       ARRAY['dog_policy']::text[],
       NULL,
       'Ventura Municipal Code §8.050.510 Leash law: Dogs must be, at all times, when in a public space, securely leashed and the leash is held continuously in the hands of a responsible person capable of controlling such dog. §8.050.515 Designated off-leash areas: only at Camino Real Park (fenced) and Arroyo Verde Park (off-leash hours). No beach in the City of Ventura is a designated off-leash area; the citywide leash law of §8.050.510 applies to all beach access. [Verbatim per Franz pointer 2026-05-16. City Municipal Code platform not directly accessed for fetch; Ventura''s code is hosted at the city''s own portal — to be backfilled with source_url + verbatim TOC during a future refinement pass.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Ventura Municipal Code §8.050.510%'
);

-- ─── 2. Link all 6 Ventura-city-polygon beaches → on_leash ──────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Ventura Municipal Code §8.050.510: dogs must be securely leashed at all times when in a public space, leash held continuously in the hands of a responsible person. No beach is a §8.050.515 designated off-leash area (only Camino Real Park and Arroyo Verde Park are designated).',
       NULL, NULL
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('California','CA') AND j.place_type='C1'
  AND j.name LIKE 'San Buenaventura%'
CROSS JOIN public.policy_source ps
WHERE g.state='CA' AND g.is_active=true
  AND ps.citation LIKE 'Ventura Municipal Code §8.050.510%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
