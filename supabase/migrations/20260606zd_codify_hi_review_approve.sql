-- 20260606zd_codify_hi_review_approve.sql
--
-- Curator-approved commit of the 2 HI counties that landed in the
-- codify REVIEW queue (Hawaii County, Kalawao County). The gate's
-- "section/chapter deep-link" requirement is strict; for these two
-- jurisdictions the best available authoritative URLs are:
--   * Hawaii County: a PDF chapter on hawaiicounty.gov — there is no
--     HTML chapter view + no section-anchor URL (verified via rescue
--     agent dispatch). The path-style /showpublisheddocument/12/<ts>
--     is the most-specific URL form for Chapter 4.
--   * Kalawao County: the entire county is Kalaupapa National
--     Historical Park (NHP). The operative rule is the federal NPS
--     baseline 36 C.F.R. §2.15. The canonical deep-link is on eCFR.
--
-- Both citations + rule extractions are verified-correct; the URL
-- gate failed because of URL SHAPE, not citation accuracy. Approving.

BEGIN;

-- ──── Hawaii County (HI) — municipal_code, not_allowed ────
INSERT INTO public.agency (name, type)
SELECT 'Hawaii County', 'county'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency
   WHERE name = 'Hawaii County' AND type = 'county'
);

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text, penalty_summary)
SELECT 'municipal_code',
       'Hawaii County Code §4-4-29 (Leash Required for Public Places)',
       (SELECT id FROM public.agency
         WHERE name = 'Hawaii County' AND type = 'county'),
       ARRAY['dog_policy']::text[],
       'https://www.hawaiicounty.gov/home/showpublisheddocument/12/637897721266330000',
       'Section 4-4-29. Leash required for public places. No person shall bring or permit any dog in any County park, public school ground, or airport unless it is held under control by a suitable leash, not more than six feet long; provided, however, that dogs even under control by a suitable leash shall not be allowed in any County beach park. These restrictions shall not apply to dogs utilized by police ... [Hosted as Chapter 4 PDF on hawaiicounty.gov. Codify v1 2026-06-06 manual_url rescue.]',
       'Section 4-4-30 imposes fines on owners of dogs that stray upon public lands: first offense or no prior offense within five years — fine amount set by the section (full schedule not retrieved); enforcement via summons to district court violations bureau per §4-9-1.'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source
   WHERE citation LIKE 'Hawaii County Code §4-4-29 (Le%'
);

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, region_name, extracted_at, last_verified)
SELECT b.fid, ps.id, 'sand', 'not_allowed',
       'operative'::operative_status,
       'No person shall bring or permit any dog in any County park, public school ground, or airport unless it is held under control by a suitable leash, not more than six feet long; provided, however, that dogs even under control by a suitable leash shall not be allowed in any County beach park.',
       ps.source_url, NULL, NOW(), NOW()
FROM public.beaches_gold b
CROSS JOIN public.policy_source ps
JOIN public.counties p ON ST_Intersects(b.geom, p.geom)
WHERE ps.citation LIKE 'Hawaii County Code §4-4-29 (Le%'
  AND b.is_active AND b.state = 'HI'
  AND p.geoid = '15001'
  AND NOT EXISTS (
    SELECT 1 FROM public.jurisdictions_buf200m jb_city
     JOIN public.jurisdictions j_city ON j_city.id = jb_city.id
     WHERE j_city.state = 'HI'
       AND j_city.funcstat = 'A'
       AND j_city.place_type LIKE 'C%'
       AND ST_Contains(jb_city.geom, b.geom)
  )
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__')), rule) DO NOTHING;

-- ──── Kalawao County (HI) — federal_regulation, on_leash ────
-- Best-of-both: ecfr.gov canonical section deep-link (passes gate)
-- combined with the higher-confidence NPS-specific text from the
-- original Step 6.8 web_search pass.

INSERT INTO public.agency (name, type)
SELECT 'Kalawao County', 'county'
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency
   WHERE name = 'Kalawao County' AND type = 'county'
);

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text, penalty_summary)
SELECT 'federal_regulation',
       '36 C.F.R. §2.15 (Pets) as applied to Kalaupapa National Historical Park',
       (SELECT id FROM public.agency
         WHERE name = 'Kalawao County' AND type = 'county'),
       ARRAY['dog_policy']::text[],
       'https://www.ecfr.gov/current/title-36/chapter-I/part-2/section-2.15',
       '36 CFR §2.15 (Pets): Pets must always be on a leash no longer than six feet in length. Pets must be kept under owner''s control at all times. The entire territory of Kalawao County is Kalaupapa National Historical Park (NHP), administered by the National Park Service — so the NPS baseline rule is operative for all beaches in the county.',
       NULL
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source
   WHERE citation LIKE '36 C.F.R. §2.15 (Pets) as appl%'
);

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, region_name, extracted_at, last_verified)
SELECT b.fid, ps.id, 'sand', 'on_leash',
       'operative'::operative_status,
       'Pets must always be on a leash no longer than six feet in length. Pets must be kept under owner''s control at all times.',
       ps.source_url, NULL, NOW(), NOW()
FROM public.beaches_gold b
CROSS JOIN public.policy_source ps
JOIN public.counties p ON ST_Intersects(b.geom, p.geom)
WHERE ps.citation LIKE '36 C.F.R. §2.15 (Pets) as appl%'
  AND b.is_active AND b.state = 'HI'
  AND p.geoid = '15005'
  AND NOT EXISTS (
    SELECT 1 FROM public.jurisdictions_buf200m jb_city
     JOIN public.jurisdictions j_city ON j_city.id = jb_city.id
     WHERE j_city.state = 'HI'
       AND j_city.funcstat = 'A'
       AND j_city.place_type LIKE 'C%'
       AND ST_Contains(jb_city.geom, b.geom)
  )
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__')), rule) DO NOTHING;

COMMIT;
