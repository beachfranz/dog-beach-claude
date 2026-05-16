-- Phase B: San Diego County code backfill.
--
-- Discovered 2026-05-16 via Franz's pointer: SD County Department of
-- Animal Services publishes a summary at sddac.com/content/sdc/das/
-- license-laws/laws.html that cites the codified ordinance sections.
-- SD County's actual code is on AMLEGAL (not Municode):
--   https://codelibrary.amlegal.com/codes/san_diego/...
--
-- Verbatim from the DAS summary (operative tier-3 agency policy):
--   "At home dog owners must control their dogs by voice, an electronic
--    containment system, or keep them restrained by leash, fence or other
--    enclosure. (San Diego County Code section 62.669; California Penal
--    Code Section 597)"
--   "Away from home dog owners must restrain their dog using a hand-held
--    leash no longer than six feet. The person holding the leash must be
--    capable of controlling the dog."
--
-- Codified sections cited:
--   §62.668 Sanitation
--   §62.669 Restraint requirement
--   §62.670 Feces removal
--   §62.610, §62.613, §62.615, §62.620, §62.674 (licensing, rabies, bites,
--   dangerous-dog declarations — adjacent but not core dog-policy)
--
-- 69 SD County beaches in beaches_gold.

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'San Diego County Code §62.669 (Restraint) + §62.668 (Sanitation) + §62.670 (Feces)',
       (SELECT id FROM public.agency WHERE name='San Diego County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://codelibrary.amlegal.com/codes/san_diego/latest/sandiego_regs/0-0-0-102784',
       'San Diego County Code §62.669: Restraint requirement — at home owners must control their dogs by voice, an electronic containment system, or keep them restrained by leash, fence or other enclosure. Away from home, dog owners must restrain their dog using a hand-held leash no longer than six feet, and the person holding the leash must be capable of controlling the dog. §62.668: Sanitation — owners must keep premises sanitary and free from fly-breeding, offensive odors, disease. §62.670: Feces removal required to a proper receptacle. [SD County Code, hosted on amlegal at codelibrary.amlegal.com/codes/san_diego/. Summarized via SDDAC laws page 2026-05-16; section text via amlegal deferred.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'San Diego County Code §62.669%'
);

-- ─── 2. Insert tier-3 SDDAC summary as agency_administrative_policy ─

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)
SELECT 'agency_administrative_policy',
       'San Diego County Department of Animal Services — Pet Ownership Laws',
       (SELECT id FROM public.agency WHERE name='San Diego County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://www.sddac.com/content/sdc/das/license-laws/laws.html',
       'San Diego County Department of Animal Services — Pet Ownership Laws (summary of SD County Code). Restraint and Leash Law: at home dog owners must control their dogs by voice, an electronic containment system, or keep them restrained by leash, fence or other enclosure. Away from home dog owners must restrain their dog using a hand-held leash no longer than six feet. [SDDAC operative summary. Fetched 2026-05-16.]',
       (SELECT id FROM public.policy_source WHERE citation LIKE 'San Diego County Code §62.669%')
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE source_url='https://www.sddac.com/content/sdc/das/license-laws/laws.html'
);

-- ─── 3. Link SD County beaches ───────────────────────────────────

WITH sd_id AS (
  SELECT id FROM public.agency WHERE name='San Diego County' AND type='county'
), targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  LEFT JOIN public.beach_agency ba_dp
    ON ba_dp.beach_fid = g.fid AND ba_dp.authority_domain = 'dog_policy' AND ba_dp.precedence_rank = 1
  WHERE g.state='CA' AND g.is_active=true AND g.county_name='San Diego'
    AND (ba_dp.agency_id = (SELECT id FROM sd_id) OR ba_dp.agency_id IS NULL)
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'SD County Code §62.669: away from home, dog owners must restrain their dog using a hand-held leash no longer than six feet.',
       ps.source_url,
       NULL
FROM targets t
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'San Diego County Code §62.669%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Also link to the SDDAC summary (operative tier-3)
WITH sd_id AS (
  SELECT id FROM public.agency WHERE name='San Diego County' AND type='county'
), targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  LEFT JOIN public.beach_agency ba_dp
    ON ba_dp.beach_fid = g.fid AND ba_dp.authority_domain = 'dog_policy' AND ba_dp.precedence_rank = 1
  WHERE g.state='CA' AND g.is_active=true AND g.county_name='San Diego'
    AND (ba_dp.agency_id = (SELECT id FROM sd_id) OR ba_dp.agency_id IS NULL)
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'SDDAC operative summary: away from home dog owners must restrain their dog using a hand-held leash no longer than six feet.',
       ps.source_url,
       NULL
FROM targets t
CROSS JOIN public.policy_source ps
WHERE ps.source_url='https://www.sddac.com/content/sdc/das/license-laws/laws.html'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
