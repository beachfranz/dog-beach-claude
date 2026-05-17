-- Wave 5 WA: City of Seattle — 33 beaches, blanket no-dogs per
-- SMC §18.12.080.
--
-- Source: Seattle Municipal Code §18.12.080 (Parks Code — pets) +
-- §9.25 (Animal Control).
-- URLs:
--   https://www3.ci.seattle.wa.us/parks/about-us/rules-and-regulations
--   https://us.citylaws.org/wa/seattle/seattle-leash-laws-and-off-leash-areas-guide
--
-- Operative rules:
--   - SMC §18.12.080: dogs must be on leash at all times in city
--     parks, EXCEPT inside boundaries of 14 designated off-leash
--     areas (DOLAs). Dogs are NOT allowed at organized athletic
--     fields, beaches, or children's play areas in Seattle parks.
--   - SMC §9.25 covers animal control / licensing.
--   - Penalties: $50-150 typical; $500 at a beach (Seattle treats
--     beach violations as elevated).
--
-- Seattle has 14 DOLAs (off-leash dog parks) — separate from beaches,
-- typically fenced inland sites. None are beaches.
--
-- 33 Seattle-city-polygon beaches in beaches_gold. All linked as
-- not_allowed per the Parks Code beach prohibition. Idempotent skip
-- on existing links (most will already have King County §7.12.225 +
-- WAC §352-32-060 baseline from earlier Wave 4 WA migrations).

-- ─── 1. Seed Seattle agency if not exists ────────────────────────

INSERT INTO public.agency (name, type, web_url)
VALUES ('Seattle', 'city', 'https://www.seattle.gov/')
ON CONFLICT (name, type) DO NOTHING;

-- ─── 2. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Seattle Municipal Code §18.12.080 (Pets in parks) — dogs prohibited at beaches; designated DOLAs exempt',
       (SELECT id FROM public.agency WHERE name='Seattle' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://www3.ci.seattle.wa.us/parks/about-us/rules-and-regulations',
       'Seattle Municipal Code §18.12.080 (Parks Code — pets) + §9.25 (Animal Control). Dogs must be on a leash at all times in city parks, EXCEPT inside the boundaries of 14 designated off-leash areas (DOLAs). Dogs are NOT allowed at organized athletic fields, beaches, or children''s play areas in Seattle parks. Penalties: $50-150 typical; $500 at a beach (Seattle treats beach violations as elevated). Seattle''s 14 DOLAs are fenced inland sites — none are beaches. [Source: City of Seattle Parks Department Rules and Regulations page. Verbatim §18.12.080 text not directly captured (codified on the city clerk portal — clerk.seattle.gov); operative rule corroborated via multiple sources. Fetched 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Seattle Municipal Code §18.12.080%'
);

-- ─── 3. Link all 33 Seattle-polygon beaches → not_allowed ─────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'Seattle Municipal Code §18.12.080: dogs not allowed at beaches in Seattle parks. Citywide blanket prohibition; only the 14 designated DOLAs (none of which are beaches) permit off-leash. Beach violations carry elevated $500 penalty.',
       ps.source_url,
       'Co-applicable: King County §7.12.225 leash law (already linked) for any non-Seattle-park beach; WAC §352-32-060 for any state-park overlap. Seattle Parks Code is the operative rule on city park beaches.'
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('Washington','WA') AND j.place_type='C1' AND j.name='Seattle'
CROSS JOIN public.policy_source ps
WHERE g.state='WA' AND g.is_active=true
  AND ps.citation LIKE 'Seattle Municipal Code §18.12.080%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
