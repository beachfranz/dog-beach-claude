-- Wave 3: City of Santa Cruz — blanket no-dogs-on-beach for 11
-- SC-city-polygon beaches.
--
-- Source: City of Santa Cruz Municipal Code §8.14.200(1)(a) (Dogs in
-- Public Places — Prohibited Locations).
-- URL: https://www.codepublishing.com/CA/SantaCruz/html/SantaCruz08/SantaCruz0814.html
--
-- Platform: codepublishing.com (per-jurisdiction hosting; not on
-- Municode/amlegal/qcode/ecode360/county.codes). Phase A discovery
-- missed Santa Cruz city because the script doesn't probe
-- codepublishing.com. Only this one CA city found on codepublishing
-- so far — below the threshold for wiring as a discovery fallback.
--
-- Operative rule (§8.14.200(1)(a), verbatim):
--   "Except as provided in subsection (2) and Section 8.14.201, it is
--   unlawful for any person owning, having an interest in, harboring
--   or having charge of the care, custody, control or possession of
--   any dog to cause or permit such dog to be in any of the following
--   locations, whether with or without a leash;
--   (a) On any public beach within the city of Santa Cruz;"
--
-- Other §8.14.200 prohibited locations (for context): San Lorenzo
-- River watercourse, Pacific Avenue Mall, Santa Cruz Municipal Wharf.
--
-- The exemptions in §8.14.201 cover service dogs, guide dogs, and
-- training — narrow carve-outs, not general-public off-leash zones.
--
-- 11 SC-city-polygon beaches in beaches_gold; 7 with Santa Cruz as
-- precedence-1 dog_policy authority, 4 with no resolved authority
-- (Cowell Beach, Harbor Beach, Natural Bridges State Beach, Younger
-- Beach — likely state park or harbor district). All 11 are unlinked.
-- Apply citywide policy to all 11; Natural Bridges and other state-
-- park beaches will have state-park policy supplement once governance
-- resolver runs.

-- ─── 1. Insert policy_source ────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Santa Cruz Municipal Code §8.14.200(1)(a) (Dogs prohibited on any public beach within the city)',
       (SELECT id FROM public.agency WHERE name='Santa Cruz' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://www.codepublishing.com/CA/SantaCruz/html/SantaCruz08/SantaCruz0814.html',
       'Santa Cruz Municipal Code Chapter 8.14 (Dogs and Other Domesticated Animals). §8.14.200 Dogs in Public Places — Prohibited Locations: (1) Except as provided in subsection (2) and Section 8.14.201, it is unlawful for any person owning, having an interest in, harboring or having charge of the care, custody, control or possession of any dog to cause or permit such dog to be in any of the following locations, whether with or without a leash: (a) On any public beach within the city of Santa Cruz; (b) within any portion of the watercourse of the San Lorenzo River within the city of Santa Cruz; (c) on any portion of the Pacific Avenue Mall; (d) on any portion of the Santa Cruz Municipal Wharf unless completely confined within a motor vehicle. §8.14.201 provides exemptions (service/guide/training dogs). §8.14.320 establishes citywide leash requirement for dogs off premises. §8.14.215 requires removal of dog droppings. [Hosted on codepublishing.com — Santa Cruz publishes its code there rather than on Municode/amlegal/qcode. Fetched verbatim via Playwright 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Santa Cruz Municipal Code §8.14.200%'
);

-- ─── 2. Link all 11 SC-city-polygon beaches → not_allowed ────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'Santa Cruz Municipal Code §8.14.200(1)(a): unlawful to cause or permit any dog to be on any public beach within the city of Santa Cruz, whether with or without a leash.',
       ps.source_url,
       NULL
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('California','CA') AND j.place_type='C1' AND j.name='Santa Cruz'
CROSS JOIN public.policy_source ps
WHERE g.state='CA' AND g.is_active=true
  AND ps.citation LIKE 'Santa Cruz Municipal Code §8.14.200%'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source bps WHERE bps.beach_fid=g.fid
  )
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
