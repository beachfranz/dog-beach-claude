-- Wave 3: City of Fort Bragg — 4 beaches, citywide on_leash per
-- §7.16.030(A).
--
-- Source: Fort Bragg Municipal Code Chapter 7.16 ANIMAL REGULATIONS,
-- via codepublishing.com.
-- URL: https://www.codepublishing.com/CA/FortBragg/html/FortBragg07/FortBragg0716.html
--
-- Operative rule (§7.16.030(A) Dogs at Large — Prohibited, verbatim):
--   "No person owning or having a dog in his or her care, charge,
--   control, custody or possession shall cause, permit or allow such
--   dog to be at large in or upon any public street, road, alley or
--   other public place unless such dog is under restraint by a
--   substantial leash or chain, except when the dog is confined to a
--   motor vehicle or other enclosure under humane conditions."
--
-- §7.16.030(C) exceptions: guide/signal/service dogs; LE dogs;
-- commercial training/obedience class participation.
--
-- §7.16.020(K)+(L) require feces removal and that owner carry an
-- implement for that purpose.
--
-- No specific beach prohibition or off-leash beach designation in the
-- Fort Bragg code — citywide leash-required rule applies to all beach
-- access. Note that Glass Beach is part of MacKerricher State Park
-- (CA DPR jurisdiction) for some sections; the city-polygon overlap
-- still allows city rule to apply where city jurisdiction obtains.
--
-- 4 Fort Bragg-city-polygon beaches:
--   fid 8428 Glass Beach          — already linked (CA DPR state-park layer)
--   fid 6206 Noyo Beach           — Fort Bragg as precedence-1, unlinked
--   fid 6118 Otsuchi Point        — Fort Bragg as precedence-1, unlinked
--   fid 8699 Pudding Creek Beach  — already linked (probably CA DPR)
--
-- All 4 get on_leash; idempotent skip-on-existing-link applies for
-- already-linked beaches (Glass + Pudding Creek will be skipped by the
-- WHERE NOT EXISTS; Noyo + Otsuchi will be linked).

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Fort Bragg Municipal Code §7.16.030 (Dogs at Large — Prohibited) + §7.16.020 (Control of Animals)',
       (SELECT id FROM public.agency WHERE name='Fort Bragg' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://www.codepublishing.com/CA/FortBragg/html/FortBragg07/FortBragg0716.html',
       'Fort Bragg Municipal Code Chapter 7.16 ANIMAL REGULATIONS. §7.16.030(A) Dogs at Large — Prohibited: No person owning or having a dog in his or her care, charge, control, custody or possession shall cause, permit or allow such dog to be at large in or upon any public street, road, alley or other public place unless such dog is under restraint by a substantial leash or chain, except when the dog is confined to a motor vehicle or other enclosure under humane conditions. §7.16.030(C) exceptions: (1) guide/signal/service dogs with valid assistance dog ID; (2) dogs assisting peace officers; (3) dogs enrolled and actively participating in commercial training/obedience class/exhibition/competition. §7.16.020(K) prohibits permitting any dog to defecate on public property or private property of another unless feces immediately removed (visually disabled persons with seeing-eye dogs exempt). §7.16.020(L) requires walker to carry suitable container for feces removal. No specific beach prohibition or off-leash beach designation in the code; citywide leash law applies. Ord. 891 (02-14-2011); Am. Ord. 895 (07-11-2011). [Hosted on codepublishing.com. Fetched verbatim via Playwright 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Fort Bragg Municipal Code §7.16.030%'
);

-- ─── 2. Link Fort Bragg beaches → on_leash (unlinked only) ──────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Fort Bragg Municipal Code §7.16.030(A): no person shall permit a dog to be at large in or upon any public place unless under restraint by a substantial leash or chain. No specific beach off-leash designation in the code.',
       ps.source_url, NULL
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('California','CA') AND j.place_type='C1' AND j.name='Fort Bragg'
CROSS JOIN public.policy_source ps
WHERE g.state='CA' AND g.is_active=true
  AND ps.citation LIKE 'Fort Bragg Municipal Code §7.16.030%'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source bps WHERE bps.beach_fid=g.fid
  )
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
