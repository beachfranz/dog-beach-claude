-- Wave 3: City of Oakland — 3 beaches, citywide on_leash per §6.04.070
-- (Lake Temescal Beach is EBRPD-governed not_allowed — skipped).
--
-- Source: Oakland Municipal Code §6.04.070 (Dogs at large), Title 6
-- ANIMALS, Chapter 6.04 ANIMAL CONTROL REGULATIONS GENERALLY.
-- URL: http://oakland-ca.elaws.us/code/coor_title6_ch6.04_sec6.04.070
-- (Code is also hosted on Municode at library.municode.com/ca/oakland/
-- codes/code_of_ordinances; Municode SPA selector mismatch in
-- fetch_municode.py — eLaws URL used instead.)
--
-- Operative rule (§6.04.070 verbatim, per oaklandanimalservices.org
-- and elaws.us):
--   "All dogs shall be leashed and securely and continuously held by a
--   responsible person when on public property. All dog guardians
--   (owners, caretakers, dog walkers) must keep the dog securely on a
--   leash no further than six feet away from a responsible dog
--   guardian, and the leash must be securely attached to a collar or
--   harness at all times when on sidewalks, streets, alleys, parks or
--   other public property. Chain leashes or tethers are prohibited.
--   Dogs may only be off leash on private property where permission
--   from the property owner permits the dog to be off-leash, or in
--   designated off-leash areas."
--   Exceptions: Seeing Eye dogs, signal dogs, service dogs, police dogs.
--
-- §6.04.080 designates off-leash areas (none of the Oakland-polygon
-- beaches are listed).
--
-- 3 Oakland-city-polygon beaches:
--   fid 8533 Lake Temescal Beach — already linked to EBRPD Ord 38
--                                   not_allowed (EBRPD governs Lake
--                                   Temescal Regional Recreation Area).
--                                   SKIP — don't apply city default.
--   fid 9078 Radio Beach          — Bay Bridge toll plaza beach;
--                                   city-jurisdiction shoreline. Default
--                                   on_leash per §6.04.070. Supplements
--                                   existing Alameda County link.
--   fid 9076 Toll Plaza Beach     — same site as Radio Beach (separate
--                                   beaches_gold fid). Default on_leash
--                                   per §6.04.070. Supplements existing
--                                   Alameda County link.

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Oakland Municipal Code §6.04.070 (Dogs at large — citywide six-foot leash on all public property)',
       (SELECT id FROM public.agency WHERE name='Oakland' AND type='city'),
       ARRAY['dog_policy']::text[],
       'http://oakland-ca.elaws.us/code/coor_title6_ch6.04_sec6.04.070',
       'Oakland Municipal Code Title 6 ANIMALS, Chapter 6.04 ANIMAL CONTROL REGULATIONS GENERALLY, §6.04.070 Dogs at large (verbatim, per Oakland Animal Services and elaws.us): All dogs shall be leashed and securely and continuously held by a responsible person when on public property. All dog guardians (owners, caretakers, dog walkers) must keep the dog securely on a leash no further than six feet away from a responsible dog guardian, and the leash must be securely attached to a collar or harness at all times when on sidewalks, streets, alleys, parks or other public property. Chain leashes or tethers are prohibited. Dogs may only be off leash on private property where permission from the property owner permits the dog to be off-leash, or in designated off-leash areas. Exceptions: Seeing Eye dogs, signal dogs, service dogs, police dogs. §6.04.080 designates specific off-leash areas in parks (none of the Oakland-polygon beaches are listed; off-leash dog parks include Hardy, Mosswood, Joaquin Miller, Grove Shafter). Lake Temescal Regional Recreation Area is EBRPD-governed (not city) — EBRPD Ord 38 controls there. [Hosted on Municode at library.municode.com/ca/oakland/codes/code_of_ordinances and elaws.us. Municode SPA fetcher selector mismatch — verbatim sourced via elaws/oaklandanimalservices.org. Fetched 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Oakland Municipal Code §6.04.070%'
);

-- ─── 2. Link Radio Beach + Toll Plaza Beach → on_leash ────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Oakland MC §6.04.070: "All dogs shall be leashed and securely and continuously held by a responsible person when on public property" (6-foot max leash). No off-leash designation for these shoreline beaches in §6.04.080.',
       ps.source_url,
       'Bay Bridge toll-plaza shoreline; city jurisdiction. Beach is bordered by Caltrans roadway infrastructure but on Oakland city shoreline. Supplements existing Alameda County leash link.'
FROM (values (9078),(9076)) as v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Oakland Municipal Code §6.04.070%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
