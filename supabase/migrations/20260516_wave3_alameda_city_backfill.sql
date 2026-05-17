-- Wave 3: City of Alameda — 3 beaches, citywide on_leash per §7-3.8
-- (Crown Memorial State Beach sand stays DPR-not_allowed — skipped).
--
-- Source: Alameda Municipal Code §7-3.8 (Leashing), Chapter VII Animal
-- Control, Appendix 7-3 Dogs and Cats.
-- URL: http://alameda-ca.elaws.us/code/coor_chvii_7-3_sec7-3.8
-- Also hosted on Municode at library.municode.com/ca/alameda/codes/
-- code_of_ordinances (Chapter VII ANIMAL CONTROL).
--
-- Operative rule (§7-3.8 verbatim, per web-search-confirmed sources):
--   "It is unlawful for any person owning a dog to cause or permit
--   such dog to be in any public street, alley, or other public place
--   unless the dog is kept securely fastened by a rope, chain or leash
--   of sufficient length and strength to maintain control of the
--   animal and under the supervision of a responsible person, or
--   confined within the private property of the owner."
--   The leashing requirement does not apply to areas officially
--   designated by the City as "Dog Parks" or "Dog Runs."
--
-- Governance note: Robert W. Crown Memorial State Beach is CA DPR-owned
-- and EBRPD-operated (per walkthrough pin); operative rule on sand is
-- DPR not_allowed (already linked). City leash law is a citywide
-- floor that supplements but does NOT override more-specific
-- state/operator rules on state-park beach sand.
--
-- 3 Alameda-city-polygon beaches:
--   fid 8452 Robert W. Crown Memorial State Beach
--                                — CA DPR governs (already linked
--                                  not_allowed on sand, on_leash on
--                                  paved_paths); ALSO already has
--                                  Alameda County on_leash link.
--                                  SKIP — don't double-link.
--   fid 2160 Alameda Point Shoreline
--                                — Former NAS Alameda shoreline; city
--                                  jurisdiction. Default on_leash per
--                                  §7-3.8. Supplements existing Alameda
--                                  County on_leash link.
--   fid 9269 Encinal beach       — South-shore Alameda beach (sub-area
--                                  of greater Crown Memorial area).
--                                  Default on_leash per §7-3.8.
--                                  Supplements existing Alameda County
--                                  on_leash link.

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Alameda Municipal Code §7-3.8 (Leashing — citywide leash requirement on public streets, alleys, and other public places)',
       (SELECT id FROM public.agency WHERE name='Alameda' AND type='city'),
       ARRAY['dog_policy']::text[],
       'http://alameda-ca.elaws.us/code/coor_chvii_7-3_sec7-3.8',
       'Alameda Municipal Code Chapter VII ANIMAL CONTROL, Appendix 7-3 Dogs and Cats, §7-3.8 Leashing (verbatim per City of Alameda Pet Owner Ordinances + elaws.us): It is unlawful for any person owning a dog to cause or permit such dog to be in any public street, alley, or other public place unless the dog is kept securely fastened by a rope, chain or leash of sufficient length and strength to maintain control of the animal and under the supervision of a responsible person, or confined within the private property of the owner. The leashing requirement does not apply to areas officially designated by the City as "Dog Parks" or "Dog Runs." Companion Chapter XXIII PARKS AND RECREATION governs park use. Governance note: Robert W. Crown Memorial State Beach is CA DPR-owned and operated by East Bay Regional Park District (1976 agreement); operative dog rule on sand is DPR not_allowed (parks.ca.gov/?page_id=526) — city leash law does not override more-specific state-park rule on state-park sand. Per [[bounded-operator principle]] walkthrough #4. [Hosted on Municode at library.municode.com/ca/alameda/codes/code_of_ordinances and elaws.us. Both platforms 403/503 against automated fetch on 2026-05-16; verbatim sourced via web search. Fetched 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Alameda Municipal Code §7-3.8%'
);

-- ─── 2. Link Alameda Point Shoreline + Encinal Beach → on_leash ───

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Alameda MC §7-3.8: dogs in public places must be "securely fastened by a rope, chain or leash" under supervision. Beach is city-jurisdiction Alameda Island shoreline (no Dog Park / Dog Run designation).',
       ps.source_url,
       'City leash law supplements existing Alameda County leash link; Crown Memorial sand stays DPR-not_allowed.'
FROM (values (2160),(9269)) as v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Alameda Municipal Code §7-3.8%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
