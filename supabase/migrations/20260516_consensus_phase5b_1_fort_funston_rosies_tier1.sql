-- Consensus engine rewrite — Phase 5b.1: add missing tier-1 law
-- links for Fort Funston and Rosie's Dog Beach.
--
-- Phase 5b's coverage report showed only 1 of 245 tier-1+2 CA
-- beaches had a tier-1 statute linked (HBDB). The Phase 2 walkthrough
-- seeds had captured the policy_source ROWS for Fort Funston's
-- §2.15 and Rosie's LBMC, but neglected to insert the
-- beach_policy_source LINKS to those tier-1 sources.
--
-- This migration closes that gap for both beaches, preserving the
-- HBDB pattern: the tier-1 baseline rule is recorded with
-- operative_status reflecting how the lower-tier source modifies it.

-- ─── Fort Funston (fid 6097) — link to 36 CFR §2.15 ──────────────
-- §2.15 (ps 1) is the federal baseline: on-leash unless the
-- Superintendent designates an exception. The 2025 GOGA Compendium
-- (ps 19, already linked) is that designation, authorizing voice
-- control on Fort Funston's main beach. So §2.15's baseline
-- on-leash rule is operatively superseded by lower-tier
-- (the Compendium designation operating under §2.15(b)).

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule,
   operative_status, status_basis_id,
   evidence_verbatim, evidence_url, status_note)
VALUES
  (6097, 1, 'sand', 'on_leash',
   'superseded_by_lower_tier'::operative_status, 19,
   'Federal baseline: pets must be restrained on a leash not to exceed six feet in length, crated, caged, or otherwise physically confined at all times. Exception under §2.15(b) authorizes superintendent to designate areas. The 2025 GOGA Superintendent''s Compendium implements this designation for Fort Funston main beach (voice-control authorized outside the 12-acre NW closure).',
   'https://www.ecfr.gov/current/title-36/chapter-I/part-2/section-2.15',
   'Operatively superseded at Fort Funston by the Compendium-authorized voice-control designation. The 12-acre NW bank-swallow closure remains under §2.15 baseline (no dogs).')
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── Rosie's Dog Beach (fid 6411) — link to LBMC Dog Beach Zone ──
-- LBMC Dog Beach Zone designation (ps 7) is the tier-1 codified
-- authority for the off-leash carve-out. Long Beach's general leash
-- law (ps 8) applies elsewhere in Long Beach; at fid 6411 specifically,
-- the Dog Beach Zone is operative.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule,
   operative_status, status_basis_id,
   evidence_verbatim, evidence_url, status_note)
VALUES
  (6411, 7, 'sand', 'off_leash',
   'operative'::operative_status, NULL,
   'Long Beach Municipal Code designates the area between Granada Ave and Roycroft Ave as the Dog Beach Zone, authorizing off-leash dog access under voice control. Specific section number pending Municode deep-link access; designated ~2003-2005 per pilot-program era.',
   'https://library.municode.com/ca/long_beach',
   'LBMC section number TBD — Municode SPA navigation deferred. Operator-published rules (ps 22) implement the zone.')
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
