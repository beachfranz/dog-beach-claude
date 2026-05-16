-- Walkthrough seeds round 2 — addendum: Coronado beach_agency rows.
--
-- Phase 7.1 scoped agency-linking to tier-1+2 CA beaches. Coronado main
-- beach (fid 8715) is tier-4 (no_dogs) so it didn't get covered.
-- The walkthrough explicitly captures Coronado, so we link it now.

INSERT INTO public.beach_agency (beach_fid, agency_id, authority_domain, precedence_rank)
SELECT 8715, id, d.dom, 1
  FROM public.agency,
       (VALUES ('dog_policy'), ('operations'), ('parking'), ('water_safety')) d(dom)
 WHERE name='Coronado' AND type='city'
ON CONFLICT (beach_fid, agency_id, authority_domain) DO NOTHING;

INSERT INTO public.beach_agency (beach_fid, agency_id, authority_domain, precedence_rank)
SELECT 8715, id, 'water_quality', 1
  FROM public.agency
 WHERE name='San Diego County' AND type='county'
ON CONFLICT (beach_fid, agency_id, authority_domain) DO NOTHING;
