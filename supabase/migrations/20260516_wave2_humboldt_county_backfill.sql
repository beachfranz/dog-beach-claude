-- Phase B: Humboldt County code backfill.
--
-- Discovered 2026-05-16 via Franz's pointer:
--   https://humboldt.county.codes/Code/541
--
-- Platform: county.codes (hosted by General Code, same family as eCode360).
-- Subdomain pattern: <county>.county.codes. Only Humboldt found on this
-- platform among the CA county unknowns — SLO, Del Norte, Contra Costa
-- explicitly return "Site Not Found" at <slug>.county.codes. So no fallback
-- platform extension to the discovery script for one jurisdiction.
--
-- Code structure:
--   Title V: Health and Safety
--     Div. 4: Plants and Animals
--       Chapter 1 (numbered "541"): Regulating and Licensing Dogs
--       Chapter 5 (numbered "545"): Regulating Domestic Animals (Other Than Dogs)
--
-- Operative rule (§541-21 Confined to Owner's Premises, verbatim):
--   (a) It shall be unlawful for any person to cause, permit or allow any
--   dog owned, harbored, controlled or kept by him/her to roam, run or
--   stray away from the premises where the same is owned, harbored or
--   kept, at any time, except in the custody and control of the owner or
--   some responsible person, duly authorized by the owner.
--   (b) The Animal Control Officer may seize and impound any dog found
--   running at large at any time contrary to the provision of this section.
--
-- IMPORTANT — voice-control jurisdiction: Humboldt's rule uses "custody and
-- control" language, NOT explicit "leash" mandate. Unlike SD (§62.669
-- 6-foot leash) and SB (§32.0108(c) leash), Humboldt allows off-leash
-- provided the dog is in the custody/control of a responsible person.
-- Default sand rule = `off_leash_voice_control`, not `on_leash`.
--
-- §541-1(a) purposes — "To prevent dogs running at large at any time."
-- §541-1(g) — "the owner of any dog is responsible to confine his dog to
-- his premises to prevent the dog from damaging the person or property
-- of others."
--
-- 26 Humboldt County beaches in beaches_gold (coastal — far-north CA).

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Humboldt County Code §541-21 (Confined to Owner''s Premises) — Title V / Div. 4 / Chapter 1',
       (SELECT id FROM public.agency WHERE name='Humboldt County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://humboldt.county.codes/Code/541',
       'Humboldt County Code §541-21 Confined to Owner''s Premises. (a) It shall be unlawful for any person to cause, permit or allow any dog owned, harbored, controlled or kept by him/her to roam, run or stray away from the premises where the same is owned, harbored or kept, at any time, except in the custody and control of the owner or some responsible person, duly authorized by the owner. (b) The Animal Control Officer may seize and impound any dog found running at large at any time contrary to the provision of this section. §541-1 purposes (a) To prevent dogs running at large at any time; (g) the owner of any dog is responsible to confine his dog to his premises to prevent the dog from damaging the person or property of others. §541-81 Penalties: infraction, $50 first offense, $100 subsequent; misdemeanor if dog wounds/kills livestock or poultry. [Hosted on county.codes (General Code) at humboldt.county.codes/Code/541. Fetched verbatim via Playwright 2026-05-16. Note: §541-21 does NOT mandate a leash — "custody and control" language permits voice control.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Humboldt County Code §541-21%'
);

-- ─── 2. Link Humboldt County beaches ────────────────────────────

WITH hc_id AS (
  SELECT id FROM public.agency WHERE name='Humboldt County' AND type='county'
), targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  LEFT JOIN public.beach_agency ba_dp
    ON ba_dp.beach_fid = g.fid AND ba_dp.authority_domain = 'dog_policy' AND ba_dp.precedence_rank = 1
  WHERE g.state='CA' AND g.is_active=true AND g.county_name='Humboldt'
    AND (ba_dp.agency_id = (SELECT id FROM hc_id) OR ba_dp.agency_id IS NULL)
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid, ps.id, 'sand', 'off_leash_voice_control', 'operative'::operative_status,
       'Humboldt County Code §541-21: unlawful to permit dog to roam from premises except in the custody and control of the owner or some responsible person. "Custody and control" language permits voice control — no explicit leash mandate.',
       ps.source_url,
       NULL
FROM targets t
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Humboldt County Code §541-21%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
