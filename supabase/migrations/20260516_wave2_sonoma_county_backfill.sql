-- Phase B: Sonoma County code backfill.
--
-- Per [[comprehensive-source-accelerator]] + [[state-expansion-playbook]]:
-- Sonoma County Code Chapter 20 §20-8 (Animals) is the codified dog
-- ordinance for all Sonoma County-operated parks/beaches. §20-8.5
-- adds a coastal-zone-specific provision authorizing additional
-- restrictions where dog predation of coastal livestock is a concern.
--
-- 45 Sonoma County beaches in beaches_gold. Distribution:
--   18 Sonoma County primary (county-operated)
--    8 DPR primary (already linked via Wave 2 DPR catalog)
--   19 no primary set (likely unincorporated/private)
--
-- This migration links Sonoma County primary + no-primary beaches
-- (37 total) to Sonoma §20-8 as the tier-1 statute. DPR-primary
-- beaches (state parks within Sonoma) are NOT linked here — they're
-- governed by DPR catalog already.

-- ─── 1. Insert §20-8 policy_source ────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Sonoma County Code §20-8 + §20-8.5 (Animals — County Parks + Coastal Zone)',
       (SELECT id FROM public.agency WHERE name='Sonoma County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/ca/sonoma_county/codes/code_of_ordinances?nodeId=CH20PARE',
       'Sec. 20-8 Animals. No person shall be permitted to bring, carry, entice or transport a dog, cat or other animal into the park unless such dog, cat or other animal is securely leashed on a maximum six (6) foot leash and in immediate control of a person at all times. ... No dog, cat or other animal shall be permitted at swimming areas, or any area with public facilities, or in any structure of the park except seeing eye dogs for the benefit of a blind person. Dogs may be permitted to run free in areas which, from time to time, may be set aside by the park authority for the specific purpose of exercising a dog... Sec. 20-8.5 Dogs in parks located within the coastal zone. Dogs shall be prohibited from parks, campgrounds and other recreational sites located within the coastal zone of Sonoma County whenever the decision-making body makes a finding and imposes a condition on the coastal development permit that such areas are adjacent to vulnerable grazing lands and dog predation cannot be effectively controlled... [Sonoma County Code Chapter 20 - PARKS AND RECREATION. Fetched via Municode 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Sonoma County Code §20-8%'
);

-- ─── 2. Link Sonoma County primary + no-primary beaches ──────────

WITH sonoma_id AS (
  SELECT id FROM public.agency WHERE name='Sonoma County' AND type='county'
), targets AS (
  -- County primary or no primary set
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  LEFT JOIN public.beach_agency ba_dp
    ON ba_dp.beach_fid = g.fid
   AND ba_dp.authority_domain = 'dog_policy'
   AND ba_dp.precedence_rank = 1
  WHERE g.state='CA' AND g.is_active=true AND g.county_name='Sonoma'
    AND (ba_dp.agency_id = (SELECT id FROM sonoma_id) OR ba_dp.agency_id IS NULL)
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Sonoma County Code §20-8%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Sonoma County Code §20-8: 6-foot leash required in county parks; no dogs at swimming areas. §20-8.5 authorizes additional coastal-zone restrictions adjacent to vulnerable grazing lands.',
       'https://library.municode.com/ca/sonoma_county/codes/code_of_ordinances?nodeId=CH20PARE',
       'Sand rule defaults to on-leash per §20-8 baseline. Swim areas within a beach would be §20-8 not_allowed; per-beach refinement deferred. Coastal-zone §20-8.5 prohibition fires only when a specific CDP condition imposes it.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
