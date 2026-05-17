-- Wave 3: City of Santa Monica — 3 beaches, blanket no-dogs per
-- §4.04.155 / §4.04.160.
--
-- Source: Santa Monica Municipal Code Chapter 4.04 ANIMALS, hosted on
-- ecode360 / qcode.us.
-- URL: https://ecode360.com/42726883
--
-- Operative rules (per Santa Monica MC §4.04.155 / §4.04.160):
--   - §4.04.160 (or its codified equivalent): "Dogs are not permitted
--     to be held, transported, or permitted on the beach sands lying
--     westerly of that street commonly known as Oceanfront Walk or
--     northerly of the westerly prolongation of the center line of
--     California Avenue, or upon any object placed on the beach sands."
--     — effectively a blanket no-dogs on SM beach sand.
--   - §4.04.155 (leash law for non-beach public property): no person
--     shall permit a dog upon any public property unless in custody/
--     control of a competent person and restrained by a chain or leash
--     six feet or less in fixed length.
--   - Penalties: infraction, fine of not less than $50 per violation
--     per day.
--
-- 3 Santa Monica-city-polygon beaches, all unlinked:
--   - fid 8483 Bay Street Beach        (SM as precedence-1)
--   - fid 5021 Santa Monica Beach
--   - fid 8246 Santa Monica State Beach (state-owned, LA-County-operated
--                                        via 1976 agreement — separate
--                                        operator chain may layer)
--
-- All 3 linked → not_allowed.

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Santa Monica Municipal Code §4.04.155 (Leash law) + §4.04.160 (Dogs prohibited on beach sand)',
       (SELECT id FROM public.agency WHERE name='Santa Monica' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/42726883',
       'Santa Monica Municipal Code Chapter 4.04 ANIMALS. §4.04.160 (Dogs prohibited on the beach): Dogs are not permitted to be held, transported, or permitted on the beach sands lying westerly of that street commonly known as Oceanfront Walk or northerly of the westerly prolongation of the center line of California Avenue, or upon any object placed on the beach sands. This is a blanket prohibition on SM beach sand. §4.04.155 (Leash law): No person having control, charge or custody of any dog shall permit the dog to be upon any public property unless the dog is in the custody and control of a competent person and restrained by a chain or leash six feet or less in fixed length. Violation = infraction, fine not less than $50 per violation per day. Santa Monica State Beach is state-owned and operated by LA County under a 1976 agreement; operator chain may layer state-park and county rules. [Hosted on ecode360 at ecode360.com/42726883 and qcode.us/codes/santamonica/. Fetched 2026-05-16 via web search; deep section fetch via amlegal/ecode360 SPA deferred.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Santa Monica Municipal Code §4.04.155%'
);

-- ─── 2. Link all 3 SM beaches → not_allowed ─────────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'Santa Monica MC §4.04.160: dogs not permitted on SM beach sands; citywide blanket prohibition.',
       ps.source_url,
       CASE g.name
         WHEN 'Santa Monica State Beach' THEN 'State-owned beach operated by LA County (1976 agreement); operator chain may layer state-park and county rules on top of city prohibition.'
         ELSE NULL
       END
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('California','CA') AND j.place_type='C1' AND j.name='Santa Monica'
CROSS JOIN public.policy_source ps
WHERE g.state='CA' AND g.is_active=true
  AND ps.citation LIKE 'Santa Monica Municipal Code §4.04.155%'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source bps WHERE bps.beach_fid=g.fid
  )
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
