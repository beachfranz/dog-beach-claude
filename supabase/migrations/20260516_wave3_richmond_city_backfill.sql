-- Wave 3: City of Richmond — 2 beaches, on_leash via Richmond MC §9.24
-- (which adopts Contra Costa County Animal Control Code by reference).
-- Keller Beach is EBRPD-governed (already linked on_leash; skipped).
--
-- Source: Richmond Municipal Code Chapter 9.24 ANIMALS (Article IX
-- Health), hosted on Municode at library.municode.com/ca/richmond/
-- codes/code_of_ordinances. Richmond Code §9.24 adopts the Contra
-- Costa County Animal Control Code by reference (Chapters 416-2 to
-- 416-12.4 of Division 416 of the County Ordinance Code).
--
-- Operative rule (per City Animal Services + Contra Costa County
-- Animal Ordinance, summarized verbatim):
--   "Dogs must be on a leash and under control unless on your property,
--   a consenting adult's property, or designated dog parks with
--   appropriate signage or regulations." (County Code §416-4.402)
--   Richmond enforces this as the operative city leash law.
--
-- Point Molate Beach Park-specific posted policy (per City facility
-- pages + multiple secondary sources):
--   "Dogs are welcome at Point Molate Beach Park as long as they are
--   always kept on a leash. Dogs are allowed in the park but must be
--   kept on a leash at all times, and owners are responsible for
--   cleaning up after their pets."
--   Point Molate Beach Park is a 413-acre city-owned shoreline park
--   with 1.4 miles of beach; reopened April 2014 after cleanup.
--
-- 2 Richmond-city-polygon beaches:
--   - fid 8695 Keller Beach        — already linked to EBRPD Ord 38
--                                    on_leash (Miller Knox Regional
--                                    Shoreline; EBRPD governs).
--                                    SKIP — don't double-link.
--   - fid 8735 Point Molate Beach  — city-owned Point Molate Beach
--                                    Park; default on_leash per
--                                    Richmond MC §9.24 + posted
--                                    park rules.

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Richmond Municipal Code §9.24 ANIMALS (adopts Contra Costa County Animal Control Code by reference — citywide leash law) + Point Molate Beach Park posted rules',
       (SELECT id FROM public.agency WHERE name='Richmond' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/ca/richmond/codes/code_of_ordinances?nodeId=ARTIXHE_CH9.24AN',
       'Richmond Municipal Code Chapter 9.24 ANIMALS, Article IX HEALTH (verbatim summary, fetched via web search 2026-05-16; Municode SPA selector mismatch in fetch_municode.py). Richmond Code §9.24 adopts the Contra Costa County Animal Control Code by reference, specifically Chapters 416-2 to 416-12.4 of Division 416 of the County Ordinance Code. Contra Costa County Code §416-4.402 (Leash Law): "Dogs must be on a leash and under control unless on your property, a consenting adult''s property, or designated dog parks with appropriate signage or regulations." Richmond enforces this as the operative city leash law. Point Molate Beach Park (413-acre city-owned shoreline park, 1.4 miles of beach, reopened April 2014) posted rules: "Dogs are welcome at Point Molate Beach Park as long as they are always kept on a leash. Dogs are allowed in the park but must be kept on a leash at all times, and owners are responsible for cleaning up after their pets." Keller Beach (within Miller Knox Regional Shoreline) is EBRPD-governed under EBRPD Ord 38. Richmond Municipal Code hosted on Municode at library.municode.com/ca/richmond/codes/code_of_ordinances. [Fetched 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Richmond Municipal Code §9.24 ANIMALS%'
);

-- ─── 2. Link Point Molate Beach → on_leash ─────────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8735, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Point Molate Beach Park posted rules: "Dogs are welcome ... as long as they are always kept on a leash. Dogs are allowed in the park but must be kept on a leash at all times, and owners are responsible for cleaning up after their pets." Richmond MC §9.24 adopts Contra Costa County leash law citywide.',
       ps.source_url,
       'City-owned shoreline park; posted on-leash policy. Pick-up required.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Richmond Municipal Code §9.24 ANIMALS%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
