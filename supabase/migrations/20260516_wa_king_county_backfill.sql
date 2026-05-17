-- Wave 4: King County (WA) parks dog-policy backfill.
--
-- Source: King County Code Title 7 §7.12.225 (Pet animals) — codified
-- as Chapter 7.12 RULES FOR USE OF FACILITIES. Title 11 (Animal Care
-- and Control) is the licensing/dangerous-dog framework but the
-- operative rule for KC parks/beaches is §7.12.225.
--
-- Live URL: https://aqua.kingcounty.gov/council/clerk/code/10_Title_7.htm
--
-- Operative rule (§7.12.225, verbatim, multi-subsection):
--   A. Domestic pet animals are permitted in all park areas except
--      play areas and athletic fields or where otherwise prohibited
--      by posting.
--   B. Except in a designated off-leash area for dogs, pet animals
--      must be kept on a leash no greater than eight feet long and
--      under control at all times. Dogs in designated off-leash areas
--      must be accompanied by the dog's owner or other caretaker, be
--      under vocal control, and not cause a nuisance or safety hazard.
--   C. Any person with a pet animal shall be responsible for the
--      conduct of the animal and for removing from the park area
--      feces deposited by the animal.
--   D. Pet animals must not be allowed to bite or in any way molest
--      or annoy park visitors or bark continuously.
--   E. Pets and other listed animals are prohibited in designated
--      swimming areas.
--
-- Service animals exempted where postings apply.
--
-- 53 King County beaches in beaches_gold. Many are within Seattle
-- city limits (governed by Seattle Municipal Code, not KCC Title 7)
-- but King County code is the unincorporated-area baseline and the
-- KC Parks rule controls KC-operated parks regardless. Per-beach
-- city/county jurisdiction refinement deferred to a later Wave 3 city
-- backfill cycle. For this Wave 4 migration: link every active KC
-- beach to §7.12.225 as the tier-1 county-park-rule.
--
-- Sand rule = on_leash (8-foot leash baseline).
-- Swim areas within a beach would be not_allowed under §7.12.225.E,
-- but per-beach refinement deferred.

-- ─── 1. Insert policy_source ─────────────────────────────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'King County Code §7.12.225 (Pet animals) — Title 7 / Chapter 7.12 Rules for Use of Facilities',
       (SELECT id FROM public.agency WHERE name='King County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://aqua.kingcounty.gov/council/clerk/code/10_Title_7.htm',
       'King County Code §7.12.225 Pet animals. A. Domestic pet animals are permitted in all park areas except play areas and athletic fields or where otherwise prohibited by posting. B. Except in a designated off-leash area for dogs, pet animals must be kept on a leash no greater than eight feet long and under control at all times. Dogs in designated off-leash areas must be accompanied by the dog''s owner or other caretaker, be under vocal control, and not cause a nuisance or safety hazard. C. Any person with a pet animal shall be responsible for the conduct of the animal and for removing from the park area feces deposited by the animal. D. Pet animals must not be allowed to bite or in any way molest or annoy park visitors or bark continuously. E. Pets, horses, pack animals, and other listed animals are prohibited in designated swimming areas. Service animals exempted where postings apply. [Title 11 KCC governs animal licensing/dangerous dogs separately; §7.12.225 is the operative park-and-beach rule. Fetched via WebFetch 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'King County Code §7.12.225%'
);

-- ─── 2. Link King County beaches ─────────────────────────────────

WITH targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  WHERE g.state='WA' AND g.is_active=true AND g.county_name='King'
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'King County Code §7.12.225%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'King County Code §7.12.225.B: pet animals must be kept on a leash no greater than 8 feet and under control at all times in any KC park area, except in designated off-leash areas. §7.12.225.E prohibits pets in designated swimming areas.',
       'https://aqua.kingcounty.gov/council/clerk/code/10_Title_7.htm',
       'Sand rule defaults to on-leash per §7.12.225.B (8-foot leash). Designated swim areas within a beach would be not_allowed under §7.12.225.E; per-beach refinement deferred. KC beaches inside incorporated cities (e.g. Seattle, Bellevue) may have stricter city ordinances overlaying this baseline; city-level Wave 3 refinement deferred.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
