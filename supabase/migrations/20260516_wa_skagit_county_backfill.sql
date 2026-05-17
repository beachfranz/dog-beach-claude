-- Wave 4: Skagit County (WA) parks dog-policy backfill.
--
-- Source: Skagit County Code §9.41 (Parks and Recreation Code) —
-- specifically the dog/pet leash, off-leash, and swim-area sections.
--
-- Live URL: https://www.codepublishing.com/WA/SkagitCounty/html/SkagitCounty09/SkagitCounty0941.html
--
-- Code structure:
--   Title 7 (Animals) — animal control framework, licensing, dangerous
--     dogs, etc. — NO countywide leash-on-public-property rule.
--   Title 9 / Chapter 9.41 Parks and Recreation Code — the PARKS-
--     SPECIFIC dog rule. §9.41 (subsections 1-4 in the Animals
--     subsection) is the operative rule for county park dogs.
--
-- Operative rule (§9.41 Animals subsection, verbatim from
-- Code Publishing 2026-05-16):
--   (1) All dogs or other pets must be physically controlled by a
--   leash no greater than eight (8) feet in length while in a County
--   park, except in areas designated and posted by the Department as
--   "off leash pet areas."
--   (2) Any person whose dog or other pet is in any County park shall
--   be strictly liable for the conduct of the animal.
--   (4) Dogs, pets or domestic animals are not permitted on any
--   designated swimming beach, or play area in any County park unless
--   specifically permitted by posting. This Section shall not apply
--   to guide animals.
--
-- Sand rule = on_leash (8-foot leash max in county parks).
-- Designated swim portions = not_allowed; per-beach refinement
-- deferred.
--
-- 24 Skagit County beaches in beaches_gold.

-- ─── 1. Insert policy_source ─────────────────────────────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Skagit County Code §9.41 (Parks and Recreation Code — Animals) — Title 9 / Chapter 9.41',
       (SELECT id FROM public.agency WHERE name='Skagit County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://www.codepublishing.com/WA/SkagitCounty/html/SkagitCounty09/SkagitCounty0941.html',
       'Skagit County Code §9.41 (Animals subsection of Parks and Recreation Code). (1) All dogs or other pets must be physically controlled by a leash no greater than eight (8) feet in length while in a County park, except in areas designated and posted by the Department as "off leash pet areas." (2) Any person whose dog or other pet is in any County park shall be strictly liable for the conduct of the animal. (4) Dogs, pets or domestic animals are not permitted on any designated swimming beach, or play area in any County park unless specifically permitted by posting. This Section shall not apply to guide animals. Definitions §9.41 (17): "Domestic animals" means dogs, cats and other small, domesticated animals. [Title 7 SkCC covers animal control / licensing / dangerous dogs separately and has no countywide leash law; §9.41 is the operative park-and-beach rule. Fetched via curl 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Skagit County Code §9.41%'
);

-- ─── 2. Link Skagit County beaches ──────────────────────────────

WITH targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  WHERE g.state='WA' AND g.is_active=true AND g.county_name='Skagit'
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Skagit County Code §9.41%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Skagit County Code §9.41(1): 8-foot max leash in any county park unless designated as off-leash. §9.41(4) prohibits dogs on designated swimming beach or play area unless permitted by posting.',
       'https://www.codepublishing.com/WA/SkagitCounty/html/SkagitCounty09/SkagitCounty0941.html',
       'Sand rule = on_leash per §9.41(1). Designated swim portions = not_allowed under §9.41(4); per-beach refinement deferred.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
