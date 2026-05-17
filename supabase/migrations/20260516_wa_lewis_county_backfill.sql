-- Wave 4: Lewis County (WA) parks dog-policy backfill.
--
-- Source: Lewis County Code §12.05.110 (Animals — County Park Use)
-- — Title 12 / Chapter 12.05 County Park Use.
--
-- Live URL: https://www.codepublishing.com/WA/LewisCounty/html/LewisCounty12/LewisCounty1205.html
--
-- Operative rule (§12.05.110(1), verbatim via Playwright 2026-05-16):
--   Small Animals. It is unlawful for any small animal to be allowed
--   to run free in any park. The animal must be leashed at all times
--   and the leash in control of a responsible individual of suitable
--   age, discretion and capability to control the animal. The leash
--   can be no longer than 10 feet. In no event shall any small animal
--   be permitted on any bathing beach or in water adjacent thereto. A
--   "guide dog" or "service dog," as those terms are utilized in
--   Chapter 70.84 RCW, or a police dog on duty are excepted from
--   these regulations.
--
-- §12.05 definitions:
--   (6) "Small animals" means dogs, cats and other small,
--       domesticated animals.
--   (8) "Park" means Lewis County park.
--
-- Sand rule = on_leash (10-foot max). Designated bathing beach +
-- adjacent water = not_allowed under §12.05.110(1).
--
-- Title 6 / Chapter 6.05 (Animals) covers countywide animal control
-- but is hosted on ecode360 (LE4740) after migration from
-- codepublishing.com. For parks/beaches the operative rule is
-- §12.05.110.
--
-- 2 Lewis County beaches in beaches_gold.

-- ─── 1. Insert policy_source ─────────────────────────────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Lewis County Code §12.05.110 (Animals — County Park Use) — Title 12 / Chapter 12.05',
       (SELECT id FROM public.agency WHERE name='Lewis County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://www.codepublishing.com/WA/LewisCounty/html/LewisCounty12/LewisCounty1205.html',
       'Lewis County Code §12.05.110 Animals. (1) Small Animals. It is unlawful for any small animal to be allowed to run free in any park. The animal must be leashed at all times and the leash in control of a responsible individual of suitable age, discretion and capability to control the animal. The leash can be no longer than 10 feet. In no event shall any small animal be permitted on any bathing beach or in water adjacent thereto. A "guide dog" or "service dog," as those terms are utilized in Chapter 70.84 RCW, or a police dog on duty are excepted from these regulations. (2) Large Animals. It is unlawful to have any large animal outside of the designated parking area. (3) Removal of Animal Waste Products. (Ord. 1130 § 11, 1993; Ord. 1157, 1998). §12.05 definitions: "Small animals" means dogs, cats and other small, domesticated animals; "Park" means Lewis County park. [Title 6 Chapter 6.05 (Animals) on ecode360 LE4740 handles countywide animal control; §12.05.110 is the operative park-and-beach rule. Fetched via Playwright 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Lewis County Code §12.05.110%'
);

-- ─── 2. Link Lewis County beaches ───────────────────────────────

WITH targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  WHERE g.state='WA' AND g.is_active=true AND g.county_name='Lewis'
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Lewis County Code §12.05.110%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Lewis County Code §12.05.110(1): in any Lewis County park, small animals must be leashed at all times; max 10 feet. Bathing beach + adjacent water explicitly prohibited.',
       'https://www.codepublishing.com/WA/LewisCounty/html/LewisCounty12/LewisCounty1205.html',
       'Sand rule = on_leash per §12.05.110(1). Designated bathing beach = not_allowed; per-beach refinement deferred.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
