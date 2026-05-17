-- Wave 4: Kitsap County (WA) parks dog-policy backfill.
--
-- Source: Kitsap County Code §10.12.070 (Animals in playfields or
-- parks) — Title 10 / Chapter 10.12 PARK CODE.
--
-- IMPORTANT — Kitsap County's Title 7 (Animals) covers licensing,
-- dangerous dogs, stock-restricted areas; it does NOT contain a
-- countywide leash-on-public-property rule. The operative dog rule
-- for KC parks/beaches is the PARK CODE at §10.12.070.
--
-- Live URL: https://www.codepublishing.com/WA/KitsapCounty/html/Kitsap10/Kitsap1012.html
--
-- §10.12.020(1) defines "Park" as "any park, playground, beach,
-- recreation center or any other area in Kitsap County owned or used
-- by the county under the supervision of the park director" —
-- so the §10.12.070 rule explicitly reaches county beaches.
--
-- Operative rule (§10.12.070, verbatim):
--   It is unlawful to allow or permit a horse, dog or other domestic
--   animal in any designated playfield area, swimming area or picnic
--   area or to enter any lake, pond, fountain or stream therein or to
--   run at large in any area of any park.
--
-- This is a PROHIBITION on dogs in (a) swimming areas, (b) playfields,
-- (c) picnic areas, (d) any water within a park, AND a prohibition on
-- "run at large" anywhere in any park. There is no leash-permitted
-- carve-out in the verbatim text — dogs are either restrained (not at
-- large) or prohibited from the sub-areas listed.
--
-- Sand rule = on_leash. Logic: a leashed dog on a non-swim-area
-- sandy beach is NOT "at large" and is NOT in a "designated swimming
-- area or picnic area." The §10.12.070 prohibition narrowly forbids
-- (a) the at-large state and (b) being in swim/picnic/water areas.
-- A leashed walk along the dry-sand portion of a county beach
-- satisfies the rule. Designated swim portions of the beach would
-- be `not_allowed`; per-beach refinement deferred.
--
-- Violation = misdemeanor per §10.12.190.
--
-- 45 Kitsap County beaches in beaches_gold (Bremerton peninsula +
-- Bainbridge Island + Hood Canal east shore).

-- ─── 1. Insert policy_source ─────────────────────────────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Kitsap County Code §10.12.070 (Animals in playfields or parks) — Title 10 / Chapter 10.12 PARK CODE',
       (SELECT id FROM public.agency WHERE name='Kitsap County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://www.codepublishing.com/WA/KitsapCounty/html/Kitsap10/Kitsap1012.html',
       'Kitsap County Code §10.12.070 Animals in playfields or parks. It is unlawful to allow or permit a horse, dog or other domestic animal in any designated playfield area, swimming area or picnic area or to enter any lake, pond, fountain or stream therein or to run at large in any area of any park. §10.12.020(1) defines "Park" as "any park, playground, beach, recreation center or any other area in Kitsap County owned or used by the county under the supervision of the park director." Violation = misdemeanor per §10.12.190. [Title 7 KCC (Animals) covers licensing/dangerous dogs separately and contains no countywide leash law; §10.12.070 is the operative park-and-beach rule. Fetched via curl 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Kitsap County Code §10.12.070%'
);

-- ─── 2. Link Kitsap County beaches ───────────────────────────────

WITH targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  WHERE g.state='WA' AND g.is_active=true AND g.county_name='Kitsap'
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Kitsap County Code §10.12.070%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Kitsap County Code §10.12.070: dogs cannot run at large in any county park; cannot be in designated swimming areas, playfields, picnic areas, or enter park waters. "Park" includes beaches per §10.12.020(1). Leashed walk on non-swim dry sand satisfies the rule.',
       'https://www.codepublishing.com/WA/KitsapCounty/html/Kitsap10/Kitsap1012.html',
       'Sand rule defaults to on-leash per §10.12.070 + §10.12.020(1). Designated swim portions would be not_allowed; per-beach refinement deferred.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
