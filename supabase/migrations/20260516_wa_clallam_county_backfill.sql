-- Wave 4: Clallam County (WA) parks dog-policy backfill.
--
-- Source: Clallam County Code §23.03.070 (Animals) — Title 23 /
-- Chapter 23.03 General Park and Fairgrounds Rules and Regulations.
--
-- Live URL: https://clallam.county.codes/CCC/23.03.070
-- (Hosted on county.codes / General Code platform.)
--
-- Code structure:
--   Title 17 / Chapter 17.02 Dogs and Cats — countywide animal
--     control (licensing, leash control areas, dog at large
--     violations). §17.02.010 designates all of unincorporated
--     Clallam County (except national parks and tribal lands) as a
--     "dog control zone" per RCW 16.10.
--   Title 23 / Chapter 23.03 General Park and Fairgrounds Rules —
--     §23.03.070 is the PARKS-SPECIFIC rule with explicit leash
--     length, swimming beach prohibition, and service-animal carve-out.
--
-- Operative rule for parks/beaches (§23.03.070, verbatim):
--   (1) Dogs, cats, and other pets, including livestock, are
--   prohibited unless crated, caged, or on a leash, bridle or halter
--   of not more than eight feet in length, and/or otherwise under
--   physical restrictive control at all times when inside park lands.
--   (2) Dogs, cats, horses, and other pets are not permitted on any
--   designated swimming beach or in any park building unless so
--   authorized by the Director. This subsection does not apply to
--   service animals or law enforcement dogs.
--   (3) Grazing or ranging of domestic animals, livestock, or
--   poultry is prohibited on all park lands, with the exception of
--   designated areas within Robin Hill Farm County Park and the
--   Clallam County Fairgrounds.
--
-- Sand rule = on_leash (8-foot max in park lands).
-- Designated swim portions = not_allowed under §23.03.070(2);
-- per-beach refinement deferred.
--
-- 33 Clallam County beaches in beaches_gold (NW Olympic peninsula,
-- Strait of Juan de Fuca shore).

-- ─── 1. Insert policy_source ─────────────────────────────────────

INSERT INTO public.policy_source
  (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Clallam County Code §23.03.070 (Animals) — Title 23 / Chapter 23.03 General Park and Fairgrounds Rules and Regulations',
       (SELECT id FROM public.agency WHERE name='Clallam County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://clallam.county.codes/CCC/23.03.070',
       'Clallam County Code §23.03.070 Animals. (1) Dogs, cats, and other pets, including livestock, are prohibited unless crated, caged, or on a leash, bridle or halter of not more than eight feet in length, and/or otherwise under physical restrictive control at all times when inside park lands. (2) Dogs, cats, horses, and other pets are not permitted on any designated swimming beach or in any park building unless so authorized by the Director. This subsection does not apply to service animals or law enforcement dogs. (3) Grazing or ranging of domestic animals, livestock, or poultry is prohibited on all park lands, with the exception of designated areas within Robin Hill Farm County Park and the Clallam County Fairgrounds. [Title 17 Chapter 17.02 covers countywide dog control / licensing separately; §23.03.070 is the operative park-and-beach rule. Hosted on county.codes (General Code). Fetched via Playwright 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Clallam County Code §23.03.070%'
);

-- ─── 2. Link Clallam County beaches ─────────────────────────────

WITH targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  WHERE g.state='WA' AND g.is_active=true AND g.county_name='Clallam'
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Clallam County Code §23.03.070%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Clallam County Code §23.03.070(1): max 8-foot leash on park lands. §23.03.070(2) prohibits pets on designated swimming beaches (service/LE dogs exempt).',
       'https://clallam.county.codes/CCC/23.03.070',
       'Sand rule = on_leash per §23.03.070(1) for county park lands. Designated swim portions = not_allowed under §23.03.070(2); per-beach refinement deferred. Many Clallam beaches are within Olympic National Park (federal NPS rule controls — already linked via Wave 2 NPS backfill); per-beach jurisdiction refinement deferred.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
