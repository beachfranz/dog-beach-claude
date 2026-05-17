-- Phase B: Sacramento County code backfill.
--
-- Discovered 2026-05-16 via Franz's pointer:
--   https://ecode360.com/44030277
--
-- Platform: ecode360 (General Code) — same family as humboldt.county.codes
-- but a different subdomain convention. Phase A discovery missed Sacramento
-- County because the script doesn't probe ecode360 (only Municode + amlegal
-- + qcode). Only Sacramento County on ecode360 among CA county unknowns —
-- below the threshold for wiring ecode360 as a discovery fallback.
--
-- Code structure:
--   Volume II / Title 8 Animals / Chapter 8.08 GENERAL PROVISIONS
--     § 8.08.050 Prohibited Conduct (no animal at large, excepting cat)
--     § 8.08.056 Dogs at Large (operative leash rule — 8-foot max)
--
-- Operative rule (§8.08.056 Dogs at Large, verbatim):
--   "No person shall permit or suffer a dog to stray from private property
--   owned or legally possessed by the dog owner or the person who has a
--   right to control the dog unless the dog is restrained by a leash or
--   lead not exceeding eight feet in length, except in the following
--   situations:
--   1. When the dog is assisting a peace officer who is engaged in law
--      enforcement duties or when the dog is participating in a search and
--      rescue effort at the specific request of a law enforcement authority;
--   2. When the dog is enrolled in and actually participating in a dog
--      training or obedience course, exhibition, or competition conducted
--      by an organization on private or public property with the
--      permission of the owner or operator of the grounds or facilities;
--   3. When the dog is assisting the owner or person in charge of livestock
--      in the herding or control of such livestock; or
--   4. When the dog is accompanying and under the direction of a person
--      engaged in hunting on land which is within a restricted shooting
--      district as defined in Section 9.40.060 of the Sacramento County
--      Code."
--
-- §8.08.050(1) Prohibited Conduct also bars any animal (excepting cat)
-- from being "at large".
--
-- 8 Sacramento County beaches in beaches_gold (American/Sacramento river,
-- delta — no ocean coast).

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Sacramento County Code §8.08.056 (Dogs at Large) + §8.08.050 (Prohibited Conduct) — Title 8 / Chapter 8.08',
       (SELECT id FROM public.agency WHERE name='Sacramento County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/44030277',
       'Sacramento County Code Title 8 (Animals), Chapter 8.08 General Provisions. §8.08.056 Dogs at Large: No person shall permit or suffer a dog to stray from private property owned or legally possessed by the dog owner or the person who has a right to control the dog unless the dog is restrained by a leash or lead not exceeding eight feet in length, except in the following situations: (1) law enforcement / search and rescue dog assisting at specific request; (2) dog enrolled and actively participating in training/obedience/exhibition; (3) dog assisting owner in herding livestock; (4) dog accompanying hunter within a restricted shooting district per §9.40.060. §8.08.050 Prohibited Conduct: No owner of any animal shall permit it to (1) be at large (excepting the domestic cat); (2) bite/scratch/claw; (3) make disturbing noise; (4) constitute an animal nuisance; (5) endanger life/health; (6) damage property; (7) be untreated for zoonotic disease. [Hosted on ecode360 at ecode360.com/44030277. Fetched verbatim via Playwright 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Sacramento County Code §8.08.056%'
);

-- ─── 2. Link Sacramento County beaches ──────────────────────────

WITH sac_id AS (
  SELECT id FROM public.agency WHERE name='Sacramento County' AND type='county'
), targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  LEFT JOIN public.beach_agency ba_dp
    ON ba_dp.beach_fid = g.fid AND ba_dp.authority_domain = 'dog_policy' AND ba_dp.precedence_rank = 1
  WHERE g.state='CA' AND g.is_active=true AND g.county_name='Sacramento'
    AND (ba_dp.agency_id = (SELECT id FROM sac_id) OR ba_dp.agency_id IS NULL)
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Sacramento County Code §8.08.056: dog must be restrained by a leash or lead not exceeding eight feet in length when off owner''s private property, with narrow LE/training/herding/hunting exceptions. §8.08.050(1) also prohibits any animal (except domestic cat) from being at large.',
       ps.source_url,
       NULL
FROM targets t
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Sacramento County Code §8.08.056%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
