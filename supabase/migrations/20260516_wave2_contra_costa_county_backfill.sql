-- Phase B: Contra Costa County code backfill.
--
-- Discovered 2026-05-16 via Franz's pointer:
--   https://library.municode.com/ca/contra_costa_county/codes/ordinance_code?nodeId=TIT4HESA_DIV416AN_CH416-4GEPR
--
-- Platform: Municode. Phase A discovery missed Contra Costa because the
-- script's MUNICODE_DOC_SLUGS list only tries `code_of_ordinances`,
-- `municipal_code`, `code` — not `ordinance_code`. Script extended after
-- this find to also try `ordinance_code`.
--
-- Code structure:
--   Title 4: HEALTH AND SAFETY
--     Division 416: ANIMALS
--       Chapter 416-4: GENERAL PROVISIONS
--         Article 416-4.4: RESTRAINT
--           § 416-4.402 Animals at large (operative)
--           § 416-4.404 Abandonment
--           § 416-4.406 Prohibition
--
-- Operative rule (§416-4.402 Animals at large, verbatim):
--   "(a) No person owning, possessing, harboring, or controlling an animal
--   may allow the animal to be at large.
--   (b) As used in this section, an 'at large' animal means any of the
--   following:
--   (1) A dog that is on public property or common areas of private
--   property and is not under effective restraint of a leash. A dog is not
--   'under effective restraint of a leash' if the leash: (A) is not made
--   of sufficient material and strength to restrain the dog; (B) is longer
--   than six feet; (C) is extendable or retractable; or (D) is not held in
--   a manner that prevents the dog from being at large or causing injury to
--   another animal or person. A dog is not required to be under restraint
--   by a leash if the dog is on private property owned by, or in the
--   possession of, the person owning or controlling the dog.
--   (2) An animal other than a dog or a cat that is not in the immediate
--   presence and under the effective control of a person.
--   (3) An animal that is tethered or leashed: (A) for longer than fifteen
--   minutes on any street or other public place not set aside for
--   tethering or leashing; or (B) in such a way as to block a public
--   walkway or thoroughfare."
--
-- Notable: Contra Costa's definition of "effective restraint" explicitly
-- excludes extendable/retractable leashes AND caps leash length at six feet.
-- This is more specific than most CA counties' leash rules.
--
-- 3 Contra Costa County beaches in beaches_gold (Bay/delta).

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Contra Costa County Ordinance Code §416-4.402 (Animals at large) — Title 4 / Div 416 / Ch 416-4 / Art 416-4.4',
       (SELECT id FROM public.agency WHERE name='Contra Costa County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/ca/contra_costa_county/codes/ordinance_code?nodeId=TIT4HESA_DIV416AN_CH416-4GEPR_ART416-4.4RE',
       'Contra Costa County Ordinance Code §416-4.402 Animals at large. (a) No person owning, possessing, harboring, or controlling an animal may allow the animal to be at large. (b) An "at large" animal means: (1) A dog that is on public property or common areas of private property and is not under effective restraint of a leash. A dog is not under effective restraint of a leash if the leash: (A) is not made of sufficient material and strength to restrain the dog; (B) is longer than six feet; (C) is extendable or retractable; or (D) is not held in a manner that prevents the dog from being at large or causing injury. A dog is not required to be under restraint by a leash if on private property owned by the person controlling the dog. (2) An animal other than a dog or cat not in the immediate presence and under effective control of a person. (3) An animal tethered/leashed: (A) longer than fifteen minutes on any street or public place not set aside for tethering/leashing; or (B) in such a way as to block a public walkway. [Hosted on Municode at library.municode.com/ca/contra_costa_county/codes/ordinance_code. Note: doc-slug is `ordinance_code`, not the more common `code_of_ordinances`. Fetched verbatim via Playwright 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Contra Costa County Ordinance Code §416-4.402%'
);

-- ─── 2. Link Contra Costa County beaches ────────────────────────

WITH cc_id AS (
  SELECT id FROM public.agency WHERE name='Contra Costa County' AND type='county'
), targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  LEFT JOIN public.beach_agency ba_dp
    ON ba_dp.beach_fid = g.fid AND ba_dp.authority_domain = 'dog_policy' AND ba_dp.precedence_rank = 1
  WHERE g.state='CA' AND g.is_active=true AND g.county_name='Contra Costa'
    AND (ba_dp.agency_id = (SELECT id FROM cc_id) OR ba_dp.agency_id IS NULL)
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Contra Costa County §416-4.402(b)(1): a dog on public property is "at large" if not under effective restraint of a leash; leash must be ≤6 feet, not extendable/retractable, and held competently. Allowing at large is prohibited per (a).',
       ps.source_url,
       NULL
FROM targets t
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Contra Costa County Ordinance Code §416-4.402%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
