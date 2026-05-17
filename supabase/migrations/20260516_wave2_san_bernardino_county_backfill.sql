-- Phase B: San Bernardino County code backfill.
--
-- Discovered 2026-05-16 via Franz's pointer:
--   https://codelibrary.amlegal.com/codes/sanbernardino/latest/sanberncty_ca/0-0-0-157604
--
-- amlegal slug: `sanbernardino` (concatenated, no separator) — Phase A
-- discovery missed it because the script's AMLEGAL_SLUG_VARIANTS only tried
-- `san_bernardino` (snake) and `san_bernardino_county` (snake + suffix).
-- Script extended after this find to also try concatenated form.
--
-- Code structure:
--   TITLE 3: HEALTH AND SANITATION AND ANIMAL REGULATIONS
--     DIVISION 2: ANIMALS
--       CHAPTER 1: ANIMAL CONTROL
--         § 32.0108 Control of Animals (operative leash rule)
--
-- Operative rule (§32.0108(c), verbatim):
--   "No person may lawfully bring his or her dog out of his or her property
--   unless: (1) The dog is restrained by a leash and is in the charge of a
--   person competent to restrain the dog; or (2) The dog is properly
--   restrained and enclosed in a vehicle, cage or similar enclosure."
--
-- §32.0101 LEASH definition (6-foot max, verbatim):
--   "Any rope, leather strap, chain or other material not exceeding six
--   feet in length being held in the hand of a person capable of
--   controlling the animal to which it is attached."
--
-- §32.0108(a) "running at large" prohibition covers public parks, sidewalks,
-- and places of public assembly (the SB beaches are inland lake beaches at
-- Lake Arrowhead, Big Bear, Silverwood, etc., all in unincorporated SB
-- County — they qualify as "any other public place" under §32.0108(a)).
-- §32.0108(f) requires feces/urine cleanup on public sidewalks (does not
-- enumerate beaches by name).
-- §32.0108(d): dogs found running at large may be impounded for 96+ hours.
--
-- 15 SB County beaches in beaches_gold (inland lake beaches — SB has no
-- coastline; beaches are at Lake Arrowhead, Big Bear, Silverwood, etc.).

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'San Bernardino County Code §32.0108 (Control of Animals) — Title 3 / Division 2 / Chapter 1',
       (SELECT id FROM public.agency WHERE name='San Bernardino County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://codelibrary.amlegal.com/codes/sanbernardino/latest/sanberncty_ca/0-0-0-157604',
       'San Bernardino County Code §32.0108 Control of Animals. (a) No person owning or having control of any animal shall permit such animal to stray, to run at large upon any private or public street, sidewalk, school ground, public park, playground, place of public assembly or any other public place or upon any unenclosed private lot or other unenclosed private place or upon any private property without the consent of the owner or person in control thereof. (c) No person may lawfully bring his or her dog out of his or her property unless: (1) The dog is restrained by a leash and is in the charge of a person competent to restrain the dog; or (2) The dog is properly restrained and enclosed in a vehicle, cage or similar enclosure. (d) Any dog found running at large, running loose or unrestrained may be impounded for a period of not less than 96 hours. (f) A person having custody of any dog shall not permit any such dog to defecate or urinate upon a public street or sidewalk. §32.0101 LEASH definition: any rope, leather strap, chain or other material not exceeding six feet in length being held in the hand of a person capable of controlling the animal to which it is attached. [Hosted on amlegal at codelibrary.amlegal.com/codes/sanbernardino/. Fetched verbatim via Playwright + PDF cross-check 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'San Bernardino County Code §32.0108%'
);

-- ─── 2. Link SB County beaches ───────────────────────────────────

WITH sb_id AS (
  SELECT id FROM public.agency WHERE name='San Bernardino County' AND type='county'
), targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  LEFT JOIN public.beach_agency ba_dp
    ON ba_dp.beach_fid = g.fid AND ba_dp.authority_domain = 'dog_policy' AND ba_dp.precedence_rank = 1
  WHERE g.state='CA' AND g.is_active=true AND g.county_name='San Bernardino'
    AND (ba_dp.agency_id = (SELECT id FROM sb_id) OR ba_dp.agency_id IS NULL)
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'SB County Code §32.0108(c): no person may bring a dog out of property unless the dog is restrained by a leash (per §32.0101: max 6 feet, held in hand of a person capable of controlling the animal) and is in the charge of a person competent to restrain the dog. §32.0108(a) prohibits running at large in public places including parks.',
       ps.source_url,
       NULL
FROM targets t
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'San Bernardino County Code §32.0108%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
