-- Phase B: Mendocino + Ventura county code backfill.
--
-- Mendocino County Code Title 10 Ch 10.04 (Animal Regulation Ordinance):
--   Defines "at-large" — leash not exceeding 6 feet; allows
--   designated off-leash areas set aside by the County.
--
-- Ventura County Code Division 4 Ch 4 §§4461-4466:
--   §4461 Leash law
--   §4462 Animals at large
--   §4466 Animals on public beaches, sidewalks, parks, school grounds
--         or County property — directly addresses beach access

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
VALUES
  ('municipal_code',
   'Mendocino County Code Ch 10.04 (Animal Regulation Ordinance)',
   (SELECT id FROM public.agency WHERE name='Mendocino County' AND type='county'),
   ARRAY['dog_policy']::text[],
   'https://library.municode.com/ca/mendocino_county/codes/code_of_ordinances?nodeId=MECOCO_TIT10AN',
   '"At large." Any animal, excepting a working animal, shall be deemed to be "at large" when off the premises of the owner and not under restraint by leash or physical control of its owner. "Leash" means any rope, leather strap, chain, or other material not exceeding six (6) feet in length being held in the hand of the person capable of controlling the animal to which it is attached. To allow a dog on public property without a leash not exceeding six (6) feet in length. To comply with this section, the person in charge of the dog must have physical control of the leash. Dogs may be permitted to run without a leash in designated areas which, from time to time, may be set aside by the County for the specific purpose of exercising a dog... [Mendocino County Code Title 10 Ch 10.04. Fetched via Municode 2026-05-16.]'),
  ('municipal_code',
   'Ventura County Code §4461 + §4466 (Leash law + Animals on public beaches)',
   (SELECT id FROM public.agency WHERE name='Ventura County' AND type='county'),
   ARRAY['dog_policy']::text[],
   'https://library.municode.com/ca/ventura_county/codes/code_of_ordinances?nodeId=DIV4PUHE',
   '§4461 Leash law. §4462 Animals at large. §4466 Animals on public beaches, sidewalks, parks, school grounds or County property — directly addresses beach access. [Ventura County Code Division 4 Public Health, Chapter 4 Animals. Section text not fully captured in this migration pass; deferred to per-section drill-down. Fetched via Municode 2026-05-16.]')
ON CONFLICT DO NOTHING;

-- Mendocino link
WITH mc_id AS (
  SELECT id FROM public.agency WHERE name='Mendocino County' AND type='county'
), targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  LEFT JOIN public.beach_agency ba_dp
    ON ba_dp.beach_fid = g.fid AND ba_dp.authority_domain = 'dog_policy' AND ba_dp.precedence_rank = 1
  WHERE g.state='CA' AND g.is_active=true AND g.county_name='Mendocino'
    AND (ba_dp.agency_id = (SELECT id FROM mc_id) OR ba_dp.agency_id IS NULL)
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Mendocino County Code Ch 10.04%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Mendocino County Code Ch 10.04: 6-foot leash required on public property; County may designate off-leash exercise areas.',
       'https://library.municode.com/ca/mendocino_county/codes/code_of_ordinances?nodeId=MECOCO_TIT10AN'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Ventura link
WITH vc_id AS (
  SELECT id FROM public.agency WHERE name='Ventura County' AND type='county'
), targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  LEFT JOIN public.beach_agency ba_dp
    ON ba_dp.beach_fid = g.fid AND ba_dp.authority_domain = 'dog_policy' AND ba_dp.precedence_rank = 1
  WHERE g.state='CA' AND g.is_active=true AND g.county_name='Ventura'
    AND (ba_dp.agency_id = (SELECT id FROM vc_id) OR ba_dp.agency_id IS NULL)
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Ventura County Code §4461%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Ventura County Code §4461 Leash law + §4466 Animals on public beaches, sidewalks, parks. Full section text fetch deferred.',
       'https://library.municode.com/ca/ventura_county/codes/code_of_ordinances?nodeId=DIV4PUHE',
       '§4466 specifically addresses beach access — per-beach rule refinement (rule=not_allowed if §4466 prohibits) deferred pending section text capture.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
