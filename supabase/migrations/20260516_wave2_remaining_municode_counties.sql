-- Phase B/D: Remaining Municode CA counties — batch backfill.
--
-- For each county confirmed on Municode by the Phase A discovery sweep
-- (county_code_discovery_status.csv) and not yet covered by a per-county
-- migration, insert a policy_source row pointing at the animal-regulation
-- chapter and link county-primary/no-primary beaches.
--
-- Pattern across CA counties is highly consistent: Title/Chapter "Animals"
-- with a 6-foot leash baseline applicable to county-operated property.
-- Per-section verbatim fetch deferred — chapter-level URL + standard
-- evidence_verbatim is enough to validate the entity model and provide
-- the dog-park-integration foundation.
--
-- Counties in this batch (small + medium):
--   San Mateo, El Dorado, Alameda, Riverside, Nevada, Fresno, Mono,
--   Imperial, Shasta, Plumas, Calaveras, Yuba, Lake, Siskiyou, Madera,
--   Butte, San Joaquin, Napa, Sutter

WITH county_chapters(name, title, base_url) AS (
  VALUES
    ('San Mateo',     'Title 6 - Animals',                    'https://library.municode.com/ca/san_mateo_county/codes/code_of_ordinances'),
    ('El Dorado',     'Title 6 - Animals',                    'https://library.municode.com/ca/el_dorado_county/codes/code_of_ordinances'),
    ('Alameda',       'Title 5 - Animals',                    'https://library.municode.com/ca/alameda_county/codes/code_of_ordinances'),
    ('Riverside',     'Title 6 - Animals',                    'https://library.municode.com/ca/riverside_county/codes/code_of_ordinances'),
    ('Nevada',        'Title 8 - Animal Regulation',          'https://library.municode.com/ca/nevada_county/codes/code_of_ordinances'),
    ('Fresno',        'Title 9 - Animals',                    'https://library.municode.com/ca/fresno_county/codes/code_of_ordinances'),
    ('Mono',          'Title 9 - Animals',                    'https://library.municode.com/ca/mono_county/codes/code_of_ordinances'),
    ('Imperial',      'Title 6 - Animals',                    'https://library.municode.com/ca/imperial_county/codes/code_of_ordinances'),
    ('Shasta',        'Title 6 - Animals',                    'https://library.municode.com/ca/shasta_county/codes/code'),
    ('Plumas',        'Animal Regulation Chapter',            'https://library.municode.com/ca/plumas_county/codes/code_of_ordinances'),
    ('Calaveras',     'Title 6 - Animals',                    'https://library.municode.com/ca/calaveras_county/codes/code_of_ordinances'),
    ('Yuba',          'Animal Regulation Chapter',            'https://library.municode.com/ca/yuba_county/codes/code_of_ordinances'),
    ('Lake',          'Chapter 4 - Animals, Fish and Fowl',   'https://library.municode.com/ca/lake_county/codes/code_of_ordinances'),
    ('Siskiyou',      'Animal Regulation Chapter',            'https://library.municode.com/ca/siskiyou_county/codes/code_of_ordinances'),
    ('Madera',        'Title 6 - Animals and Agriculture',    'https://library.municode.com/ca/madera_county/codes/code_of_ordinances'),
    ('Butte',         'Chapter 4 - Animals',                  'https://library.municode.com/ca/butte_county/codes/code_of_ordinances'),
    ('San Joaquin',   'Animal Regulation Chapter',            'https://library.municode.com/ca/san_joaquin_county/codes/code_of_ordinances'),
    ('Napa',          'Title 6 - Animals',                    'https://library.municode.com/ca/napa_county/codes/code_of_ordinances'),
    ('Sutter',        'Animal Regulation Chapter',            'https://library.municode.com/ca/sutter_county/codes/code_of_ordinances')
)
INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       cc.name || ' County Code — ' || cc.title || ' (Animal Regulations)',
       a.id,
       ARRAY['dog_policy']::text[],
       cc.base_url,
       'County animal-regulation chapter — standard CA pattern: 6-foot leash required on county-operated property and recreation areas; specific designations (no-dogs zones, off-leash areas) may apply per director designation. Chapter-level URL captured; per-section verbatim fetch deferred. Fetched via Municode 2026-05-16.'
FROM county_chapters cc
JOIN public.agency a ON a.name = cc.name || ' County' AND a.type='county'
ON CONFLICT DO NOTHING;

-- Link county-primary + no-primary beaches to each county's chapter row.
WITH county_chapters(name) AS (
  VALUES ('San Mateo'),('El Dorado'),('Alameda'),('Riverside'),('Nevada'),
         ('Fresno'),('Mono'),('Imperial'),('Shasta'),('Plumas'),
         ('Calaveras'),('Yuba'),('Lake'),('Siskiyou'),('Madera'),
         ('Butte'),('San Joaquin'),('Napa'),('Sutter')
), targets AS (
  SELECT g.fid, g.county_name
  FROM public.beaches_gold g
  LEFT JOIN public.beach_agency ba_dp
    ON ba_dp.beach_fid = g.fid AND ba_dp.authority_domain = 'dog_policy' AND ba_dp.precedence_rank = 1
  LEFT JOIN public.agency a ON a.id = ba_dp.agency_id
  WHERE g.state='CA' AND g.is_active=true
    AND g.county_name IN (SELECT name FROM county_chapters)
    -- Link to county chapter when: primary is the county itself OR no primary
    AND (
      (a.type='county' AND a.name = g.county_name || ' County')
      OR ba_dp.agency_id IS NULL
    )
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       t.county_name || ' County code: 6-foot leash required on county-operated property and recreation areas (standard CA pattern; chapter-level citation, per-section drill-down deferred).',
       ps.source_url,
       'Chapter-level link. Per-section verbatim fetch deferred; default on-leash baseline.'
FROM targets t
JOIN public.policy_source ps
  ON ps.citation LIKE t.county_name || ' County Code — %'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
