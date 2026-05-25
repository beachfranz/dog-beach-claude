-- 20260524_harvest_top10_operator_seed.sql
--
-- Seed operator_location_sources with dog-park hub URLs for the
-- canonical operators we ALREADY have in `operator` (singular).
-- Per the proof-point harvest 2026-05-24: operator pages surface
-- dog parks OSM doesn't tag (4 confirmed LA gaps alone).
--
-- The 4 missing operators (San Jose, Davis, LARPD, San Ramon) need the
-- canonical agency hierarchy setup (operator.agency_id FK) before they
-- can be added via the standard path — deferred to a follow-up.

BEGIN;

INSERT INTO public.operator_location_sources
  (operator_id, content_type, source_url, shape, discovery_method, notes)
SELECT op.id, 'dog_park', src.url, src.shape::text, 'manual', src.notes
  FROM (VALUES
    ('City of San Diego',
     'https://www.sandiego.gov/park-and-recreation/parks/dogs/leashfree',
     'hub_subpages', 'Top-10 seed (39 OSM-attributed parks).'),
    ('City of Los Angeles',
     'https://www.laparks.org/dogparks',
     'hub_subpages', 'Top-10 seed (28 OSM-attributed parks). 4 confirmed OSM gaps in pilot.'),
    ('San Francisco, City and County of',
     'https://sfrecpark.org/457/Dog-Play-Areas',
     'hub_subpages', 'Top-10 seed (26 OSM-attributed parks).'),
    ('City of Sacramento',
     'http://www.cityofsacramento.org/parksandrec/parks/specialty-parks/dog-parks',
     'js_rendered', 'Top-10 seed (8 OSM-attributed parks). Static HTML empty; needs JS render.'),
    ('City of Irvine',
     'https://cityofirvine.org/parks-facilities/central-bark-dog-park',
     'single_entity_page', 'Top-10 seed (7 OSM-attributed parks). URL is single-park.'),
    ('San Diego County',
     'https://www.sdparks.org/content/sdparks/en/AboutUs/FAQs.html',
     'unknown', 'Top-10 seed (6 OSM-attributed parks). FAQ does not enumerate; needs alternate URL.')
  ) AS src(op_name, url, shape, notes)
JOIN public.operator op ON op.name = src.op_name
ON CONFLICT (operator_id, content_type, source_url) DO NOTHING;

-- Verify
SELECT op.name, ols.shape, ols.source_url
  FROM public.operator_location_sources ols
  JOIN public.operator op ON op.id = ols.operator_id
 WHERE ols.content_type = 'dog_park'
 ORDER BY op.name;

COMMIT;
