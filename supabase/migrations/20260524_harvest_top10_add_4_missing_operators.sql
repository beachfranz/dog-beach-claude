-- 20260524_harvest_top10_add_4_missing_operators.sql
--
-- Add the 4 operators missing from the top-10 dog-park scaling:
--   San Jose, Davis, San Ramon (city operators)
--   Livermore Area Recreation and Park District (special district)
--
-- These fail chk_operator_gov_or_delegated unless agency_id is set.
-- Existing CA city operators link to public.agency rows of type='city'
-- with the bare city name. Pattern observed:
--   City of Irvine (op 48)        → agency 86 'Irvine'
--   City of Los Angeles (op 53)   → agency 66 'Los Angeles'
--   City of Sacramento (op 74)    → agency 57 'Sacramento'
--   City of San Diego (op 77)     → agency 101 'San Diego'
--
-- For LARPD: it's a special district covering Livermore. We link it
-- to a Livermore agency entry (same agency name; different operator).
--
-- After this migration: 4 new operator rows + 4 new operator_location_sources
-- rows. Top-10 harvest will then have full coverage.

BEGIN;

-- ── Step 1: agency rows (idempotent via NOT EXISTS) ──────────────────

INSERT INTO public.agency (name, type)
SELECT v.name, v.type::public.agency_type
FROM (VALUES
  ('San Jose',   'city'),
  ('Davis',      'city'),
  ('San Ramon',  'city'),
  ('Livermore',  'city')
) AS v(name, type)
WHERE NOT EXISTS (
  SELECT 1 FROM public.agency a
   WHERE LOWER(a.name) = LOWER(v.name) AND a.type::text = v.type
);

-- ── Step 2: operator rows (link to the agencies above) ───────────────

INSERT INTO public.operator (name, type, short_name, agency_id, web_url, metadata)
SELECT
  v.op_name,
  v.op_type::public.operator_type,
  v.short_name,
  a.id,
  v.web_url,
  v.metadata::jsonb
FROM (VALUES
  ('City of San Jose',    'city',             'San Jose',  'San Jose',  'https://www.sanjoseca.gov/',     '{"added_for": "harvest_top10"}'),
  ('City of Davis',       'city',             'Davis',     'Davis',     'https://www.cityofdavis.org/',   '{"added_for": "harvest_top10"}'),
  ('City of San Ramon',   'city',             'San Ramon', 'San Ramon', 'https://www.sanramon.ca.gov/',   '{"added_for": "harvest_top10"}'),
  ('Livermore Area Recreation and Park District',
                          'special_district', 'LARPD',     'Livermore', 'https://www.larpd.org/',         '{"added_for": "harvest_top10", "covers_city": "Livermore"}')
) AS v(op_name, op_type, short_name, agency_name, web_url, metadata)
JOIN public.agency a ON LOWER(a.name) = LOWER(v.agency_name)
WHERE NOT EXISTS (
  SELECT 1 FROM public.operator o WHERE LOWER(o.name) = LOWER(v.op_name)
);

-- ── Step 3: bridge existing `operators` (plural) rows to the new singular rows ──
-- These 4 operators already exist in plural with canonical_operator_id=NULL
-- (came in via TIGER import). Just UPDATE the FK; don't INSERT new rows.

UPDATE public.operators o2
   SET canonical_operator_id = op.id,
       notes = COALESCE(o2.notes, '') ||
               ' [bridged to operator(singular) via harvest_top10 seed]'
  FROM public.operator op
 WHERE op.name IN ('City of San Jose', 'City of Davis', 'City of San Ramon',
                   'Livermore Area Recreation and Park District')
   AND LOWER(o2.canonical_name) = LOWER(op.name)
   AND o2.canonical_operator_id IS NULL;

-- ── Step 4: operator_location_sources rows for the 4 new operators ──

INSERT INTO public.operator_location_sources
  (operator_id, content_type, source_url, shape, discovery_method, notes)
SELECT op.id, 'dog_park', src.url, src.shape::text, 'manual', src.notes
  FROM (VALUES
    ('City of San Jose',
     'https://www.sanjoseca.gov/Home/Components/FacilityDirectory/FacilityDirectory/2174/2028',
     'hub_subpages',
     'Top-10 seed (25 OSM parks). Bot-blocked the default UA last run; Chrome UA now in place.'),
    ('City of Davis',
     'https://www.cityofdavis.org/city-hall/parks-and-community-services/parks-and-open-space/dog-parks',
     'hub_subpages',
     'Top-10 seed (8 OSM parks). Bot-blocked the default UA last run; Chrome UA now in place.'),
    ('City of San Ramon',
     'https://www.sanramon.ca.gov/our_city/departments_and_divisions/parks_community_services/parks_facilities/dog_parks/unleashed_dog_parks',
     'hub_subpages',
     'Top-10 seed (6 OSM parks).'),
    ('Livermore Area Recreation and Park District',
     'https://www.larpd.org/dog-parks',
     'hub_subpages',
     'Top-10 seed (6 OSM parks).')
  ) AS src(op_name, url, shape, notes)
JOIN public.operator op ON op.name = src.op_name
ON CONFLICT (operator_id, content_type, source_url) DO NOTHING;

-- Verify
SELECT op.name, ols.shape, count(*) OVER () AS total_sources
  FROM public.operator_location_sources ols
  JOIN public.operator op ON op.id = ols.operator_id
 WHERE ols.content_type = 'dog_park'
 ORDER BY op.name;

COMMIT;
