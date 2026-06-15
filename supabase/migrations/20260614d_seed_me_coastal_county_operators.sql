-- 20260614d_seed_me_coastal_county_operators.sql
--
-- Seed 8 missing ME county operators in the consumer-surface registry.
-- These are the COASTAL ME counties (where the beaches actually are)
-- that were skipped during initial TIGER-county seeding. Counterpart to
-- sibling migration 20260614c which promoted the 8 ME INLAND county
-- operators that did get seeded.
--
-- Identified by the 2026-06-14 audit: 754 scoring-tier beaches have no
-- resolved operator. 209 of them sit in these 8 coastal ME counties
-- where no operator row exists at all (cf. 53 inland-ME orphans where
-- the row exists but is_canonical=false, handled in 20260614c).
--
-- Schema fields mirror the existing TIGER-seeded inland-ME operators
-- (e.g. id=85033 Androscoggin) — same level, subtype, origin_source,
-- aliases shape, counter init. fips_state='23' (Maine), fips_county
-- from US Census standard 3-digit county FIPS.
--
-- New operators (8 rows, id auto-assigned):
--   Cumberland County (fips=005, ~51 orphans)
--   Franklin County   (fips=007, ~2 orphans inland but seeded for completeness)
--   Hancock County    (fips=009, ~57 orphans — biggest single bucket)
--   Knox County       (fips=013, ~21 orphans)
--   Lincoln County    (fips=015, ~15 orphans)
--   Somerset County   (fips=025, ~1 orphan inland but seeded for completeness)
--   Washington County (fips=029, ~22 orphans)
--   York County       (fips=031, ~40 orphans)

BEGIN;

INSERT INTO public.operators
  (slug, canonical_name, short_name, aliases, level, subtype,
   state_code, fips_state, fips_county, county_geoid,
   osm_operator_strings, origin_source,
   cpad_unit_count, ccc_point_count, osm_feature_count, usbeach_count,
   is_active, is_canonical, created_at, updated_at)
VALUES
  ('cumberland-county-me', 'Cumberland County', 'Cumberland',
   ARRAY['Cumberland County', 'Cumberland', 'Cumberland County, County of', 'County of Cumberland'],
   'county', 'county',
   'ME', '23', '005', '23005',
   ARRAY[]::text[], 'tiger_counties',
   0, 0, 0, 0,
   true, true, now(), now()),
  ('franklin-county-me', 'Franklin County', 'Franklin',
   ARRAY['Franklin County', 'Franklin', 'Franklin County, County of', 'County of Franklin'],
   'county', 'county',
   'ME', '23', '007', '23007',
   ARRAY[]::text[], 'tiger_counties',
   0, 0, 0, 0,
   true, true, now(), now()),
  ('hancock-county-me', 'Hancock County', 'Hancock',
   ARRAY['Hancock County', 'Hancock', 'Hancock County, County of', 'County of Hancock'],
   'county', 'county',
   'ME', '23', '009', '23009',
   ARRAY[]::text[], 'tiger_counties',
   0, 0, 0, 0,
   true, true, now(), now()),
  ('knox-county-me', 'Knox County', 'Knox',
   ARRAY['Knox County', 'Knox', 'Knox County, County of', 'County of Knox'],
   'county', 'county',
   'ME', '23', '013', '23013',
   ARRAY[]::text[], 'tiger_counties',
   0, 0, 0, 0,
   true, true, now(), now()),
  ('lincoln-county-me', 'Lincoln County', 'Lincoln',
   ARRAY['Lincoln County', 'Lincoln', 'Lincoln County, County of', 'County of Lincoln'],
   'county', 'county',
   'ME', '23', '015', '23015',
   ARRAY[]::text[], 'tiger_counties',
   0, 0, 0, 0,
   true, true, now(), now()),
  ('somerset-county-me', 'Somerset County', 'Somerset',
   ARRAY['Somerset County', 'Somerset', 'Somerset County, County of', 'County of Somerset'],
   'county', 'county',
   'ME', '23', '025', '23025',
   ARRAY[]::text[], 'tiger_counties',
   0, 0, 0, 0,
   true, true, now(), now()),
  ('washington-county-me', 'Washington County', 'Washington',
   ARRAY['Washington County', 'Washington', 'Washington County, County of', 'County of Washington'],
   'county', 'county',
   'ME', '23', '029', '23029',
   ARRAY[]::text[], 'tiger_counties',
   0, 0, 0, 0,
   true, true, now(), now()),
  ('york-county-me', 'York County', 'York',
   ARRAY['York County', 'York', 'York County, County of', 'County of York'],
   'county', 'county',
   'ME', '23', '031', '23031',
   ARRAY[]::text[], 'tiger_counties',
   0, 0, 0, 0,
   true, true, now(), now())
ON CONFLICT (slug) DO NOTHING;

-- Audit: this should report 8 if all rows landed
SELECT count(*) AS seeded
  FROM public.operators
 WHERE state_code = 'ME'
   AND level = 'county'
   AND slug IN (
     'cumberland-county-me','franklin-county-me','hancock-county-me',
     'knox-county-me','lincoln-county-me','somerset-county-me',
     'washington-county-me','york-county-me'
   );

COMMIT;

NOTIFY pgrst, 'reload schema';
