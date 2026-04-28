-- TIGER county polygons (public.counties) become the gold standard for
-- county assignment across our three beach-location sources. Strict
-- ST_Contains: a point that doesn't fall inside a CA county polygon
-- stays unassigned. Existing `county` / `canonical_county` columns are
-- left untouched as historical reference values.
--
-- New columns are suffixed `_tiger` so it's obvious which is canonical.

-- ── Schema additions ─────────────────────────────────────────────
alter table public.ccc_access_points
  add column if not exists county_name_tiger text,
  add column if not exists county_fips_tiger text;   -- 5-digit GEOID

alter table public.us_beach_points
  add column if not exists county_name_tiger text,
  add column if not exists county_fips_tiger text;

alter table public.osm_features
  add column if not exists county_name_tiger text,
  add column if not exists county_fips_tiger text;

-- ── Backfill: PIP against CA counties (state_fp='06') ───────────
-- A LATERAL join scoped to CA narrows the candidate set; ST_Contains
-- on a GIST-indexed geom column is fast.

update public.ccc_access_points t
   set county_name_tiger = c.name,
       county_fips_tiger = c.geoid
  from public.counties c
 where c.state_fp = '06'
   and st_contains(c.geom, t.geom);

update public.us_beach_points t
   set county_name_tiger = c.name,
       county_fips_tiger = c.geoid
  from public.counties c
 where c.state_fp = '06'
   and t.state    = 'CA'
   and st_contains(c.geom, t.geom);

update public.osm_features t
   set county_name_tiger = c.name,
       county_fips_tiger = c.geoid
  from public.counties c
 where c.state_fp = '06'
   and st_contains(c.geom, t.geom);
