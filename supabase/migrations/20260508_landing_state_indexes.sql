-- 20260508_landing_state_indexes.sql
--
-- Pre-state-expansion: add state-column indexes on landing tables so
-- state-scoped queries (e.g. "show me all OR beaches not yet in arena")
-- stay fast as the tables grow with multi-state data.
--
-- Per-table state column conventions (audited 2026-05-08):
--   poi_landing.state           — set at ingest from us_beach_points CSV
--   osm_landing.state           — NOT a column; derive from county_geoid → counties
--   ccc_landing.county_geoid    — California-only by source (no state column needed)

begin;

-- POI: state column already exists; index it
create index if not exists poi_landing_state_idx
  on public.poi_landing(state)
  where is_active is true;

-- POI: also index county_geoid (used by promote function joins)
create index if not exists poi_landing_county_geoid_idx
  on public.poi_landing(county_geoid)
  where is_active is true;

-- OSM: county_geoid is the proxy for state-scoping (no state column on the table)
create index if not exists osm_landing_county_geoid_idx
  on public.osm_landing(county_geoid)
  where is_active is true;

-- OSM: speed up the natural=beach filter
create index if not exists osm_landing_beach_tag_idx
  on public.osm_landing((tags->>'natural'))
  where is_active is true;

commit;
