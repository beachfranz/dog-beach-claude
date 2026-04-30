-- Derived governing_level on poi_landing + osm_landing.
--
-- One column that summarizes which administrative tier this row falls
-- under, so callers can group / pivot without re-deriving the case
-- expression every time:
--
--   'city'   — TIGER place is incorporated (place_type C%)
--   'cdp'    — TIGER place is a CDP / unincorporated community (place_type U%)
--   'county' — outside any TIGER place but inside a county (state beach,
--              regional park, unincorporated coastline)
--   'state'  — neither place nor county (effectively: outside CA — these
--              tables sometimes carry non-CA rows and jurisdictions/
--              counties are CA-only)
--
-- Generated column: stays in sync automatically when place_type or
-- county_geoid is updated. No backfill needed.

alter table public.poi_landing
  add column if not exists governing_level text generated always as (
    case
      when place_type like 'C%'      then 'city'
      when place_type like 'U%'      then 'cdp'
      when county_geoid is not null  then 'county'
      else                                'state'
    end
  ) stored;

alter table public.osm_landing
  add column if not exists governing_level text generated always as (
    case
      when place_type like 'C%'      then 'city'
      when place_type like 'U%'      then 'cdp'
      when county_geoid is not null  then 'county'
      else                                'state'
    end
  ) stored;

create index if not exists poi_landing_governing_level_idx
  on public.poi_landing (governing_level);
create index if not exists osm_landing_governing_level_idx
  on public.osm_landing (governing_level);
