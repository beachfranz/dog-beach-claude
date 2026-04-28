-- Managing-agency inference for every beach-location record across the
-- three sources. Three columns per table:
--   managing_agency        — agency name (e.g., "California State Parks")
--   managing_agency_level  — federal/state/county/city/special-district
--   managing_agency_source — 'cpad' | 'osm_tag' | 'tiger_c1' | 'manual'
--
-- Inference cascade (run in order, only fill rows that are still null):
--   Pass 1: CPAD PIP — most authoritative; CPAD polygon's mng_agncy
--           directly says who manages the property.
--   Pass 2: OSM operator tag — only for osm_features rows; OSM
--           contributors sometimes tagged the operator explicitly.
--   Pass 3: TIGER C1 fallback — geographic city is the presumed
--           operator if nothing else has spoken. Carries level='city'.

-- ── Schema ──────────────────────────────────────────────────────────
alter table public.ccc_access_points
  add column if not exists managing_agency        text,
  add column if not exists managing_agency_level  text,
  add column if not exists managing_agency_source text;

alter table public.us_beach_points
  add column if not exists managing_agency        text,
  add column if not exists managing_agency_level  text,
  add column if not exists managing_agency_source text;

alter table public.osm_features
  add column if not exists managing_agency        text,
  add column if not exists managing_agency_level  text,
  add column if not exists managing_agency_source text;
