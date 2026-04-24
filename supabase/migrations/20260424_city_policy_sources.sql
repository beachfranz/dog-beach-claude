-- city_policy_sources — URL registry per TIGER-place-identified city (2026-04-24)
-- First pass of the municipal policy layer: maps each place_fips to one or
-- more authoritative URLs we can scrape for beach metadata (dog policy,
-- access rules, amenities).
--
-- source_type taxonomy:
--   city_official          — city.gov or equivalent home / main government page
--   city_beaches           — city parks/beaches index or listing
--   city_dog_policy        — specific dog-rules page if a dedicated one exists
--   city_muni_code         — codified beach/dog ordinance (law; highest trust)
--   visitor_bureau         — CVB / tourism office homepage
--   visitor_bureau_beaches — CVB's beaches section (often richer than city site)
--   other                  — catch-all for one-off useful pages

create table if not exists public.city_policy_sources (
  id             bigserial primary key,
  place_fips     text not null,  -- joins to jurisdictions(fips_state || fips_place)
  source_type    text not null check (source_type in (
                   'city_official', 'city_beaches', 'city_dog_policy',
                   'city_muni_code', 'visitor_bureau', 'visitor_bureau_beaches', 'other'
                 )),
  url            text not null,
  title          text,
  notes          text,
  last_checked   timestamptz,
  curated_at     timestamptz not null default now(),
  curated_by     text default 'claude'
);

create index if not exists city_policy_sources_place_fips_idx
  on public.city_policy_sources(place_fips);
create index if not exists city_policy_sources_source_type_idx
  on public.city_policy_sources(source_type);

comment on table public.city_policy_sources is
  'URL registry per TIGER-place city. Feeds the policy-extraction pipeline. One city can have multiple rows (city official + CVB + specific beaches page, etc.). See project_extraction_calibration.md.';

-- ── Seed: top 10 CA coastal cities by beach count ────────────────────────
-- curated_by='claude' marks these as bot-proposed; an admin should spot-check
-- before we rely on them.

insert into public.city_policy_sources (place_fips, source_type, url, title, curated_by) values
-- Laguna Beach (35 beaches)
('0639178', 'city_official',          'https://www.lagunabeachcity.net/',                              'City of Laguna Beach', 'claude'),
('0639178', 'visitor_bureau',         'https://www.visitlagunabeach.com/',                             'Visit Laguna Beach', 'claude'),

-- Malibu (34)
('0645246', 'city_official',          'https://www.malibucity.org/',                                   'City of Malibu', 'claude'),
('0645246', 'visitor_bureau',         'https://www.visitmalibu.com/',                                  'Visit Malibu', 'claude'),

-- Los Angeles (24) — LA uses County beaches + DBH (Dept of Beaches & Harbors)
('0644000', 'city_official',          'https://www.lacity.gov/',                                       'City of Los Angeles', 'claude'),
('0644000', 'visitor_bureau',         'https://www.discoverlosangeles.com/',                           'Discover Los Angeles', 'claude'),
('0644000', 'other',                  'https://beaches.lacounty.gov/',                                 'LA County Beaches & Harbors (NB: county, not city-run)', 'claude'),

-- San Francisco (19) — consolidated city-county
('0667000', 'city_official',          'https://sf.gov/',                                               'City & County of San Francisco', 'claude'),
('0667000', 'visitor_bureau',         'https://www.sftravel.com/',                                     'San Francisco Travel', 'claude'),

-- Long Beach (18)
('0643000', 'city_official',          'https://www.longbeach.gov/',                                    'City of Long Beach', 'claude'),
('0643000', 'visitor_bureau',         'https://www.visitlongbeach.com/',                               'Visit Long Beach', 'claude'),

-- Half Moon Bay (17)
('0631708', 'city_official',          'https://www.hmbcity.com/',                                      'City of Half Moon Bay', 'claude'),
('0631708', 'visitor_bureau',         'https://www.visithalfmoonbay.org/',                             'Visit Half Moon Bay', 'claude'),

-- Dana Point (14)
('0617946', 'city_official',          'https://www.danapoint.org/',                                    'City of Dana Point', 'claude'),
('0617946', 'visitor_bureau',         'https://www.visitdanapoint.com/',                               'Visit Dana Point', 'claude'),

-- Huntington Beach (14)
('0636000', 'city_official',          'https://www.huntingtonbeachca.gov/',                            'City of Huntington Beach', 'claude'),
('0636000', 'visitor_bureau',         'https://www.surfcityusa.com/',                                  'Visit Huntington Beach (Surf City USA)', 'claude'),

-- Santa Barbara (12)
('0669070', 'city_official',          'https://www.santabarbaraca.gov/',                               'City of Santa Barbara', 'claude'),
('0669070', 'visitor_bureau',         'https://santabarbaraca.com/',                                   'Visit Santa Barbara', 'claude'),

-- Encinitas (11)
('0622678', 'city_official',          'https://www.encinitasca.gov/',                                  'City of Encinitas', 'claude'),
('0622678', 'visitor_bureau',         'https://www.sandiego.org/explore/cities-neighborhoods/north-county-coastal/encinitas.aspx', 'Visit San Diego — Encinitas (no standalone CVB)', 'claude');
