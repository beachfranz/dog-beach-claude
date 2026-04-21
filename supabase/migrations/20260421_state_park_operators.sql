-- state_park_operators: curated mapping of state parks that are operationally
-- managed by a city or county under contract with California State Parks.
--
-- Rationale: for dog-beach-scout, "governing jurisdiction" means who sets the
-- dog policy and day-to-day rules, which for these state-owned parks is the
-- operating authority (city/county) rather than CA State Parks.
--
-- Source of truth: CA State Parks concessions/operating agreements, confirmed
-- against city/county parks department websites. Updated 2026-04-21.
--
-- Key field: state_park_name is matched against beaches_staging_new.governing_body
-- when governing_body_source = 'state_polygon'. Exact string match.

create table if not exists public.state_park_operators (
  state_park_name       text primary key,
  operator_jurisdiction text not null check (operator_jurisdiction in (
    'governing city', 'governing county'
  )),
  operator_body         text not null,
  notes                 text
);

insert into public.state_park_operators (state_park_name, operator_jurisdiction, operator_body, notes)
values
  ('Santa Monica SB',     'governing city',   'City of Santa Monica',
     'State-owned, operated by City of Santa Monica under long-standing lease.'),
  ('Corona del Mar SB',   'governing city',   'City of Newport Beach',
     'State-owned, operated by City of Newport Beach.'),
  ('Leucadia SB',         'governing city',   'City of Encinitas',
     'State-owned, operated by City of Encinitas.'),
  ('Moonlight SB',        'governing city',   'City of Encinitas',
     'State-owned, operated by City of Encinitas.'),
  ('Seabright SB',        'governing city',   'City of Santa Cruz',
     'State-owned, operated by City of Santa Cruz.'),
  ('Pacifica SB',         'governing city',   'City of Pacifica',
     'State-owned, operated by City of Pacifica.'),
  ('San Buenaventura SB', 'governing city',   'City of Ventura',
     'State-owned, operated by City of San Buenaventura (Ventura).'),
  ('Monterey SB',         'governing city',   'City of Monterey',
     'State-owned, operated by City of Monterey.'),
  ('Will Rogers SB',      'governing county', 'County of Los Angeles',
     'State-owned, operated by LA County Beaches & Harbors.'),
  ('Dockweiler SB',       'governing county', 'County of Los Angeles',
     'State-owned, operated by LA County Beaches & Harbors.')
on conflict (state_park_name) do nothing;
