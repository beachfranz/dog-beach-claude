-- Seed Oregon state-level + county-level agencies for Wave 4 (OR
-- county dog-policy backfills).
--
-- Before: 0 OR-relevant agencies. After: 1 state + 15 counties + key
-- state-level agencies (Oregon Parks and Recreation Department).
--
-- Per the agency table design, agencies are state-agnostic at the
-- column level (state is encoded in the agency name like "Lincoln
-- County" or implicit via parent_agency_id). The unique constraint is
-- on (name, type).

INSERT INTO public.agency (name, type, web_url) VALUES
  ('Oregon State', 'state', 'https://www.oregon.gov/'),
  ('Oregon Parks and Recreation Department', 'state_department', 'https://www.oregon.gov/oprd/'),
  ('Oregon Department of Fish and Wildlife', 'state_department', 'https://myodfw.com/'),
  -- 15 OR counties that have active beaches in beaches_gold
  ('Lincoln County', 'county', 'https://www.co.lincoln.or.us/'),
  ('Curry County', 'county', 'https://www.co.curry.or.us/'),
  ('Tillamook County', 'county', 'https://www.co.tillamook.or.us/'),
  ('Clatsop County', 'county', 'https://www.co.clatsop.or.us/'),
  ('Coos County', 'county', 'https://www.co.coos.or.us/'),
  ('Lane County', 'county', 'https://www.lanecountyor.gov/'),
  ('Columbia County', 'county', 'https://www.columbiacountyor.gov/'),
  ('Multnomah County', 'county', 'https://www.multco.us/'),
  ('Hood River County', 'county', 'https://www.co.hood-river.or.us/'),
  ('Umatilla County', 'county', 'https://www.umatillacounty.gov/'),
  ('Marion County', 'county', 'https://www.co.marion.or.us/'),
  ('Clackamas County', 'county', 'https://www.clackamas.us/'),
  ('Morrow County', 'county', 'https://www.co.morrow.or.us/'),
  ('Douglas County', 'county', 'https://www.douglascountyor.gov/'),
  ('Wasco County', 'county', 'https://co.wasco.or.us/')
ON CONFLICT (name, type) DO NOTHING;
