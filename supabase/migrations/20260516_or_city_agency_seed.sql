-- Wave 5 OR: seed OR coastal-city agencies (the cities with active
-- beaches in beaches_gold).
--
-- Idempotent via ON CONFLICT (name, type) DO NOTHING.

INSERT INTO public.agency (name, type, web_url) VALUES
  ('Lincoln City', 'city', 'https://www.lincolncity.org/'),
  ('Newport', 'city', 'https://newportoregon.gov/'),
  ('Seaside', 'city', 'https://cityofseaside.us/'),
  ('Bandon', 'city', 'https://cityofbandon.org/'),
  ('Cannon Beach', 'city', 'https://www.ci.cannon-beach.or.us/'),
  ('Rockaway Beach', 'city', 'https://www.corb.us/'),
  ('Gearhart', 'city', 'https://cityofgearhart.com/'),
  ('Brookings', 'city', 'https://www.brookings.or.us/'),
  ('Warrenton', 'city', 'https://www.ci.warrenton.or.us/'),
  ('Manzanita', 'city', 'https://ci.manzanita.or.us/'),
  ('Waldport', 'city', 'https://www.waldport.org/'),
  ('Yachats', 'city', 'https://www.yachatsoregon.org/'),
  ('Gold Beach', 'city', 'https://www.goldbeachoregon.gov/'),
  ('Port Orford', 'city', 'https://portorford.org/'),
  ('Hood River', 'city', 'https://cityofhoodriver.gov/'),
  ('Portland', 'city', 'https://www.portland.gov/')
ON CONFLICT (name, type) DO NOTHING;
