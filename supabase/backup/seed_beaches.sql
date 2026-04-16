-- ============================================================
-- Dog Beach Scout — beaches seed data
-- Captured: 2026-04-15
-- ============================================================

INSERT INTO public.beaches (
  location_id, display_name, latitude, longitude,
  noaa_station_id, besttime_venue_id,
  timezone, open_time, close_time,
  address, website
) VALUES
(
  'huntington-dog-beach',
  'Huntington Dog Beach',
  33.6599, -117.9992,
  '9410580',
  'ven_41536536532d47507278495241593368387856385335644a496843',
  'America/Los_Angeles', '06:00', '22:00',
  'Huntington Dog Beach, Pacific Coast Highway, Huntington Beach, CA',
  'https://www.huntingtonbeachca.gov/residents/parks_facilities/dog_beach/'
),
(
  'coronado-dog-beach',
  'Coronado Dog Beach',
  32.6763, -117.1731,
  '9410170',
  'ven_594d4d2d5943573044473452416f33732d4c66337077334a496843',
  'America/Los_Angeles', '06:00', '21:00',
  '301 Ocean Blvd, Coronado, CA 92118',
  'https://www.coronado.ca.us/'
),
(
  'del-mar-dog-beach',
  'Del Mar Dog Beach',
  32.9752, -117.2712,
  '9410230',
  'ven_34496872356c3734664d765241493350674874585131704a496843',
  'America/Los_Angeles', '06:00', '22:00',
  '3902 29th St, Del Mar, CA 92014',
  'https://www.delmar.ca.us/'
),
(
  'ocean-beach-dog-beach',
  'Ocean Beach Dog Beach',
  32.7555, -117.2520,
  '9410170',
  'ven_6b333462596d3159335a5752416f33723663546f59624f4a496843',
  'America/Los_Angeles', '00:00', '23:59',
  '5156 W Point Loma Blvd, San Diego, CA 92107',
  'https://www.sandiego.gov/'
),
(
  'rosies-dog-beach',
  'Rosie''s Dog Beach',
  33.7560, -118.1426,
  '9410680',
  'ven_3058566976486f4263695f524159337734555f4d35564d4a496843',
  'America/Los_Angeles', '06:00', '20:00',
  '4800 E Ocean Blvd, Long Beach, CA 90803',
  'https://www.longbeach.gov/'
)
ON CONFLICT (location_id) DO NOTHING;
