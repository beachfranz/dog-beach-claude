-- ─────────────────────────────────────────────────────────────────────
-- canonical_area_description seeds — first batch.
-- Franz 2026-05-21. Type-case beaches used for v2 area-first re-extract
-- (scripts/reextract_area_first.py) validation.
-- ─────────────────────────────────────────────────────────────────────

UPDATE public.beaches_gold SET canonical_area_description =
  'The Coronado Dog Beach: the designated off-leash dog beach at the '
  'NORTH END of Coronado, at the foot of Sunset Park along Ocean '
  'Boulevard. The only designated off-leash dog beach on Coronado. '
  'Bounded on the north by the Naval Air Station fenceline.'
 WHERE fid = 6202;

UPDATE public.beaches_gold SET canonical_area_description =
  'The main Coronado Beach: the public beach along Ocean Boulevard '
  'SOUTH of Sunset Park / the Dog Beach area, extending southward '
  'to the southern Coronado city limit. Excludes the Dog Beach at '
  'the north end (fid 6202).'
 WHERE fid = 8715;

UPDATE public.beaches_gold SET canonical_area_description =
  E'Rosie\'s Dog Beach: the designated off-leash dog exercise area '
  'on the City of Long Beach beach between Granada Avenue and '
  'Roycroft Avenue, extending from the bike path inland approximately '
  '60 yards toward the water line. The only off-leash beach in the '
  'City of Long Beach.'
 WHERE fid = 6411;

UPDATE public.beaches_gold SET canonical_area_description =
  'Huntington Beach Dog Beach (HBDB): the designated off-leash dog '
  'beach in Huntington Beach, CA, located between Goldenwest Street '
  'and Seapoint Avenue along Pacific Coast Highway. Operated jointly '
  'by City of Huntington Beach and the Preservation Society of '
  'Huntington Dog Beach. Distinct from the SD Ocean Beach Dog Beach.'
 WHERE fid = 6212;

UPDATE public.beaches_gold SET canonical_area_description =
  'Fiesta Island in Mission Bay Park, San Diego — the entire island '
  'including multiple sub-zones (the off-leash dog area on the '
  'south/west shores, on-leash areas, and the boat launch zone).'
 WHERE fid = 9716;
