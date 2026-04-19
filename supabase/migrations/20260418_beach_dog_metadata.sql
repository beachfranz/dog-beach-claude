-- Add dog-access metadata fields to beaches table
ALTER TABLE public.beaches
  ADD COLUMN IF NOT EXISTS locality          text,
  ADD COLUMN IF NOT EXISTS off_leash_flag    boolean,
  ADD COLUMN IF NOT EXISTS access_rule       text,   -- off_leash | on_leash | mixed
  ADD COLUMN IF NOT EXISTS access_scope      text,   -- specific_beach | partial_beach
  ADD COLUMN IF NOT EXISTS allowed_hours_text text;  -- plain-text access hours/rule description

-- Rosie's Dog Beach — Long Beach
UPDATE public.beaches SET
  locality           = 'Long Beach',
  off_leash_flag     = true,
  leash_policy       = 'Off-leash inside designated area',
  access_rule        = 'off_leash',
  access_scope       = 'specific_beach',
  allowed_hours_text = 'Dog Zone open 6:00am–8:00pm daily.',
  parking_text       = 'Metered parking in Granada Avenue lot.',
  dog_rules          = 'Leash required while entering and exiting; off-leash inside the 4.1-acre zone.'
WHERE location_id = 'rosies-dog-beach';

-- Huntington Dog Beach — Huntington Beach
UPDATE public.beaches SET
  locality           = 'Huntington Beach',
  off_leash_flag     = false,
  leash_policy       = 'Leash required at all times',
  access_rule        = 'on_leash',
  access_scope       = 'specific_beach',
  allowed_hours_text = NULL,
  parking_text       = 'Street and lot parking vary by access point; verify seasonally.',
  dog_rules          = 'Dogs permitted at the designated Dog Beach section; leash required at all times.'
WHERE location_id = 'huntington-dog-beach';

-- Ocean Beach Dog Beach — San Diego
UPDATE public.beaches SET
  locality           = 'Ocean Beach, San Diego',
  off_leash_flag     = true,
  leash_policy       = 'Off-leash',
  access_rule        = 'off_leash',
  access_scope       = 'specific_beach',
  allowed_hours_text = NULL,
  parking_text       = 'Nearby lot and street parking; verify posted restrictions.',
  dog_rules          = 'One of San Diego''s two designated leash-free shoreline exercise areas.'
WHERE location_id = 'ocean-beach-dog-beach';

-- Coronado Dog Beach — Coronado
UPDATE public.beaches SET
  locality           = 'Coronado',
  off_leash_flag     = true,
  leash_policy       = 'Off-leash on beach; leash required on approach',
  access_rule        = 'off_leash',
  access_scope       = 'specific_beach',
  allowed_hours_text = NULL,
  parking_text       = 'North-end beach parking and street access; verify local restrictions.',
  dog_rules          = 'Dogs allowed only in the north-end dog beach area. Leash required on approach to the beach.'
WHERE location_id = 'coronado-dog-beach';

-- Del Mar Dog Beach — Del Mar
UPDATE public.beaches SET
  locality           = 'Del Mar',
  off_leash_flag     = true,
  leash_policy       = 'Mixed; off-leash off-season, restricted in summer',
  access_rule        = 'mixed',
  access_scope       = 'specific_beach',
  allowed_hours_text = 'Off-season (Labor Day–June 15): off-leash. Summer (June 16–Labor Day): off-leash dawn–8:00am, leashed after.',
  parking_text       = 'Nearby paid and street parking; competitive in summer.',
  dog_rules          = 'Peak season (June 16–Labor Day): off-leash dawn–8am only, then leashed. Off-season: off-leash all day.'
WHERE location_id = 'del-mar-dog-beach';
