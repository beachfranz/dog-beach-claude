-- Add missing dog_park bands so compute_dog_park_advisories can emit
-- the full set of weather-derived cautions that today live inline in
-- dog-park.html's buildCautionsSection. After this migration, parity
-- with beach is one renderer-side swap away.
--
-- Adds:
--   feels_like_hot  — heat-stress advisory (uses feels_like, not temp_air)
--   feels_like_cold — cold-stress advisory
--   precip_chance   — rain
--
-- wind_harsh_neg already exists. car_heat_neg + car_cold_neg added
-- in 20260530_car_heat_cold_bands.sql.

begin;

INSERT INTO public.scoring_config_v2 (entity_type, signal_key, bands, notes, updated_at)
VALUES (
  'dog_park', 'feels_like_hot',
  $$[
    {"max": 80, "score": 0},
    {"min": 80, "max": 90, "score": 2},
    {"min": 90, "max": 100, "score": 6},
    {"min": 100, "score": 10}
  ]$$::jsonb,
  'Heat-stress band (feels_like °F). 80/90/100 → advisory/caution/no_go.',
  now()
),
(
  'dog_park', 'feels_like_cold',
  $$[
    {"min": 50, "score": 0},
    {"min": 35, "max": 50, "score": 2},
    {"min": 20, "max": 35, "score": 6},
    {"max": 20, "score": 10}
  ]$$::jsonb,
  'Cold-stress band (feels_like °F). 50/35/20.',
  now()
),
(
  'dog_park', 'precip_chance',
  $$[
    {"max": 30, "score": 0},
    {"min": 30, "max": 60, "score": 2},
    {"min": 60, "max": 85, "score": 6},
    {"min": 85, "score": 10}
  ]$$::jsonb,
  'Rain chance % band. 30/60/85.',
  now()
)
ON CONFLICT (entity_type, signal_key) DO UPDATE
  SET bands = EXCLUDED.bands,
      notes = EXCLUDED.notes,
      updated_at = now();

commit;
