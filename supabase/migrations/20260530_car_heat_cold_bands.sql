-- Add car_heat_neg + car_cold_neg bands to scoring_config_v2 for both
-- beach and dog_park entity types. Drives a new "Hot car warning" /
-- "Cold car warning" advisory in beach_advisory (and the equivalent
-- dog-park surface when wired).
--
-- Thresholds modeled on ASPCA / standard pet-safety guidance:
--   HOT (temp_air, °F):
--     <70   clear (no advisory)
--     70-80 advisory  — "On a 70°F day, car interior can hit 104°F in 30min"
--     80-90 caution   — "interior 119°F+ in 30min; mortal risk in 15min"
--     >=90  no_go     — "fatal heatstroke risk in minutes"
--
--   COLD (temp_air, °F):
--     >=40  clear
--     32-40 advisory  — "interior cools to ambient quickly; short coats at risk"
--     20-32 caution   — "freezing; even brief stops dangerous"
--     <20   no_go     — "hypothermia risk in minutes"
--
-- Both metrics use the entity's hourly temp_air (NOT feels_like — interior
-- car heating depends on actual ambient + solar load, not wind chill).
--
-- The advisory itself is emitted by compute_weather_advisories.py via the
-- existing HOURLY_METRICS pattern. Pills surface on beach.html via the
-- Cautions card and on dog-park.html (once wired).

begin;

INSERT INTO public.scoring_config_v2 (entity_type, signal_key, bands, notes, updated_at)
VALUES (
  'beach', 'car_heat_neg',
  $$[
    {"max": 70, "score": 0},
    {"min": 70, "max": 80, "score": 2},
    {"min": 80, "max": 90, "score": 6},
    {"min": 90, "score": 10}
  ]$$::jsonb,
  'ASPCA-aligned car-heat advisory bands. temp_air °F. 70/80/90 cuts → advisory/caution/no_go via _v2_lookup_band fraction (score/max).',
  now()
),
(
  'beach', 'car_cold_neg',
  $$[
    {"min": 40, "score": 0},
    {"min": 32, "max": 40, "score": 2},
    {"min": 20, "max": 32, "score": 6},
    {"max": 20, "score": 10}
  ]$$::jsonb,
  'ASPCA-aligned car-cold advisory bands. temp_air °F. 40/32/20 cuts.',
  now()
),
(
  'dog_park', 'car_heat_neg',
  $$[
    {"max": 70, "score": 0},
    {"min": 70, "max": 80, "score": 2},
    {"min": 80, "max": 90, "score": 6},
    {"min": 90, "score": 10}
  ]$$::jsonb,
  'Mirror of beach car_heat_neg — same thresholds apply at parks.',
  now()
),
(
  'dog_park', 'car_cold_neg',
  $$[
    {"min": 40, "score": 0},
    {"min": 32, "max": 40, "score": 2},
    {"min": 20, "max": 32, "score": 6},
    {"max": 20, "score": 10}
  ]$$::jsonb,
  'Mirror of beach car_cold_neg.',
  now()
)
ON CONFLICT (entity_type, signal_key) DO UPDATE
  SET bands = EXCLUDED.bands,
      notes = EXCLUDED.notes,
      updated_at = now();

commit;
