-- 20260528_beach_v2_bands.sql
--
-- Beach v2 scoring — Phase 1: seed entity_type='beach' bands into the
-- existing scoring_config_v2 table. Same shape/helper as dog_park v2.
--
-- Strategy: start with bands matching v1 status thresholds where possible,
-- copy weather curves from dog_park initially (will diverge later per
-- Franz's (b) — weather scoring is per-entity). New buckets: sand_temp_neg,
-- tide_neg, crowd_neg. Iterate the bands AFTER the structural port lands.

begin;

insert into public.scoring_config_v2 (entity_type, signal_key, bands, notes) values

  -- WEATHER POSITIVES (initial copy from dog_park; tune later per (b))
  ('beach', 'wind_pos',
   '[{"max": 6, "score": 2},
     {"min": 6, "max": 13, "score": 6},
     {"min": 13, "score": 0}]'::jsonb,
   'Beach wind positive — initial copy of dog_park bands. Reconsider for surfer/kiter use cases later.'),

  -- WEATHER HARSH NEG
  ('beach', 'wind_harsh_neg',
   '[{"max": 13, "score": 0},
     {"min": 13, "max": 19, "score": 2},
     {"min": 19, "max": 25, "score": 4},
     {"min": 25, "max": 35, "score": 6},
     {"min": 35, "score": 10}]'::jsonb,
   'Beach harsh wind — copy of dog_park. Blowing sand may make beaches more sensitive — tune later.'),

  -- HEAT NEGATIVES
  ('beach', 'asphalt_neg',
   '[{"max": 115, "score": 0},
     {"min": 115, "max": 125, "score": 2},
     {"min": 125, "max": 135, "score": 6},
     {"min": 135, "max": 145, "score": 8},
     {"min": 145, "score": 8}]'::jsonb,
   'Parking-lot walk only — copy of dog_park asphalt but max 8 (beach asphalt is more transient than dog park).'),

  ('beach', 'sand_temp_neg',
   '[{"max": 115, "score": 0},
     {"min": 115, "max": 125, "score": 3},
     {"min": 125, "max": 135, "score": 6},
     {"min": 135, "max": 145, "score": 8},
     {"min": 145, "score": 10}]'::jsonb,
   'Sand-temp paw burn (NEW for beach). Same Weave thresholds as asphalt, slightly steeper since sand is the play surface.'),

  ('beach', 'uv_neg',
   '[{"max": 6, "score": 0},
     {"min": 6, "max": 8, "score": 1},
     {"min": 8, "max": 10, "score": 3},
     {"min": 10, "max": 11, "score": 4},
     {"min": 11, "score": 8}]'::jsonb,
   'EPA UV tiers — copy of dog_park. Slightly lower extreme (8 vs 10) because shade is rarer on a beach; the impact is felt differently.'),

  -- TIDE — NEW, generic thresholds. Specific beaches lose sand at different
  -- heights but per-beach tide-tolerance data is a follow-up.
  ('beach', 'tide_neg',
   '[{"max": 3, "score": 0},
     {"min": 3, "max": 5, "score": 2},
     {"min": 5, "max": 7, "score": 6},
     {"min": 7, "score": 10}]'::jsonb,
   'Tide height (ft) penalty — high tide = less sand for dogs. Generic thresholds; per-beach tolerance data is a follow-up.'),

  -- CROWD — NEW. BestTime currently soft-removed → values will mostly be NULL
  -- or neutral 50; bands keep the column wired for when crowd data returns.
  ('beach', 'crowd_neg',
   '[{"max": 30, "score": 0},
     {"min": 30, "max": 60, "score": 1},
     {"min": 60, "max": 85, "score": 3},
     {"min": 85, "score": 5}]'::jsonb,
   'Crowd / busyness penalty. Max 5 (intentionally small — crowd preferences vary; many users WANT a lively beach).'),

  -- DRIVE — copy of dog_park
  ('beach', 'drive_factor',
   '[{"max": 11, "score": 3},
     {"min": 11, "max": 26, "score": 0},
     {"min": 26, "max": 46, "score": -3},
     {"min": 46, "max": 76, "score": -6},
     {"min": 76, "score": -10}]'::jsonb,
   'Drive minutes → factor. Same shape as dog_park.')

on conflict (entity_type, signal_key) do update
  set bands = excluded.bands,
      notes = excluded.notes,
      updated_at = now();

commit;
