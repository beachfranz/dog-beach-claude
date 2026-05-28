-- 20260527_scoring_config_v2.sql
--
-- Move v2 scoring BANDED signals (UV, asphalt, wind, drive, etc.) into
-- a data table so tweaking a threshold is an UPDATE instead of a
-- function rewrite, and so multiple entity types (dog_park, beach,
-- future trail/swim_hole/etc) can be compared/tuned side-by-side.
-- Per Franz 2026-05-27 LATE — "scoring bands as data, not code".
--
-- Bell curves (temp), linear ramps (feels_hot, feels_cold, precip),
-- and weather-code lookups (WMO) stay INLINE — they don't fit cleanly
-- into a range-band schema and rarely change anyway.

begin;

create table if not exists public.scoring_config_v2 (
  entity_type text        not null,   -- 'dog_park' | 'beach' | future
  signal_key  text        not null,   -- 'wind_pos', 'uv_neg', 'asphalt_neg', etc.
  bands       jsonb       not null,   -- [{"min": <n|null>, "max": <n|null>, "score": <int>}, ...]
  notes       text,
  updated_at  timestamptz not null default now(),
  primary key (entity_type, signal_key)
);

comment on table public.scoring_config_v2 is
  'Banded scoring thresholds per (entity_type, signal_key). Each row holds '
  'an array of {min, max, score} bands; null bounds mean unbounded. The '
  '_v2_lookup_band helper does the range lookup. Centralises band tweaks '
  'so adjusting e.g. UV thresholds is one UPDATE, not a function rewrite.';

-- ── Helper: range-band lookup ───────────────────────────────────────
-- Returns the score for the first band where value falls in [min, max).
-- Null min = -∞, null max = +∞. Returns 0 if no match (shouldn't happen
-- if bands cover the full range).

create or replace function public._v2_lookup_band(p_value numeric, p_bands jsonb)
returns integer language sql stable as $$
  select coalesce((
    select (band->>'score')::int
    from jsonb_array_elements(p_bands) band
    where (band->>'min' is null or p_value >= (band->>'min')::numeric)
      and (band->>'max' is null or p_value <  (band->>'max')::numeric)
    limit 1
  ), 0)
$$;

-- ── Seed: dog-park v2 banded signals ────────────────────────────────

insert into public.scoring_config_v2 (entity_type, signal_key, bands, notes) values
  ('dog_park', 'wind_pos',
   '[{"max": 6, "score": 2},
     {"min": 6, "max": 13, "score": 6},
     {"min": 13, "score": 0}]'::jsonb,
   'Calm/light → 2; gentle breeze (optimal) → 6; moderate+ → 0. Max 6.'),

  ('dog_park', 'wind_harsh_neg',
   '[{"max": 13, "score": 0},
     {"min": 13, "max": 19, "score": 2},
     {"min": 19, "max": 25, "score": 4},
     {"min": 25, "max": 35, "score": 6},
     {"min": 35, "score": 10}]'::jsonb,
   '<13 OK; 13-18 nuisance; 19-24 discouraging; 25-34 strong; 35+ gale. Max 10.'),

  ('dog_park', 'asphalt_neg',
   '[{"max": 115, "score": 0},
     {"min": 115, "max": 125, "score": 2},
     {"min": 125, "max": 135, "score": 8},
     {"min": 135, "max": 145, "score": 12},
     {"min": 145, "score": 15}]'::jsonb,
   '<115°F safe; 115-125 warm; 125-135 burn risk (gate fires); 135-145 severe; 145+ extreme.'),

  ('dog_park', 'uv_neg',
   '[{"max": 6, "score": 0},
     {"min": 6, "max": 8, "score": 1},
     {"min": 8, "max": 10, "score": 3},
     {"min": 10, "max": 11, "score": 4},
     {"min": 11, "score": 10}]'::jsonb,
   'EPA tier mapping: Low/Moderate 0; High 1; Very High lo 3; Very High top 4; Extreme 10 (gate).'),

  ('dog_park', 'uv_need',
   '[{"max": 6, "score": 0},
     {"min": 6, "max": 8, "score": 2},
     {"min": 8, "max": 10, "score": 4},
     {"min": 10, "score": 6}]'::jsonb,
   'Shade-need from UV. Mirrors uv_neg scaled to shade max 6. Combined with temp_need via max().'),

  ('dog_park', 'temp_need',
   '[{"max": 80, "score": 0},
     {"min": 80, "max": 90, "score": 2},
     {"min": 90, "max": 95, "score": 4},
     {"min": 95, "score": 6}]'::jsonb,
   'Shade-need from feels-like temp. Mirrors uv_need scale. Combined with uv_need via max().'),

  ('dog_park', 'drive_factor',
   '[{"max": 11, "score": 3},
     {"min": 11, "max": 26, "score": 0},
     {"min": 26, "max": 46, "score": -3},
     {"min": 46, "max": 76, "score": -6},
     {"min": 76, "score": -10}]'::jsonb,
   'Drive minutes → factor. <=10 +3; 11-25 neutral; 26-45 -3; 46-75 -6; >75 -10.')
on conflict (entity_type, signal_key) do update
  set bands = excluded.bands,
      notes = excluded.notes,
      updated_at = now();

commit;
notify pgrst, 'reload schema';
