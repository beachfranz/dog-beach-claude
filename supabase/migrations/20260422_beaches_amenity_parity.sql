-- Add three amenity columns to beaches (has_fire_pits, has_food, has_picnic_area)
-- so the live table matches beaches_staging_new's amenity set. Schema drift
-- caught via admin/beach-editor.html — editing a beach and saving attempted
-- to write has_fire_pits, which didn't exist on beaches.
--
-- All three are nullable booleans (matching the existing has_* pattern on
-- beaches). Defaults to NULL = "unknown" rather than false — we don't want
-- to assert "no fire pits" for every beach on day 1.

alter table public.beaches
  add column if not exists has_fire_pits   boolean,
  add column if not exists has_food        boolean,
  add column if not exists has_picnic_area boolean;
