-- Per-area dog-access columns on cpad_unit_dogs_policy.
--
-- The mental model: beach-level dogs_allowed = "yes" if a dog can be on
-- the unit at all. The areas decompose where on the unit dogs can go
-- and under what leash status. A "yes" beach with all areas forbidden
-- except parking_lot=on_leash is correctly answered "yes" — but the
-- detail panel shows the user that only the parking lot allows dogs.
--
-- 6 fixed-enum areas, 4-state each. NULL = not yet extracted.
-- 'unknown' = extractor read the page and couldn't determine.
-- Plus two free-text fields for nuance (named designated zones,
-- explicit prohibition language).
--
-- Per-area temporal rules deferred — design for them later if needed.

alter table public.cpad_unit_dogs_policy
  add column if not exists area_sand        text check (area_sand        is null or area_sand        in ('off_leash','on_leash','forbidden','unknown')),
  add column if not exists area_water       text check (area_water       is null or area_water       in ('off_leash','on_leash','forbidden','unknown')),
  add column if not exists area_picnic_area text check (area_picnic_area is null or area_picnic_area in ('off_leash','on_leash','forbidden','unknown')),
  add column if not exists area_parking_lot text check (area_parking_lot is null or area_parking_lot in ('off_leash','on_leash','forbidden','unknown')),
  add column if not exists area_trails      text check (area_trails      is null or area_trails      in ('off_leash','on_leash','forbidden','unknown')),
  add column if not exists area_campground  text check (area_campground  is null or area_campground  in ('off_leash','on_leash','forbidden','unknown')),
  add column if not exists designated_dog_zones text,
  add column if not exists prohibited_areas     text;
