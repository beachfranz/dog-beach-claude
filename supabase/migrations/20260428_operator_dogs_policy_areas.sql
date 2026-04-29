-- Per-area columns on operator_dogs_policy for the agency-level
-- spatial-zone decomposition. Mirrors the cpad_unit_dogs_policy area
-- columns. Each agency gets a blanket position on each of the 6
-- fixed areas; per-beach exceptions live in operator_dogs_policy.exceptions
-- and override.

alter table public.operator_dogs_policy
  add column if not exists area_sand        text check (area_sand        is null or area_sand        in ('off_leash','on_leash','forbidden','unknown')),
  add column if not exists area_water       text check (area_water       is null or area_water       in ('off_leash','on_leash','forbidden','unknown')),
  add column if not exists area_picnic_area text check (area_picnic_area is null or area_picnic_area in ('off_leash','on_leash','forbidden','unknown')),
  add column if not exists area_parking_lot text check (area_parking_lot is null or area_parking_lot in ('off_leash','on_leash','forbidden','unknown')),
  add column if not exists area_trails      text check (area_trails      is null or area_trails      in ('off_leash','on_leash','forbidden','unknown')),
  add column if not exists area_campground  text check (area_campground  is null or area_campground  in ('off_leash','on_leash','forbidden','unknown')),
  add column if not exists designated_dog_zones text,
  add column if not exists prohibited_areas     text;
