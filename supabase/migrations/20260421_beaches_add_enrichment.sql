-- Add Tier A/B/D enrichment columns to the live `beaches` table, mirroring
-- the equivalent fields on beaches_staging_new. These are additive — existing
-- hand-curated fields on beaches (dog_rules, leash_policy, access_rule,
-- off_leash_flag, allowed_hours_text, parking_text, etc.) are untouched.
--
-- The sync function writes only these new fields on existing rows. For brand-
-- new rows, both legacy and new fields are populated from staging where
-- available.

begin;

alter table public.beaches
  -- Tier A: dog policy
  add column if not exists dogs_allowed              text
    check (dogs_allowed is null or dogs_allowed in ('yes','no','mixed','seasonal','unknown')),
  add column if not exists dogs_allowed_areas        text,
  add column if not exists dogs_prohibited_areas     text,
  add column if not exists dogs_leash_required       boolean,
  add column if not exists dogs_off_leash_area       text,
  add column if not exists dogs_policy_notes         text,
  add column if not exists dogs_policy_source        text,
  add column if not exists dogs_policy_source_url    text,
  add column if not exists dogs_policy_updated_at    timestamptz,
  add column if not exists dogs_time_restrictions    text,
  add column if not exists dogs_season_restrictions  text,
  add column if not exists dogs_seasonal_closures    jsonb,
  add column if not exists dogs_daily_windows        jsonb,
  add column if not exists dogs_day_of_week_mask     smallint,
  add column if not exists dogs_prohibited_reason    text,

  -- Tier B: amenities
  add column if not exists has_parking               boolean,
  add column if not exists parking_type              text,
  add column if not exists parking_notes             text,
  add column if not exists hours_text                text,
  add column if not exists hours_notes               text,
  add column if not exists has_restrooms             boolean,
  add column if not exists has_showers               boolean,
  add column if not exists has_lifeguards            boolean,
  add column if not exists has_drinking_water        boolean,
  add column if not exists has_disabled_access       boolean,

  -- Tier D: governance + metadata
  add column if not exists governing_jurisdiction    text,
  add column if not exists governing_body            text,
  add column if not exists noaa_station_name         text,
  add column if not exists enrichment_source         text,
  add column if not exists enrichment_updated_at     timestamptz,
  add column if not exists enrichment_confidence     text
    check (enrichment_confidence is null or enrichment_confidence in ('high','low'));

-- day-of-week mask bounds
alter table public.beaches
  drop constraint if exists beaches_dogs_day_of_week_mask_valid,
  add  constraint beaches_dogs_day_of_week_mask_valid
    check (dogs_day_of_week_mask is null or dogs_day_of_week_mask between 0 and 127);

commit;
