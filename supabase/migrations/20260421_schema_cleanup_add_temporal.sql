-- Schema cleanup + new structured temporal fields for beaches_staging_new.
--
-- Drops 20 dead or superseded columns:
--   v1 proximity-match fields (replaced by polygon sources):
--     nps_match_score, nps_match_name, nps_match_park,
--     csp_match_score, csp_match_name
--   early policy schema (replaced by dogs_policy_*):
--     policy_source_url, policy_confidence, policy_notes
--   early temporal schema (being replaced by structured JSONB below):
--     allowed_hours_text, seasonal_start, seasonal_end, day_restrictions,
--     dogs_prohibited_start, dogs_prohibited_end
--   early design fields never populated:
--     access_rule, zone_description, dedup_status, geocode_quality
--   populated but no downstream use:
--     quality_tier, country
--
-- Adds 3 new structured fields for day/hour-level dog-access scoring. Text
-- fields (dogs_time_restrictions, dogs_season_restrictions, dogs_policy_notes)
-- stay as the human-readable layer; these structured fields let the scoring
-- engine evaluate rules as cheap boolean checks.
--
-- Note: dogs_prohibited_reason already exists and is kept.

begin;

alter table public.beaches_staging_new
  drop column if exists nps_match_score,
  drop column if exists nps_match_name,
  drop column if exists nps_match_park,
  drop column if exists csp_match_score,
  drop column if exists csp_match_name,
  drop column if exists policy_source_url,
  drop column if exists policy_confidence,
  drop column if exists policy_notes,
  drop column if exists allowed_hours_text,
  drop column if exists seasonal_start,
  drop column if exists seasonal_end,
  drop column if exists day_restrictions,
  drop column if exists dogs_prohibited_start,
  drop column if exists dogs_prohibited_end,
  drop column if exists access_rule,
  drop column if exists zone_description,
  drop column if exists dedup_status,
  drop column if exists geocode_quality,
  drop column if exists quality_tier,
  drop column if exists country;

alter table public.beaches_staging_new
  -- Seasonal closure windows (array of {start:"MM-DD", end:"MM-DD", reason})
  add column if not exists dogs_seasonal_closures jsonb,
  -- Allowed hours-of-day windows (array of {start:"HH:MM", end:"HH:MM"})
  -- null/empty = dogs allowed any time of day (subject to other restrictions)
  add column if not exists dogs_daily_windows jsonb,
  -- Day-of-week bitmask: Sun=1, Mon=2, Tue=4, Wed=8, Thu=16, Fri=32, Sat=64
  -- Convenience: all days = 127, weekdays = 62, weekends = 65. null = any day.
  add column if not exists dogs_day_of_week_mask smallint;

-- Sanity-check constraint for the day_of_week_mask: 7-bit field, 0..127
alter table public.beaches_staging_new
  drop constraint if exists dogs_day_of_week_mask_valid,
  add  constraint dogs_day_of_week_mask_valid
    check (dogs_day_of_week_mask is null or dogs_day_of_week_mask between 0 and 127);

commit;
