-- 20260530_restore_scoring_config_stub.sql
--
-- Restore the scoring_config table as a stub. It was DROPped in
-- 20260530_drop_v1_status_schema.sql (task #12) but daily-beach-refresh
-- + get-beach-now still import _shared/scoring.ts, which runs the v1
-- raw-value scoring math (hour_score + explainability) and needs the
-- threshold table to load.
--
-- The v1 *_status columns it writes were already dropped — those writes
-- silently no-op now. hour_score + raw values still get written and are
-- consumed by compute_beach_hourly_v2 to derive hour_score_v2.
--
-- This stub holds canonical default values. We're not going to update
-- it; it's a frozen artifact. Task #12 is effectively "drop v1 status
-- columns" — the config table stays as a vestigial dependency until
-- daily-refresh is rewritten to skip _shared/scoring.ts entirely.

begin;

create table if not exists public.scoring_config (
  id                       uuid primary key default gen_random_uuid(),
  scoring_version          text                                 not null,
  effective_from           date                                 not null,
  description              text,
  is_active                boolean default true                 not null,
  nogo_precip_chance       numeric default 70                   not null,
  nogo_wind_speed          numeric default 25                   not null,
  nogo_wmo_codes           integer[] default '{63,64,65,66,67,71,72,73,74,75,76,77,95,96,97,98,99}'::integer[] not null,
  caution_precip_chance    numeric default 50                   not null,
  caution_wind_speed       numeric default 15                   not null,
  caution_tide_height      numeric default 5.0                  not null,
  caution_uv_index         numeric default 8                    not null,
  caution_temp_min         numeric(5,1) default 63              not null,
  caution_temp_max         numeric(5,1) default 85              not null,
  nogo_temp_min            numeric(5,1) default 50              not null,
  nogo_temp_max            numeric(5,1) default 90              not null,
  positive_low_tide        numeric default 1.0                  not null,
  positive_very_low_tide   numeric default 0.5                  not null,
  positive_low_precip      numeric default 15                   not null,
  positive_calm_wind       numeric default 8                    not null,
  positive_temp_min        numeric default 65                   not null,
  positive_temp_max        numeric default 80                   not null,
  positive_low_uv          numeric default 4                    not null,
  busy_quiet_max           numeric default 30                   not null,
  busy_moderate_max        numeric default 60                   not null,
  busy_dog_party_max       numeric default 80                   not null,
  weight_tide              numeric default 0.225                not null,
  weight_rain              numeric default 0.175                not null,
  weight_wind              numeric default 0.20                 not null,
  weight_crowd             numeric default 0.15                 not null,
  weight_temp              numeric default 0.05                 not null,
  weight_uv                numeric default 0.05                 not null,
  weight_weather_code      numeric default 0.15                 not null,
  norm_tide_max            numeric default 4.0                  not null,
  norm_wind_max            numeric default 25                   not null,
  norm_temp_target         numeric default 72                   not null,
  norm_temp_range          numeric default 30                   not null,
  norm_uv_max              numeric default 11                   not null,
  window_min_hours         integer default 2                    not null,
  window_max_hours         integer default 5                    not null,
  window_caution_penalty   numeric default 5.0                  not null,
  window_score_threshold   numeric(4,2) default 0.93            not null,
  -- Advisory tier (added by 20260417_*)
  advisory_precip_chance   numeric default 10                   not null,
  advisory_wind_speed      numeric default 10                   not null,
  advisory_tide_height     numeric default 3.0                  not null,
  advisory_uv_index        numeric default 3                    not null,
  nogo_uv_index            numeric default 11                   not null,
  advisory_crowd_min       numeric default 61                   not null,
  advisory_crowd_max       numeric default 84                   not null,
  advisory_sand_temp       numeric default 105                  not null,
  caution_sand_temp        numeric default 115                  not null,
  nogo_sand_temp           numeric default 125                  not null,
  advisory_asphalt_temp    numeric default 105                  not null,
  caution_asphalt_temp     numeric default 115                  not null,
  nogo_asphalt_temp        numeric default 125                  not null,
  go_temp_cold_min         numeric default 50                   not null,
  advisory_temp_cold_min   numeric default 32                   not null,
  caution_temp_cold_min    numeric default 20                   not null,
  advisory_temp_hot_max    numeric default 75                   not null,
  caution_temp_hot_max     numeric default 85                   not null,
  nogo_temp_hot_max        numeric default 95                   not null,
  caution_wmo_codes        integer[] default '{45,48,51,53,55,56,57,61}'::integer[] not null,
  created_at               timestamptz default now()            not null,
  updated_at               timestamptz default now()            not null,
  constraint scoring_config_version_date_uq unique (scoring_version, effective_from),
  constraint scoring_config_window_hours    check (window_min_hours >= 1 and window_max_hours >= window_min_hours),
  constraint scoring_config_weights_sum     check (round(weight_tide + weight_rain + weight_wind + weight_crowd + weight_temp + weight_uv + weight_weather_code, 10) = 1.0)
);

-- Single active row using all defaults. _shared/scoring.ts SCORING_VERSION constant is 'v1'.
insert into public.scoring_config (scoring_version, effective_from, description, is_active)
values ('v1', current_date, 'Stub config recreated after task #12 drop. _shared/scoring.ts still loads from this table for hour_score math.', true)
on conflict (scoring_version, effective_from) do nothing;

commit;
