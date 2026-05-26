-- 20260525_dog_park_scoring_schema.sql
--
-- B1 of dog-park sub-pipeline Phase B (see [[dog-park-subpipeline-scope]]).
--
-- Three new tables, mirroring beach scoring infrastructure with domain-specific
-- adjustments:
--   - dog_park_scoring_config        — mirrors scoring_config; drops tide/crowd/
--                                       sand metrics (per spec R1, R2 +
--                                       [[besttime-soft-removed]]); adds JSONB
--                                       surface_lookup for (metric × surface)
--                                       rescoring.
--   - dog_park_day_hourly_scores     — mirrors beach_day_hourly_scores; drops
--                                       tide/crowd/sand cols; FK to
--                                       dog_parks_gold.fid via dog_park_fid.
--   - dog_park_day_recommendations   — mirrors beach_day_recommendations; same
--                                       drops; same FK.
--
-- Naming: the FK col is `dog_park_fid` (not arena_group_id) — this is a fresh
-- table, no need to inherit the misleading legacy name (see
-- [[arena-group-id-is-fid-not-group]]).

begin;

-- ───────────────────────────────────────────────────────────────────────────
-- 1. dog_park_scoring_config
-- ───────────────────────────────────────────────────────────────────────────

create table public.dog_park_scoring_config (
  id                          uuid primary key default gen_random_uuid(),
  scoring_version             text not null,
  effective_from              date not null,
  description                 text,
  is_active                   boolean not null default false,

  -- Wind
  nogo_wind_speed             numeric not null default 25,
  caution_wind_speed          numeric not null default 15,
  advisory_wind_speed         numeric not null default 10,
  norm_wind_max               numeric not null default 25,
  norm_wind_falloff           numeric default 20,
  norm_wind_ideal_cold        numeric default 2,
  norm_wind_ideal_comfortable numeric default 8,
  norm_wind_ideal_hot         numeric default 15,
  norm_wind_temp_cold_max     numeric default 55,
  norm_wind_temp_hot_min      numeric default 80,

  -- Rain / WMO codes
  nogo_precip_chance          numeric not null default 70,
  caution_precip_chance       numeric not null default 40,
  advisory_precip_chance      numeric not null default 10,
  nogo_wmo_codes              integer[] not null default '{95,96,97,98,99,63,64,65,66,67,71,72,73,74,75,76,77}',
  caution_wmo_codes           integer[] not null default '{45,48,51,53,55,56,57,61}',

  -- Temp (hot + cold tiers)
  go_temp_cold_min            numeric not null default 50,
  advisory_temp_cold_min      numeric not null default 32,
  caution_temp_cold_min       numeric not null default 20,
  advisory_temp_hot_max       numeric not null default 75,
  caution_temp_hot_max        numeric not null default 85,
  nogo_temp_hot_max           numeric not null default 95,
  norm_temp_target            numeric not null default 68, -- dogs prefer cooler than beach humans
  norm_temp_range             numeric not null default 25,

  -- UV
  advisory_uv_index           numeric not null default 6, -- raised from 3 per CLAUDE.md note
  caution_uv_index            numeric not null default 8,
  nogo_uv_index               numeric not null default 11,
  norm_uv_max                 numeric not null default 11,

  -- Asphalt (paw burn) — surface-modulated via surface_lookup
  advisory_asphalt_temp       numeric not null default 105,
  caution_asphalt_temp        numeric not null default 115,
  nogo_asphalt_temp           numeric not null default 125,

  -- Weights (sum to 1.0). NO tide, NO crowd, NO sand-temp.
  -- Redistribute the dropped 30%+15% across remaining + surface bonus.
  weight_rain                 numeric not null default 0.30,
  weight_wind                 numeric not null default 0.20,
  weight_temp                 numeric not null default 0.20,
  weight_uv                   numeric not null default 0.10,
  weight_weather_code         numeric not null default 0.10,
  weight_surface              numeric not null default 0.10, -- NEW: surface × metric rescoring contribution

  -- Surface × metric rescoring lookup (per spec; see project pin)
  -- Shape: { metric: { surface: { tier?, score_mul?, paw_temp_boost_f?, tier_escalation? } } }
  surface_lookup              jsonb not null default '{
    "rain": {
      "grass":              {"tier": "minor",    "score_mul": 0.85},
      "dirt":               {"tier": "caution",  "score_mul": 0.45},
      "concrete":           {"tier": "advisory", "score_mul": 0.65},
      "decomposed_granite": {"tier": "advisory", "score_mul": 0.65},
      "sand":               {"tier": "advisory", "score_mul": 0.70},
      "wood_chips":         {"tier": "advisory", "score_mul": 0.60},
      "_default":           {"tier": "advisory", "score_mul": 0.70}
    },
    "uv": {
      "asphalt":  {"paw_temp_boost_f": 30, "tier_override": "caution_when_hot"},
      "concrete": {"paw_temp_boost_f": 25, "tier_override": "caution_when_hot"},
      "sand":     {"paw_temp_boost_f": 20},
      "dirt":     {"paw_temp_boost_f": 15},
      "grass":    {"paw_temp_boost_f": 0},
      "_default": {"paw_temp_boost_f": 15}
    },
    "heat": {
      "asphalt":  {"tier_escalation": 1},
      "concrete": {"tier_escalation": 1},
      "_default": {"tier_escalation": 0}
    }
  }'::jsonb,

  -- Status multipliers (same as beach config)
  status_mult_advisory        numeric default 0.90,
  status_mult_caution         numeric default 0.75,
  status_mult_nogo            numeric default 0.50,

  -- Window selection
  window_min_hours            integer not null default 2,
  window_max_hours            integer not null default 5,
  window_score_threshold      numeric default 0.93,
  window_caution_penalty      numeric not null default 5.0,
  window_peak_oversample_n    numeric default 1,
  window_consistency_weight   numeric default 0.10,
  window_length_penalty       numeric default 0.05,
  window_soft_cap_hours       numeric default 4,

  created_at                  timestamptz not null default now(),
  updated_at                  timestamptz not null default now()
);

create unique index dpsc_active_uidx on public.dog_park_scoring_config (is_active) where is_active;

comment on table public.dog_park_scoring_config is
  'Dog-park scoring weights + thresholds. Mirrors scoring_config (beach) minus tide/crowd/sand. Adds surface_lookup JSONB for (metric × surface) rescoring. See [[dog-park-subpipeline-scope]].';

-- ───────────────────────────────────────────────────────────────────────────
-- 2. dog_park_day_hourly_scores
-- ───────────────────────────────────────────────────────────────────────────

create table public.dog_park_day_hourly_scores (
  dog_park_fid          bigint not null references public.dog_parks_gold(fid) on delete cascade,
  local_date            date not null,
  forecast_ts           timestamptz not null,
  local_hour            integer not null,
  hour_label            text not null,
  is_daylight           boolean not null,
  is_candidate_window   boolean not null default false,
  is_in_best_window     boolean not null default false,
  is_now                boolean not null default false,

  -- Inputs (no tide_height, no busyness_score, no sand_temp)
  weather_code          integer,
  temp_air              numeric,
  feels_like            numeric,
  wind_speed            numeric,
  precip_chance         numeric,
  uv_index              numeric,
  asphalt_temp          numeric, -- modulated by surface_lookup at scoring time

  -- Per-metric statuses
  hour_status           text not null,
  wind_status           text,
  rain_status           text,
  temp_status           text,
  temp_cold_status      text,
  temp_hot_status       text,
  uv_status             text,
  asphalt_status        text,
  surface_status        text, -- NEW: result of rain × surface rescoring tier

  -- Per-metric scores
  hour_score            numeric,
  wind_score            numeric,
  rain_score            numeric,
  temp_score            numeric,
  uv_score              numeric,
  weather_score         numeric,
  surface_score         numeric, -- NEW

  -- Reasoning + provenance
  positive_reason_codes jsonb not null default '[]'::jsonb,
  risk_reason_codes     jsonb not null default '[]'::jsonb,
  explainability        jsonb not null default '{}'::jsonb,
  hour_text             text,

  -- Metadata
  timezone              text not null,
  scoring_version       text not null,
  generated_at          timestamptz not null default now(),
  updated_at            timestamptz not null default now(),
  weather_refreshed_at  timestamptz,

  primary key (dog_park_fid, forecast_ts)
);

create index dpdhs_local_date_idx     on public.dog_park_day_hourly_scores (dog_park_fid, local_date);
create index dpdhs_is_now_idx         on public.dog_park_day_hourly_scores (dog_park_fid) where is_now;
create index dpdhs_in_best_window_idx on public.dog_park_day_hourly_scores (dog_park_fid, local_date) where is_in_best_window;

comment on table public.dog_park_day_hourly_scores is
  'Per-hour dog-park scores. Mirrors beach_day_hourly_scores minus tide/crowd/sand cols. Adds surface_score + surface_status from rain × surface rescoring. PK (dog_park_fid, forecast_ts).';

-- ───────────────────────────────────────────────────────────────────────────
-- 3. dog_park_day_recommendations
-- ───────────────────────────────────────────────────────────────────────────

create table public.dog_park_day_recommendations (
  dog_park_fid           bigint not null references public.dog_parks_gold(fid) on delete cascade,
  local_date             date not null,

  day_status             text not null,
  best_window_start_ts   timestamptz,
  best_window_end_ts     timestamptz,
  best_window_label      text,
  best_window_status     text,

  summary_weather        text,
  weather_code           integer,
  avg_temp               numeric,
  avg_feels_like         numeric,
  avg_wind               numeric,
  avg_uv                 numeric,

  -- Hours counts
  go_hours_count         integer not null default 0,
  advisory_hours_count   integer not null default 0,
  caution_hours_count    integer not null default 0,
  no_go_hours_count      integer not null default 0,

  -- Reasoning
  positive_reason_codes  jsonb not null default '[]'::jsonb,
  risk_reason_codes      jsonb not null default '[]'::jsonb,
  day_text               text,
  caution_text           text,
  no_go_text             text,
  best_window_text       text,

  -- Provenance
  hourly_source_max_ts   timestamptz,
  daily_source_date      date,
  timezone               text not null,
  scoring_version        text not null,
  generated_at           timestamptz not null default now(),
  updated_at             timestamptz not null default now(),

  primary key (dog_park_fid, local_date)
);

create index dpdr_local_date_idx on public.dog_park_day_recommendations (local_date);

comment on table public.dog_park_day_recommendations is
  'Daily dog-park rollups. Mirrors beach_day_recommendations minus tide/crowd/sand cols. PK (dog_park_fid, local_date).';

-- ───────────────────────────────────────────────────────────────────────────
-- 4. is_scoreable column on dog_parks_gold (universal-coverage gate)
-- ───────────────────────────────────────────────────────────────────────────
-- Per [[dog-park-subpipeline-scope]] is_scoreable decision: all 351 CA active
-- dog parks are scoreable; opt-out only for known-bad rows.
alter table public.dog_parks_gold
  add column if not exists is_scoreable boolean not null default true;

create index if not exists dpg_scoreable_idx
  on public.dog_parks_gold (state) where is_scoreable and is_active;

-- ───────────────────────────────────────────────────────────────────────────
-- 5. RLS — anon-key read access (same pattern as beach scoring tables)
-- ───────────────────────────────────────────────────────────────────────────

alter table public.dog_park_scoring_config        enable row level security;
alter table public.dog_park_day_hourly_scores     enable row level security;
alter table public.dog_park_day_recommendations   enable row level security;

create policy dpsc_anon_select  on public.dog_park_scoring_config        for select to anon, authenticated using (true);
create policy dpdhs_anon_select on public.dog_park_day_hourly_scores     for select to anon, authenticated using (true);
create policy dpdr_anon_select  on public.dog_park_day_recommendations   for select to anon, authenticated using (true);

commit;
