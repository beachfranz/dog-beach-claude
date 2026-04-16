-- ============================================================
-- Dog Beach Scout — full schema snapshot
-- Captured: 2026-04-15
-- Use this to recreate the database from scratch if needed.
-- Run in order: schema first, then seed_*.sql files.
-- ============================================================

-- ── beaches ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.beaches (
  location_id         text          PRIMARY KEY,
  display_name        text          NOT NULL,
  latitude            numeric       NOT NULL,
  longitude           numeric       NOT NULL,
  noaa_station_id     text,
  besttime_venue_id   text,
  timezone            text          DEFAULT 'America/Los_Angeles',
  open_time           text,
  close_time          text,
  address             text,
  website             text,
  description         text,
  parking_text        text,
  leash_policy        text,
  dog_rules           text,
  amenities           text,
  restrooms           text,
  location_numb       integer,
  is_active           boolean       DEFAULT true,
  created_at          timestamptz   DEFAULT now()
);

-- ── scoring_config ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.scoring_config (
  id                      uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  scoring_version         text        NOT NULL,
  effective_from          date        NOT NULL,
  description             text,
  is_active               boolean     NOT NULL DEFAULT true,

  -- No-go thresholds
  nogo_precip_chance      numeric     NOT NULL DEFAULT 70,
  nogo_wind_speed         numeric     NOT NULL DEFAULT 25,
  nogo_wmo_codes          integer[]   NOT NULL DEFAULT '{95,96,97,98,99,63,64,65,66,67,71,72,73,74,75,76,77}',
  nogo_temp_min           numeric              DEFAULT 50,
  nogo_temp_max           numeric              DEFAULT 90,

  -- Caution thresholds
  caution_precip_chance   numeric     NOT NULL DEFAULT 40,
  caution_wind_speed      numeric     NOT NULL DEFAULT 15,
  caution_tide_height     numeric     NOT NULL DEFAULT 3.5,
  caution_uv_index        numeric     NOT NULL DEFAULT 8,
  caution_temp_min        numeric              DEFAULT 63,
  caution_temp_max        numeric              DEFAULT 85,

  -- Positive signal thresholds
  positive_low_tide       numeric     NOT NULL DEFAULT 1.0,
  positive_very_low_tide  numeric     NOT NULL DEFAULT 0.5,
  positive_low_precip     numeric     NOT NULL DEFAULT 15,
  positive_calm_wind      numeric     NOT NULL DEFAULT 8,
  positive_temp_min       numeric     NOT NULL DEFAULT 65,
  positive_temp_max       numeric     NOT NULL DEFAULT 80,
  positive_low_uv         numeric     NOT NULL DEFAULT 4,

  -- Busyness category boundaries (0–100 scale)
  busy_quiet_max          numeric     NOT NULL DEFAULT 30,
  busy_moderate_max       numeric     NOT NULL DEFAULT 60,
  busy_dog_party_max      numeric     NOT NULL DEFAULT 84,

  -- Component weights (must sum to 1.0)
  weight_tide             numeric     NOT NULL DEFAULT 0.30,
  weight_rain             numeric     NOT NULL DEFAULT 0.25,
  weight_wind             numeric     NOT NULL DEFAULT 0.20,
  weight_crowd            numeric     NOT NULL DEFAULT 0.15,
  weight_temp             numeric     NOT NULL DEFAULT 0.05,
  weight_uv               numeric     NOT NULL DEFAULT 0.05,

  -- Normalisation ranges
  norm_tide_max           numeric     NOT NULL DEFAULT 4.0,
  norm_wind_max           numeric     NOT NULL DEFAULT 25,
  norm_temp_target        numeric     NOT NULL DEFAULT 72,
  norm_temp_range         numeric     NOT NULL DEFAULT 30,
  norm_uv_max             numeric     NOT NULL DEFAULT 11,

  -- Best-window selection
  window_min_hours        integer     NOT NULL DEFAULT 2,
  window_max_hours        integer     NOT NULL DEFAULT 5,
  window_caution_penalty  numeric     NOT NULL DEFAULT 5.0,
  window_score_threshold  numeric              DEFAULT 0.93,

  created_at              timestamptz NOT NULL DEFAULT now(),
  updated_at              timestamptz NOT NULL DEFAULT now()
);

-- ── beach_day_hourly_scores ───────────────────────────────────
CREATE TABLE IF NOT EXISTS public.beach_day_hourly_scores (
  location_id             text        NOT NULL REFERENCES public.beaches(location_id),
  local_date              date        NOT NULL,
  forecast_ts             timestamptz NOT NULL,
  local_hour              integer     NOT NULL,
  hour_label              text        NOT NULL,

  -- Availability flags
  is_daylight             boolean     NOT NULL DEFAULT false,
  is_candidate_window     boolean     NOT NULL DEFAULT false,
  is_in_best_window       boolean     NOT NULL DEFAULT false,

  -- Raw weather/conditions inputs
  weather_code            integer,
  temp_air                numeric,
  wind_speed              numeric,
  precip_chance           numeric,
  uv_index                numeric,
  tide_height             numeric,
  busyness_score          numeric,
  busyness_category       text,

  -- Scoring outputs
  hour_status             text        NOT NULL,
  hour_score              numeric,

  -- Explainability
  passed_checks           jsonb       NOT NULL DEFAULT '[]',
  failed_checks           jsonb       NOT NULL DEFAULT '[]',
  positive_reason_codes   jsonb       NOT NULL DEFAULT '[]',
  risk_reason_codes       jsonb       NOT NULL DEFAULT '[]',
  explainability          jsonb       NOT NULL DEFAULT '{}',
  hour_text               text,

  -- Per-metric component scores
  tide_score              numeric,
  wind_score              numeric,
  crowd_score             numeric,
  rain_score              numeric,
  temp_score              numeric,
  uv_score                numeric,

  -- Per-metric statuses
  tide_status             text,
  wind_status             text,
  crowd_status            text,
  rain_status             text,
  temp_status             text,
  uv_status               text,

  -- Metadata
  timezone                text        NOT NULL DEFAULT 'America/Los_Angeles',
  scoring_version         text        NOT NULL DEFAULT 'v1',
  generated_at            timestamptz NOT NULL DEFAULT now(),
  updated_at              timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (location_id, forecast_ts)
);

-- ── beach_day_recommendations ─────────────────────────────────
CREATE TABLE IF NOT EXISTS public.beach_day_recommendations (
  location_id             text        NOT NULL REFERENCES public.beaches(location_id),
  local_date              date        NOT NULL,

  day_status              text        NOT NULL,

  -- Best window
  best_window_start_ts    timestamptz,
  best_window_end_ts      timestamptz,
  best_window_label       text,
  best_window_status      text,

  -- Day-level weather summary
  summary_weather         text,
  weather_code            integer,
  avg_temp                numeric,
  avg_wind                numeric,
  avg_uv                  numeric,
  avg_tide_height         numeric,
  lowest_tide_height      numeric,
  avg_busyness_score      numeric,
  busyness_category       text,

  -- Hour counts
  go_hours_count          integer     NOT NULL DEFAULT 0,
  caution_hours_count     integer     NOT NULL DEFAULT 0,
  no_go_hours_count       integer     NOT NULL DEFAULT 0,

  -- Reason codes
  positive_reason_codes   jsonb       NOT NULL DEFAULT '[]',
  risk_reason_codes       jsonb       NOT NULL DEFAULT '[]',
  explainability          jsonb       NOT NULL DEFAULT '{}',
  thresholds_used         jsonb       NOT NULL DEFAULT '{}',

  -- Narrative text
  day_text                text,
  caution_text            text,
  no_go_text              text,
  best_window_text        text,

  -- Source freshness
  hourly_source_max_ts    timestamptz,
  crowd_source_max_ts     timestamptz,
  daily_source_date       date,

  -- Metadata
  timezone                text        NOT NULL DEFAULT 'America/Los_Angeles',
  scoring_version         text        NOT NULL DEFAULT 'v1',
  generated_at            timestamptz NOT NULL DEFAULT now(),
  updated_at              timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (location_id, local_date)
);

-- ── subscribers ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.subscribers (
  id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  phone_e164      text        NOT NULL UNIQUE,
  display_name    text,
  location_ids    text[]      NOT NULL DEFAULT '{}',
  notify_time     time        NOT NULL DEFAULT '07:00:00',
  timezone        text        NOT NULL DEFAULT 'America/Los_Angeles',
  is_active       boolean     NOT NULL DEFAULT true,
  opted_out_at    timestamptz,
  opted_out_reason text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);

-- ── subscriber_locations ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.subscriber_locations (
  subscriber_id   uuid        NOT NULL REFERENCES public.subscribers(id),
  location_id     text        NOT NULL REFERENCES public.beaches(location_id),
  added_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (subscriber_id, location_id)
);

-- ── notification_log ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.notification_log (
  id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  subscriber_id   uuid        REFERENCES public.subscribers(id),
  location_id     text        REFERENCES public.beaches(location_id),
  sent_at         timestamptz NOT NULL DEFAULT now(),
  message_sid     text,
  status          text,
  payload         jsonb       DEFAULT '{}'
);

-- ── refresh_errors ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.refresh_errors (
  id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  location_id     text,
  error_message   text,
  error_detail    jsonb       DEFAULT '{}',
  occurred_at     timestamptz NOT NULL DEFAULT now()
);
