-- water-conditions advisory layer — Phase 1 tables
-- Per docs/water_conditions_advisory_spec.md + Franz 2026-05-19.
--
-- Two measurements-class tables ([[entity-modeling]]):
--   beach_marine_forecast — Open-Meteo marine (hourly forecasts)
--   beach_active_alert    — NWS active alerts after polygon PIP
--
-- Both feed the existing deterministic advisory engine
-- (buildCautionsSection in beach.html + mobile-beach.html) per spec's
-- collapsed-architecture intent: NOT a separate cautions strip.

BEGIN;

-- ─── 1. Marine forecast (hourly, per beach) ─────────────────────────

CREATE TABLE IF NOT EXISTS public.beach_marine_forecast (
  beach_fid          bigint      NOT NULL REFERENCES public.beaches_gold(fid) ON DELETE CASCADE,
  ts                 timestamptz NOT NULL,
  -- Open-Meteo marine variables
  wave_height_m      numeric,
  wave_period_s      numeric,
  wave_direction_deg numeric,
  swell_wave_height_m   numeric,
  swell_wave_period_s   numeric,
  wind_wave_height_m    numeric,
  wind_wave_period_s    numeric,
  sst_c              numeric,    -- sea surface temperature
  ocean_current_velocity_ms numeric,
  -- Derived (computed at fetch time): air temp + solar angle + cloud cover + albedo
  -- Paws burn ~50°C. Worth including in the marine pass — small additional
  -- code, real user value, follows hot-asphalt precedent.
  sand_surface_c     numeric,
  -- Provenance
  source             text        NOT NULL DEFAULT 'open_meteo',
  fetched_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (beach_fid, ts)
);

COMMENT ON TABLE public.beach_marine_forecast IS
  'Hourly marine forecast per beach (waves, SST, wind, derived sand surface). '
  'Source: Open-Meteo marine API. Fed into the advisory engine via threshold '
  'layer (config/marine_thresholds.yaml). Spec: docs/water_conditions_advisory_spec.md.';

-- Time-series index for "current + next N hours" queries
CREATE INDEX IF NOT EXISTS beach_marine_forecast_ts_idx
  ON public.beach_marine_forecast (beach_fid, ts);

-- For purging old forecasts (if we ever decide rolling-window over keep-forever)
CREATE INDEX IF NOT EXISTS beach_marine_forecast_fetched_idx
  ON public.beach_marine_forecast (fetched_at);


-- ─── 2. Active NWS alerts (per beach × alert) ───────────────────────

CREATE TABLE IF NOT EXISTS public.beach_active_alert (
  beach_fid          bigint      NOT NULL REFERENCES public.beaches_gold(fid) ON DELETE CASCADE,
  alert_id           text        NOT NULL,    -- NWS-provided ID (alert.identifier)
  nws_event_type     text        NOT NULL,    -- e.g. 'Rip Current Statement'
  severity           text,                     -- as-received: 'Minor'|'Moderate'|'Severe'|'Extreme'|'Unknown'
  certainty          text,                     -- 'Observed'|'Likely'|'Possible'|'Unlikely'|'Unknown'
  urgency            text,                     -- 'Immediate'|'Expected'|'Future'|'Past'|'Unknown'
  headline           text,                     -- short NWS headline
  description        text,                     -- full NWS prose
  instruction        text,                     -- NWS-provided action instruction
  valid_from         timestamptz NOT NULL,
  valid_to           timestamptz NOT NULL,
  -- Translation (populated by Phase 2 adapter; NULL for v1 ingest)
  dog_impact_class   text,                     -- 'skip_swim'|'paws_warning'|'no_dogs_today'|...
  dog_impact_text    text,                     -- user-facing copy
  translation_source text,                     -- 'yaml_lookup'|'llm_auto'|'admin_curated'|'wildcard_pending'
  translation_confidence numeric,              -- self-rated 0-1 for llm_auto
  -- Provenance
  source             text        NOT NULL DEFAULT 'nws',
  fetched_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (beach_fid, alert_id)
);

COMMENT ON TABLE public.beach_active_alert IS
  'Active NWS alerts spatially matched to beaches via polygon PIP. '
  'Translation columns populated by Phase 2 (YAML map + LLM wildcard fallback). '
  'Auto-purge handled by valid_to check at render time. Source: NWS '
  'api.weather.gov/alerts/active. Spec: docs/water_conditions_advisory_spec.md.';

-- "Active right now for this beach" queries
CREATE INDEX IF NOT EXISTS beach_active_alert_validity_idx
  ON public.beach_active_alert (beach_fid, valid_from, valid_to);

-- Wildcard / admin-queue surfacing
CREATE INDEX IF NOT EXISTS beach_active_alert_pending_idx
  ON public.beach_active_alert (translation_source)
  WHERE translation_source = 'wildcard_pending';


-- ─── 3. Verification ────────────────────────────────────────────────

SELECT
  'beach_marine_forecast' AS table_name,
  (SELECT count(*) FROM information_schema.columns WHERE table_name='beach_marine_forecast' AND table_schema='public') AS columns
UNION ALL SELECT
  'beach_active_alert',
  (SELECT count(*) FROM information_schema.columns WHERE table_name='beach_active_alert' AND table_schema='public');

COMMIT;
