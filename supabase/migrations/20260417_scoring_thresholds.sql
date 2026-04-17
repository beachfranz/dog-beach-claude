-- ============================================================
-- Scoring threshold updates for four-tier status system
-- Adds advisory-tier thresholds for all metrics.
-- Updates existing thresholds that changed in the new grid.
-- ============================================================

-- ── New threshold columns ─────────────────────────────────────

ALTER TABLE public.scoring_config
  -- Rain advisory (new tier)
  ADD COLUMN IF NOT EXISTS advisory_precip_chance  numeric  NOT NULL DEFAULT 10,

  -- Wind advisory (new tier)
  ADD COLUMN IF NOT EXISTS advisory_wind_speed     numeric  NOT NULL DEFAULT 10,

  -- Tide advisory (new tier)
  ADD COLUMN IF NOT EXISTS advisory_tide_height    numeric  NOT NULL DEFAULT 3.0,

  -- UV: advisory + no-go (new tiers)
  ADD COLUMN IF NOT EXISTS advisory_uv_index       numeric  NOT NULL DEFAULT 3,
  ADD COLUMN IF NOT EXISTS nogo_uv_index           numeric  NOT NULL DEFAULT 11,

  -- Temperature: cold/hot split model
  ADD COLUMN IF NOT EXISTS go_temp_cold_min        numeric  NOT NULL DEFAULT 50,
  ADD COLUMN IF NOT EXISTS advisory_temp_cold_min  numeric  NOT NULL DEFAULT 32,
  ADD COLUMN IF NOT EXISTS caution_temp_cold_min   numeric  NOT NULL DEFAULT 20,
  ADD COLUMN IF NOT EXISTS advisory_temp_hot_max   numeric  NOT NULL DEFAULT 75,
  ADD COLUMN IF NOT EXISTS caution_temp_hot_max    numeric  NOT NULL DEFAULT 85,
  ADD COLUMN IF NOT EXISTS nogo_temp_hot_max       numeric  NOT NULL DEFAULT 95;

-- ── Update existing thresholds that changed ───────────────────

UPDATE public.scoring_config
SET
  -- Tide caution raised from 3.5ft → 5.0ft (advisory now at 3.0ft)
  caution_tide_height   = 5.0,

  -- Rain caution raised from 40% → 50% (advisory now at 10%)
  caution_precip_chance = 50,

  -- Crowd advisory bounds corrected: go=0-60, advisory=61-84, caution=85+
  advisory_crowd_min    = 61,
  advisory_crowd_max    = 84
WHERE is_active = true;

-- ── New columns for temp_cold/hot statuses on hourly scores ───

ALTER TABLE public.beach_day_hourly_scores
  ADD COLUMN IF NOT EXISTS temp_cold_status  text,
  ADD COLUMN IF NOT EXISTS temp_hot_status   text;
