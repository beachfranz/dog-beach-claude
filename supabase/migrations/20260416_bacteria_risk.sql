-- ============================================================
-- Bacteria risk: recent rainfall tracking
-- Adds precip totals + risk level to beach_day_recommendations.
-- Adds advisory thresholds to scoring_config.
-- Standard: 0.1" (2.5mm) in 72h triggers SoCal beach advisory.
-- ============================================================

-- ── beach_day_recommendations ─────────────────────────────────
ALTER TABLE public.beach_day_recommendations
  ADD COLUMN IF NOT EXISTS precip_24h_mm  numeric,
  ADD COLUMN IF NOT EXISTS precip_72h_mm  numeric,
  ADD COLUMN IF NOT EXISTS bacteria_risk  text;

-- ── scoring_config ────────────────────────────────────────────
ALTER TABLE public.scoring_config
  ADD COLUMN IF NOT EXISTS bacteria_caution_mm  numeric  NOT NULL DEFAULT 2.5,
  ADD COLUMN IF NOT EXISTS bacteria_nogo_mm     numeric  NOT NULL DEFAULT 25.0;
