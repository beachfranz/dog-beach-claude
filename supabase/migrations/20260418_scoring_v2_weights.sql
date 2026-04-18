-- Scoring v2: status multipliers + temp-dependent wind ideal
-- Adds config columns for:
--   • per-status sub-score multipliers (advisory/caution/no_go)
--   • temp-zone wind ideal speeds and falloff range

ALTER TABLE public.scoring_config
  ADD COLUMN IF NOT EXISTS status_mult_advisory    NUMERIC DEFAULT 0.90,
  ADD COLUMN IF NOT EXISTS status_mult_caution     NUMERIC DEFAULT 0.75,
  ADD COLUMN IF NOT EXISTS status_mult_nogo        NUMERIC DEFAULT 0.50,
  ADD COLUMN IF NOT EXISTS norm_wind_falloff       NUMERIC DEFAULT 20,
  ADD COLUMN IF NOT EXISTS norm_wind_ideal_cold    NUMERIC DEFAULT 2,
  ADD COLUMN IF NOT EXISTS norm_wind_ideal_comfortable NUMERIC DEFAULT 8,
  ADD COLUMN IF NOT EXISTS norm_wind_ideal_hot     NUMERIC DEFAULT 15,
  ADD COLUMN IF NOT EXISTS norm_wind_temp_cold_max NUMERIC DEFAULT 55,
  ADD COLUMN IF NOT EXISTS norm_wind_temp_hot_min  NUMERIC DEFAULT 80;

-- Backfill active row with explicit values (defaults already set above)
UPDATE public.scoring_config
SET
  status_mult_advisory        = 0.90,
  status_mult_caution         = 0.75,
  status_mult_nogo            = 0.50,
  norm_wind_falloff           = 20,
  norm_wind_ideal_cold        = 2,
  norm_wind_ideal_comfortable = 8,
  norm_wind_ideal_hot         = 15,
  norm_wind_temp_cold_max     = 55,
  norm_wind_temp_hot_min      = 80
WHERE is_active = true;
