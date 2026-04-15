-- Add temp thresholds to scoring_config
ALTER TABLE public.scoring_config
  ADD COLUMN IF NOT EXISTS caution_temp_min numeric(5,1) DEFAULT 63,
  ADD COLUMN IF NOT EXISTS caution_temp_max numeric(5,1) DEFAULT 85,
  ADD COLUMN IF NOT EXISTS nogo_temp_min    numeric(5,1) DEFAULT 50,
  ADD COLUMN IF NOT EXISTS nogo_temp_max    numeric(5,1) DEFAULT 90;

-- Update active config with the defined thresholds
UPDATE public.scoring_config SET
  caution_temp_min = 63,
  caution_temp_max = 85,
  nogo_temp_min    = 50,
  nogo_temp_max    = 90
WHERE is_active = true;

-- Add per-metric status columns to beach_day_hourly_scores
ALTER TABLE public.beach_day_hourly_scores
  ADD COLUMN IF NOT EXISTS tide_status  text,
  ADD COLUMN IF NOT EXISTS wind_status  text,
  ADD COLUMN IF NOT EXISTS crowd_status text,
  ADD COLUMN IF NOT EXISTS rain_status  text,
  ADD COLUMN IF NOT EXISTS temp_status  text,
  ADD COLUMN IF NOT EXISTS uv_status    text;
