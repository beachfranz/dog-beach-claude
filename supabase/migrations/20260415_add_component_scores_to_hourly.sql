-- Add individual component score columns to beach_day_hourly_scores
ALTER TABLE public.beach_day_hourly_scores
  ADD COLUMN IF NOT EXISTS tide_score  numeric(4,3),
  ADD COLUMN IF NOT EXISTS wind_score  numeric(4,3),
  ADD COLUMN IF NOT EXISTS crowd_score numeric(4,3),
  ADD COLUMN IF NOT EXISTS rain_score  numeric(4,3),
  ADD COLUMN IF NOT EXISTS temp_score  numeric(4,3),
  ADD COLUMN IF NOT EXISTS uv_score    numeric(4,3);
