-- Add weather_code as a scored component (15% weight).
-- Tide drops 30% → 22.5%, rain drops 25% → 17.5% to fund it.

-- 1. New column on hourly scores table
ALTER TABLE public.beach_day_hourly_scores
  ADD COLUMN IF NOT EXISTS weather_score numeric;

-- 2. Add weight column defaulting to 0 so existing row still sums to 1.0
ALTER TABLE public.scoring_config
  ADD COLUMN IF NOT EXISTS weight_weather_code numeric NOT NULL DEFAULT 0;

-- 3. Replace the weights check constraint to include the new component
ALTER TABLE public.scoring_config
  DROP CONSTRAINT IF EXISTS scoring_config_weights_sum;

ALTER TABLE public.scoring_config
  ADD CONSTRAINT scoring_config_weights_sum CHECK (
    round(weight_tide + weight_rain + weight_wind + weight_crowd + weight_temp + weight_uv + weight_weather_code, 10) = 1.0
  );

-- 4. Rebalance all weights in one statement so the constraint stays satisfied
UPDATE public.scoring_config
SET weight_tide         = 0.225,
    weight_rain         = 0.175,
    weight_weather_code = 0.15,
    updated_at          = now()
WHERE is_active = true;
