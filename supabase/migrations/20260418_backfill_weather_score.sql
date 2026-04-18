-- Backfill weather_score for all existing hourly rows from weather_code.
-- Mirrors the WMO_SCORES map in _shared/scoring.ts.
UPDATE public.beach_day_hourly_scores
SET weather_score = CASE weather_code
  WHEN 0  THEN 1.00
  WHEN 1  THEN 1.00
  WHEN 2  THEN 0.90
  WHEN 3  THEN 0.75
  WHEN 45 THEN 0.40
  WHEN 48 THEN 0.35
  WHEN 51 THEN 0.35
  WHEN 53 THEN 0.25
  WHEN 55 THEN 0.15
  WHEN 56 THEN 0.15
  WHEN 57 THEN 0.05
  WHEN 61 THEN 0.30
  WHEN 80 THEN 0.25
  WHEN 81 THEN 0.15
  ELSE 0.50  -- severe/unknown codes; these hours are no_go anyway
END
WHERE weather_score IS NULL;
