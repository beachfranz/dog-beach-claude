-- Remove columns that are written but never read by any function, page, or Scout

ALTER TABLE public.beach_day_hourly_scores
  DROP COLUMN IF EXISTS passed_checks,
  DROP COLUMN IF EXISTS failed_checks;

ALTER TABLE public.beach_day_recommendations
  DROP COLUMN IF EXISTS explainability,
  DROP COLUMN IF EXISTS thresholds_used,
  DROP COLUMN IF EXISTS crowd_source_max_ts;
