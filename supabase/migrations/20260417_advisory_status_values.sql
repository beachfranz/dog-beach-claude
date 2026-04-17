-- ============================================================
-- Add 'advisory' to hour_status and day_status check constraints
-- ============================================================

ALTER TABLE public.beach_day_hourly_scores
  DROP CONSTRAINT beach_day_hourly_scores_hour_status_check,
  ADD CONSTRAINT beach_day_hourly_scores_hour_status_check
    CHECK (hour_status = ANY (ARRAY['go','advisory','caution','no_go']));

ALTER TABLE public.beach_day_recommendations
  DROP CONSTRAINT beach_day_recommendations_day_status_check,
  ADD CONSTRAINT beach_day_recommendations_day_status_check
    CHECK (day_status = ANY (ARRAY['go','advisory','caution','no_go']));

ALTER TABLE public.beach_day_recommendations
  DROP CONSTRAINT beach_day_recommendations_best_window_status_check,
  ADD CONSTRAINT beach_day_recommendations_best_window_status_check
    CHECK (best_window_status = ANY (ARRAY['go','advisory','caution','no_go']));
