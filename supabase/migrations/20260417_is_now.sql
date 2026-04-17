-- ============================================================
-- Add is_now flag to beach_day_hourly_scores
-- One row per beach marked true — the most recently refreshed
-- hour with actual observed conditions (overwrites forecast).
-- ============================================================

ALTER TABLE public.beach_day_hourly_scores
  ADD COLUMN IF NOT EXISTS is_now boolean NOT NULL DEFAULT false;

-- Partial index for fast lookup of the single NOW row per beach
CREATE INDEX IF NOT EXISTS beach_day_hourly_scores_is_now_idx
  ON public.beach_day_hourly_scores(location_id)
  WHERE is_now = true;
