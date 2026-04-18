-- Window selection v2: run-first algorithm with peak oversampling,
-- consistency bonus, and soft length cap

ALTER TABLE public.scoring_config
  ADD COLUMN IF NOT EXISTS window_peak_oversample_n   NUMERIC DEFAULT 1,
  ADD COLUMN IF NOT EXISTS window_consistency_weight  NUMERIC DEFAULT 0.10,
  ADD COLUMN IF NOT EXISTS window_length_penalty      NUMERIC DEFAULT 0.05,
  ADD COLUMN IF NOT EXISTS window_soft_cap_hours      NUMERIC DEFAULT 4;

UPDATE public.scoring_config
SET
  window_peak_oversample_n  = 1,
  window_consistency_weight = 0.10,
  window_length_penalty     = 0.05,
  window_soft_cap_hours     = 4
WHERE is_active = true;
