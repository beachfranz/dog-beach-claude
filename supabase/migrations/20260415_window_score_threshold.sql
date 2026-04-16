-- Replace fixed window sizing with score-threshold-based window selection
ALTER TABLE public.scoring_config
  ADD COLUMN IF NOT EXISTS window_score_threshold numeric(4,2) DEFAULT 0.93;

UPDATE public.scoring_config
  SET window_score_threshold = 0.93
WHERE is_active = true;
