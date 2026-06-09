-- 20260609a — V3 score shadow columns
--
-- WHY: Score-reform arc. Plan at
-- C:\Users\beach\.claude\plans\yes-commit-and-plan-cheeky-peach.md.
-- V3 is the new formula (no status caps, uncapped neg buckets,
-- bimodal tide + crowd, marine signals integrated). To verify
-- without flipping reads we run V3 in shadow alongside V2 for a
-- verification window, diff the columns, then flip get_beach_info
-- + find_beaches to read V3.
--
-- This migration is behavior-neutral — just adds nullable columns.
-- Writer wiring + formula changes follow in subsequent migrations.

ALTER TABLE public.beach_day_hourly_scores
  ADD COLUMN IF NOT EXISTS hour_score_v3 numeric;

ALTER TABLE public.beach_day_recommendations
  ADD COLUMN IF NOT EXISTS composite_score_v3 numeric;

COMMENT ON COLUMN public.beach_day_hourly_scores.hour_score_v3 IS
  'Shadow V3 score (no caps, uncapped neg buckets, bimodal tide/crowd, marine-aware). Validation column during score-reform rollout 2026-06-09. Once verified against hour_score_v2, get_beach_info flips to read V3 and V2 is dropped.';

COMMENT ON COLUMN public.beach_day_recommendations.composite_score_v3 IS
  'Shadow V3 composite (same reform as hour_score_v3). Used to verify find.html rank churn before flipping find_beaches read.';
