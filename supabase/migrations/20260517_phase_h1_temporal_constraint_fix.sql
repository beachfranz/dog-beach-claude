-- Phase H1 follow-up: relax temporal_window_kind_consistency CHECK to
-- allow dawn/dusk anchors in daily windows.
--
-- Original constraint required anchor_start/anchor_end IS NULL for
-- daily windows. But dawn/dusk are TIME-of-day anchors (resolve to
-- sunrise/sunset per beach coords); they belong with daily windows.
-- Other anchors (Labor Day, Memorial Day, plover, MLK, etc.) are DATE
-- anchors and only belong with seasonal windows. The semantics are
-- determined by which anchor — not by which column.

ALTER TABLE public.beach_policy_source_temporal
  DROP CONSTRAINT IF EXISTS temporal_window_kind_consistency;

ALTER TABLE public.beach_policy_source_temporal
  ADD CONSTRAINT temporal_window_kind_consistency CHECK (
    CASE window_kind
      WHEN 'seasonal' THEN
        -- Seasonal window: no daily times; date anchors OK (NOT dawn/dusk).
        daily_start IS NULL AND daily_end IS NULL
        AND (anchor_start IS NULL OR anchor_start NOT IN ('dawn','dusk'))
        AND (anchor_end   IS NULL OR anchor_end   NOT IN ('dawn','dusk'))
        AND (effective_from_md IS NOT NULL OR effective_to_md IS NOT NULL
             OR anchor_start IS NOT NULL OR anchor_end IS NOT NULL)

      WHEN 'daily' THEN
        -- Daily window: no seasonal range. Either daily_start/end (literal HH:MM)
        -- OR dawn/dusk anchors (which resolve to time-of-day at query time).
        effective_from_md IS NULL AND effective_to_md IS NULL
        AND (anchor_start IS NULL OR anchor_start IN ('dawn','dusk'))
        AND (anchor_end   IS NULL OR anchor_end   IN ('dawn','dusk'))
        AND (daily_start IS NOT NULL OR daily_end IS NOT NULL
             OR anchor_start IN ('dawn','dusk') OR anchor_end IN ('dawn','dusk'))

      WHEN 'seasonal_and_daily' THEN
        -- Compound: both halves present. Seasonal half uses date anchors,
        -- daily half uses literal times OR dawn/dusk anchors. Since anchors
        -- share columns, this constraint can't distinguish — accept any
        -- non-null seasonal hint + any non-null daily hint.
        (effective_from_md IS NOT NULL OR effective_to_md IS NOT NULL
         OR anchor_start IS NOT NULL OR anchor_end IS NOT NULL)
        AND (daily_start IS NOT NULL OR daily_end IS NOT NULL)
      ELSE FALSE
    END
  );

COMMENT ON CONSTRAINT temporal_window_kind_consistency
  ON public.beach_policy_source_temporal IS
  'Allow dawn/dusk anchors with daily windows (time-of-day anchors); '
  'restrict date anchors (labor_day etc) to seasonal windows. '
  'seasonal_and_daily accepts any combo since anchors share columns.';
