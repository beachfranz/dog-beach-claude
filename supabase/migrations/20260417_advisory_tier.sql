-- ============================================================
-- Advisory tier + surface temps + feels-like
-- Adds:
--   • feels_like, sand_temp, asphalt_temp + statuses to hourly scores
--   • advisory_hours_count, avg_feels_like to daily recommendations
--   • advisory thresholds + caution WMO codes to scoring_config
-- ============================================================

-- ── beach_day_hourly_scores ───────────────────────────────────
ALTER TABLE public.beach_day_hourly_scores
  ADD COLUMN IF NOT EXISTS feels_like       numeric,
  ADD COLUMN IF NOT EXISTS sand_temp        numeric,
  ADD COLUMN IF NOT EXISTS asphalt_temp     numeric,
  ADD COLUMN IF NOT EXISTS sand_status      text,
  ADD COLUMN IF NOT EXISTS asphalt_status   text;

-- ── beach_day_recommendations ─────────────────────────────────
ALTER TABLE public.beach_day_recommendations
  ADD COLUMN IF NOT EXISTS advisory_hours_count  integer  NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS avg_feels_like        numeric;

-- ── scoring_config ────────────────────────────────────────────

-- Caution WMO codes (drizzle, slight rain, fog — moved out of nogo)
ALTER TABLE public.scoring_config
  ADD COLUMN IF NOT EXISTS caution_wmo_codes  integer[]  NOT NULL
    DEFAULT '{45,48,51,53,55,56,57,61}';

-- Advisory tier thresholds — crowd (busyness score bounds)
ALTER TABLE public.scoring_config
  ADD COLUMN IF NOT EXISTS advisory_crowd_min  numeric  NOT NULL DEFAULT 31,
  ADD COLUMN IF NOT EXISTS advisory_crowd_max  numeric  NOT NULL DEFAULT 60;

-- Advisory / caution / nogo thresholds for sand temp (°F)
ALTER TABLE public.scoring_config
  ADD COLUMN IF NOT EXISTS advisory_sand_temp  numeric  NOT NULL DEFAULT 105,
  ADD COLUMN IF NOT EXISTS caution_sand_temp   numeric  NOT NULL DEFAULT 115,
  ADD COLUMN IF NOT EXISTS nogo_sand_temp      numeric  NOT NULL DEFAULT 125;

-- Advisory / caution / nogo thresholds for asphalt temp (°F)
ALTER TABLE public.scoring_config
  ADD COLUMN IF NOT EXISTS advisory_asphalt_temp  numeric  NOT NULL DEFAULT 105,
  ADD COLUMN IF NOT EXISTS caution_asphalt_temp   numeric  NOT NULL DEFAULT 115,
  ADD COLUMN IF NOT EXISTS nogo_asphalt_temp      numeric  NOT NULL DEFAULT 125;

-- Update nogo_wmo_codes on the active config row to remove codes
-- now reclassified as caution (45,48,51,53,55,56,57,61)
UPDATE public.scoring_config
SET nogo_wmo_codes = ARRAY(
  SELECT unnest(nogo_wmo_codes)
  EXCEPT
  SELECT unnest(ARRAY[45,48,51,53,55,56,57,61]::integer[])
)
WHERE is_active = true;
