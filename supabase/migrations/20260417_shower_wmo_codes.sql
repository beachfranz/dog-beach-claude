-- Add rain shower WMO codes to caution list.
-- 80 (slight showers) and 81 (moderate showers) → caution.
-- 82 (violent showers) → no_go, handled in SEVERE_WMO_CODES hardcode in scoring.ts.
-- Previously all three defaulted to "go" via fallthrough — wrong.
UPDATE public.scoring_config
SET caution_wmo_codes = array(
      SELECT DISTINCT unnest(caution_wmo_codes || ARRAY[80, 81]) ORDER BY 1
    ),
    updated_at = now()
WHERE is_active = true;
