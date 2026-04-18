-- Raise UV advisory threshold from 3 → 6.
-- UV 3 (WHO "moderate") fires on nearly every clear SoCal afternoon and became noise.
-- UV 6 (WHO "high") is a meaningful signal worth surfacing to users.
UPDATE public.scoring_config
SET advisory_uv_index = 6,
    updated_at        = now()
WHERE is_active = true;
