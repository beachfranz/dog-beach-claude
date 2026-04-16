-- ============================================================
-- Dog Beach Scout — scoring_config seed data
-- Captured: 2026-04-15  (updated_at: 2026-04-15 23:31:13 UTC)
-- ============================================================

INSERT INTO public.scoring_config (
  id,
  scoring_version,
  effective_from,
  description,
  is_active,

  -- No-go thresholds
  nogo_precip_chance,
  nogo_wind_speed,
  nogo_wmo_codes,
  nogo_temp_min,
  nogo_temp_max,

  -- Caution thresholds
  caution_precip_chance,
  caution_wind_speed,
  caution_tide_height,
  caution_uv_index,
  caution_temp_min,
  caution_temp_max,

  -- Positive signal thresholds
  positive_low_tide,
  positive_very_low_tide,
  positive_low_precip,
  positive_calm_wind,
  positive_temp_min,
  positive_temp_max,
  positive_low_uv,

  -- Busyness category boundaries
  busy_quiet_max,
  busy_moderate_max,
  busy_dog_party_max,

  -- Component weights
  weight_tide,
  weight_rain,
  weight_wind,
  weight_crowd,
  weight_temp,
  weight_uv,

  -- Normalisation ranges
  norm_tide_max,
  norm_wind_max,
  norm_temp_target,
  norm_temp_range,
  norm_uv_max,

  -- Best-window selection
  window_min_hours,
  window_max_hours,
  window_caution_penalty,
  window_score_threshold
) VALUES (
  '7677ca07-616d-4caf-9efc-3b451a8e5116',
  'v1',
  '2026-01-01',
  'Initial scoring configuration for Dog Beach Scout. Tide weighted highest (30%) — low tide is the primary visit driver. Rain second (25%) — overcast/drizzle is a strong deterrent with dogs. Wind third (20%). Crowd fourth (15%). Temp and UV cosmetic (5% each).',
  true,

  -- No-go
  70, 25,
  '{95,96,97,98,99,63,64,65,66,67,71,72,73,74,75,76,77}',
  50, 90,

  -- Caution
  40, 15, 3.5, 8, 63, 85,

  -- Positive signals
  1.0, 0.5, 15, 8, 65, 80, 4,

  -- Busyness
  30, 60, 84,

  -- Weights
  0.30, 0.25, 0.20, 0.15, 0.05, 0.05,

  -- Norm ranges
  4.0, 25, 72, 30, 11,

  -- Window
  2, 5, 5.0, 0.93
)
ON CONFLICT (id) DO UPDATE SET
  updated_at              = now(),
  nogo_precip_chance      = EXCLUDED.nogo_precip_chance,
  nogo_wind_speed         = EXCLUDED.nogo_wind_speed,
  nogo_wmo_codes          = EXCLUDED.nogo_wmo_codes,
  nogo_temp_min           = EXCLUDED.nogo_temp_min,
  nogo_temp_max           = EXCLUDED.nogo_temp_max,
  caution_precip_chance   = EXCLUDED.caution_precip_chance,
  caution_wind_speed      = EXCLUDED.caution_wind_speed,
  caution_tide_height     = EXCLUDED.caution_tide_height,
  caution_uv_index        = EXCLUDED.caution_uv_index,
  caution_temp_min        = EXCLUDED.caution_temp_min,
  caution_temp_max        = EXCLUDED.caution_temp_max,
  positive_low_tide       = EXCLUDED.positive_low_tide,
  positive_very_low_tide  = EXCLUDED.positive_very_low_tide,
  positive_low_precip     = EXCLUDED.positive_low_precip,
  positive_calm_wind      = EXCLUDED.positive_calm_wind,
  positive_temp_min       = EXCLUDED.positive_temp_min,
  positive_temp_max       = EXCLUDED.positive_temp_max,
  positive_low_uv         = EXCLUDED.positive_low_uv,
  busy_quiet_max          = EXCLUDED.busy_quiet_max,
  busy_moderate_max       = EXCLUDED.busy_moderate_max,
  busy_dog_party_max      = EXCLUDED.busy_dog_party_max,
  weight_tide             = EXCLUDED.weight_tide,
  weight_rain             = EXCLUDED.weight_rain,
  weight_wind             = EXCLUDED.weight_wind,
  weight_crowd            = EXCLUDED.weight_crowd,
  weight_temp             = EXCLUDED.weight_temp,
  weight_uv               = EXCLUDED.weight_uv,
  norm_tide_max           = EXCLUDED.norm_tide_max,
  norm_wind_max           = EXCLUDED.norm_wind_max,
  norm_temp_target        = EXCLUDED.norm_temp_target,
  norm_temp_range         = EXCLUDED.norm_temp_range,
  norm_uv_max             = EXCLUDED.norm_uv_max,
  window_min_hours        = EXCLUDED.window_min_hours,
  window_max_hours        = EXCLUDED.window_max_hours,
  window_caution_penalty  = EXCLUDED.window_caution_penalty,
  window_score_threshold  = EXCLUDED.window_score_threshold;
