-- v3 advisory penalty: initial weight seed.
--
-- Phase B of the v3 advisory-driven scoring refactor (plan
-- delegated-splashing-lynx.md). Reviewed + approved 2026-06-15.
--
-- 31 beach + 13 dog_park = 44 rows. Helper is whitelist-mode
-- (20260615ca), so only these event_types contribute to penalty;
-- everything else is silently ignored.
--
-- DISABLED rows: beach_erosion_risk (kept in feed but explicitly
-- non-contributory per 20260611h).
--
-- Idempotent: ON CONFLICT updates everything but updated_at touches
-- only on actual change.

BEGIN;

WITH seed (entity_type, event_type, weight, category, enabled, notes) AS (
  VALUES
  -- ─── BEACH ─────────────────────────────────────────────────────────
  -- water_hazard (1.5 baseline; pier 0.75, cold_water_shock 1.25, erosion OFF)
  ('beach','rip_current_risk',         1.5,   'water_hazard', true,  'BHS-extracted + NWS SRF; severe rips dominate'),
  ('beach','sneaker_wave_risk',        1.5,   'water_hazard', true,  'BHS-extracted; surprise wave hazard'),
  ('beach','high_surf_risk',           1.5,   'water_hazard', true,  'BHS-extracted; large breaking waves'),
  ('beach','longshore_current_risk',   1.5,   'water_hazard', true,  'BHS-extracted; pull along shore'),
  ('beach','shore_break_risk',         1.5,   'water_hazard', true,  'BHS-extracted; waves breaking on sand'),
  ('beach','cold_water_shock_risk',    1.25,  'water_hazard', true,  'BHS-extracted; lower than rip but real'),
  ('beach','pier_hazard_risk',         0.75,  'water_hazard', true,  'BHS-extracted; localized hazard, not whole beach'),
  ('beach','beach_erosion_risk',       1.0,   'water_hazard', false, 'BHS-extracted; explicit DISABLED per 20260611h'),

  -- water_quality (2.0)
  ('beach','bacteria_risk',            2.0,   'water_quality', true, 'Deterministic bacteria; treat as severe'),

  -- water_comfort (0.75 baseline; advisory + powerful at 1.0; paw/choppy at 0.5)
  ('beach','swim_advisory',            1.0,   'water_comfort', true, 'Marine threshold; aggregate signal'),
  ('beach','powerful_surf',            1.0,   'water_comfort', true, 'Marine threshold; rough water'),
  ('beach','too_cold_swim',            0.75,  'water_comfort', true, 'Marine threshold; SST too low'),
  ('beach','too_warm_swim',            0.75,  'water_comfort', true, 'Marine threshold; SST too high'),
  ('beach','paw_cold',                 0.5,   'water_comfort', true, 'Marine threshold; paw temperature'),
  ('beach','choppy',                   0.5,   'water_comfort', true, 'Marine threshold; small-craft choppiness'),

  -- thermal (1.0)
  ('beach','sand_status',              1.0,   'thermal', true, 'Deterministic; hot/cold sand'),
  ('beach','asphalt_status',           1.0,   'thermal', true, 'Deterministic; hot/cold asphalt at parking'),
  ('beach','uv_status',                1.0,   'thermal', true, 'Deterministic; UV index'),
  ('beach','temp_hot_status',          1.0,   'thermal', true, 'Deterministic; high air temp'),
  ('beach','temp_cold_status',         1.0,   'thermal', true, 'Deterministic; low air temp'),
  ('beach','car_heat_status',          1.0,   'thermal', true, 'Deterministic; hot car interior'),
  ('beach','car_cold_status',          1.0,   'thermal', true, 'Deterministic; cold car interior'),

  -- WMO/air_safety (tiered)
  ('beach','lightning_status',         2.0,   'wmo', true, 'WMO; existential danger'),
  ('beach','freezing_precip_status',   1.5,   'wmo', true, 'WMO; ice + slip risk'),
  ('beach','heavy_rain_status',        0.5,   'wmo', true, 'WMO; visibility + comfort'),
  ('beach','snow_status',              0.5,   'wmo', true, 'WMO; visibility + access'),
  ('beach','fog_status',               0.25,  'wmo', true, 'WMO; cosmetic for most dogs'),

  -- comfort (0.5; wind 0.75)
  ('beach','tide_status',              0.5,   'comfort', true, 'Deterministic; availability not safety per 20260615a'),
  ('beach','wind_status',              0.75,  'comfort', true, 'Deterministic; sandblast + chill'),
  ('beach','rain_status',              0.5,   'comfort', true, 'Deterministic; precip probability'),
  ('beach','crowd_status',             0.5,   'comfort', true, 'BestTime; soft-removed but emitter alive'),

  -- ─── DOG_PARK (thermal/WMO + comfort + static) ─────────────────────
  ('dog_park','asphalt_status',        1.0,   'thermal', true,  'Path/parking surfaces'),
  ('dog_park','uv_status',             1.0,   'thermal', true,  ''),
  ('dog_park','temp_hot_status',       1.0,   'thermal', true,  ''),
  ('dog_park','temp_cold_status',      1.0,   'thermal', true,  ''),
  ('dog_park','car_heat_status',       1.0,   'thermal', true,  ''),
  ('dog_park','car_cold_status',       1.0,   'thermal', true,  ''),
  ('dog_park','wind_status',           0.75,  'comfort', true,  ''),
  ('dog_park','rain_status',           0.5,   'comfort', true,  ''),
  ('dog_park','lightning_status',      2.0,   'wmo',     true,  ''),
  ('dog_park','freezing_precip_status',1.5,   'wmo',     true,  ''),
  ('dog_park','heavy_rain_status',     0.5,   'wmo',     true,  ''),
  ('dog_park','snow_status',           0.5,   'wmo',     true,  ''),
  ('dog_park','fog_status',            0.25,  'wmo',     true,  ''),
  ('dog_park','unfenced_status',       1.0,   'static',  true,  'Persistent static attribute; not weather')
)
INSERT INTO public.advisory_score_config
  (entity_type, event_type, weight, category, enabled, notes, updated_at)
SELECT entity_type, event_type, weight::numeric, category, enabled,
       NULLIF(notes, ''), now()
  FROM seed
ON CONFLICT (entity_type, event_type) DO UPDATE
  SET weight     = EXCLUDED.weight,
      category   = EXCLUDED.category,
      enabled    = EXCLUDED.enabled,
      notes      = EXCLUDED.notes,
      updated_at = CASE
        WHEN advisory_score_config.weight   IS DISTINCT FROM EXCLUDED.weight
          OR advisory_score_config.category IS DISTINCT FROM EXCLUDED.category
          OR advisory_score_config.enabled  IS DISTINCT FROM EXCLUDED.enabled
          OR advisory_score_config.notes    IS DISTINCT FROM EXCLUDED.notes
        THEN now()
        ELSE advisory_score_config.updated_at
      END;

-- Verification
SELECT
  'total_rows'         AS metric, count(*)::text AS val
  FROM public.advisory_score_config
UNION ALL SELECT
  'beach_rows',        count(*)::text
  FROM public.advisory_score_config WHERE entity_type='beach'
UNION ALL SELECT
  'dog_park_rows',     count(*)::text
  FROM public.advisory_score_config WHERE entity_type='dog_park'
UNION ALL SELECT
  'disabled_rows',     count(*)::text
  FROM public.advisory_score_config WHERE enabled=false
UNION ALL SELECT
  'avg_beach_weight',  round(avg(weight),3)::text
  FROM public.advisory_score_config WHERE entity_type='beach' AND enabled=true
UNION ALL SELECT
  'smoke_total_now',
  -- Pick a beach with current active advisories
  public._advisory_penalty_total('beach',
    (SELECT ba.beach_fid FROM public.beach_advisory ba
      WHERE now() BETWEEN ba.valid_from AND ba.valid_to
      LIMIT 1),
    now()
  )::text
UNION ALL SELECT
  'unseeded_beach_events_in_db',
  (SELECT count(*)::text FROM (
    SELECT DISTINCT event_type FROM public.beach_advisory
     WHERE event_type NOT IN (
       SELECT event_type FROM public.advisory_score_config WHERE entity_type='beach'
     )
  ) x)
UNION ALL SELECT
  'unseeded_dog_park_events_in_db',
  (SELECT count(*)::text FROM (
    SELECT DISTINCT event_type FROM public.dog_park_advisory
     WHERE event_type NOT IN (
       SELECT event_type FROM public.advisory_score_config WHERE entity_type='dog_park'
     )
  ) x);

COMMIT;
