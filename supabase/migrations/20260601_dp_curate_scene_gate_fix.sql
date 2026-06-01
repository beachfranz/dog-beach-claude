-- 20260601_dp_curate_scene_gate_fix.sql
--
-- The Dagster `dp_photos_curate` asset gates on:
--   has_dog=true OR scene IN ('outdoor', 'park', 'beach')
-- The prompt's SCENES enum doesn't contain 'outdoor', 'park', or 'beach',
-- so the scene-leg of the OR matched 0 rows. The effective gate was just
-- has_dog=true.
--
-- Surfaced 2026-06-01 during DP curate criteria review.
-- See [[dp-curate-scene-gate-bug-pre-fix]] (pending pin).
--
-- This migration applies the FIXED gate to the existing dog_park_photos
-- catalog (cross-state) so the broader-than-just-has_dog cohort can be
-- curated without waiting for a Dagster re-materialization.
--
-- Fixed gate:
--   has_dog=true OR scene IN ('beach_with_sand','urban','interior','other')
-- Excluded scenes (intentionally NOT DP content):
--   water_only, wildlife_only, coast_no_sand, food, screenshot_or_map

BEGIN;

WITH ranked AS (
  SELECT p.id, p.dog_park_fid,
         ROW_NUMBER() OVER (
           PARTITION BY p.dog_park_fid
           ORDER BY p.sort_order ASC NULLS LAST, p.id ASC
         ) AS rk
    FROM public.dog_park_photos p
   WHERE p.curated_at IS NULL
     AND p.hidden_at IS NULL
     AND (
       (p.source_meta -> 'vision' ->> 'has_dog')::boolean = true
       OR (p.source_meta -> 'vision' ->> 'scene') IN
          ('beach_with_sand', 'urban', 'interior', 'other')
     )
     AND COALESCE(p.source_meta -> 'vision' ->> 'quality_issue', 'none') = 'none'
)
UPDATE public.dog_park_photos p
   SET curated_at    = now(),
       curated_by    = 'vision-auto',
       match_quality = 'medium'
  FROM ranked r
 WHERE p.id = r.id AND r.rk <= 3;

COMMIT;
