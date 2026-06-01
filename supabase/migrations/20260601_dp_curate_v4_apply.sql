-- 20260601_dp_curate_v4_apply.sql
--
-- Apply the new v4 curate gate to dog_park_photos after the v4 re-tag
-- has populated is_dog_park_relevant on existing rows.
--
-- Primary signal: is_dog_park_relevant=true.
-- Fallbacks (for any row that hasn't been re-tagged yet): has_dog OR
-- the broad scene set we shipped in 20260601_dp_curate_scene_gate_fix.sql.
--
-- Only operates on photos that are CURRENTLY UNCURATED. The 600 already-
-- curated DPs stay as-is — their selections were made under the v3
-- (has_dog-only-effective) gate; re-running on them would just churn
-- sort_order without strong signal that the new picks are better.

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
       (p.source_meta -> 'vision' ->> 'is_dog_park_relevant')::boolean = true
       OR (p.source_meta -> 'vision' ->> 'has_dog')::boolean = true
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
