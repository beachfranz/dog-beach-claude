-- 20260601_dp_curate_dog_only.sql
--
-- Policy change (Franz 2026-06-01): going forward, DP photos are
-- DOG-ONLY. A photo without a visible dog does not get curated, even
-- if Haiku says is_dog_park_relevant=true. The dp_rel signal captured
-- agility equipment, fenced enclosures, paw signage, etc. — all
-- valid DP content but not what users want to see.
--
-- Tightened gate:
--   has_dog=true AND quality_issue='none'
-- That's it. No dp_rel fallback, no scene fallback.
--
-- See [[dp-photos-are-dog-only]].
--
-- Steps:
--   1. UN-curate vision-auto photos without has_dog=true
--   2. Re-curate from the dog-only candidate pool (top-3 per park)

BEGIN;

UPDATE public.dog_park_photos
   SET curated_at    = NULL,
       curated_by    = NULL,
       match_quality = NULL
 WHERE curated_by = 'vision-auto'
   AND COALESCE((source_meta -> 'vision' ->> 'has_dog')::boolean, false) = false;

WITH ranked AS (
  SELECT p.id, p.dog_park_fid,
         ROW_NUMBER() OVER (
           PARTITION BY p.dog_park_fid
           ORDER BY p.sort_order ASC NULLS LAST, p.id ASC
         ) AS rk
    FROM public.dog_park_photos p
   WHERE p.curated_at IS NULL
     AND p.hidden_at IS NULL
     AND (p.source_meta -> 'vision' ->> 'has_dog')::boolean = true
     AND COALESCE(p.source_meta -> 'vision' ->> 'quality_issue', 'none') = 'none'
)
UPDATE public.dog_park_photos p
   SET curated_at    = now(),
       curated_by    = 'vision-auto',
       match_quality = 'medium'
  FROM ranked r
 WHERE p.id = r.id AND r.rk <= 3;

COMMIT;
