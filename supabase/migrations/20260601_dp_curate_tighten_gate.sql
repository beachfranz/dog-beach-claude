-- 20260601_dp_curate_tighten_gate.sql
--
-- The scene-fallback in the curate gate is too generous. It was meant
-- to keep v3-tagged photos eligible while v4 re-tagging caught up, but
-- Haiku assigns 'urban' to roads/trains, 'interior' to gardens, and
-- 'other' to anything ambiguous. Result: Toad Hollow Dog Park (fid 331)
-- curated 6 photos of livestock trailers + freight trains, Riverbend
-- (fid 98) curated 7 wildflower-meadow photos.
--
-- Tightened gate:
--   v4 photos: trust is_dog_park_relevant directly (Haiku answers it well)
--   v3 photos: fall back to has_dog (best signal available without re-tag)
-- This drops the scene fallback entirely.
--
-- Steps:
--   1. UN-curate any vision-auto photo that no longer passes the tighter
--      gate (preserves Curated:Human + manual curations)
--   2. Re-curate using the tighter gate, top-3 per park by sort_order
--
-- Surfaced 2026-06-01 by reviewing admin/dp_showcase.html.
-- See [[dp-curate-scene-gate-validity]] for the prior round of this bug.

BEGIN;

-- ─── Step 1: clear vision-auto picks that fail the new gate ──────────

UPDATE public.dog_park_photos
   SET curated_at    = NULL,
       curated_by    = NULL,
       match_quality = NULL
 WHERE curated_by = 'vision-auto'
   AND NOT (
     -- v4 signal — primary
     (source_meta -> 'vision' ->> 'is_dog_park_relevant')::boolean = true
     -- v3 fallback — only when dp_rel field absent
     OR (
       (source_meta -> 'vision' ->> 'is_dog_park_relevant') IS NULL
       AND (source_meta -> 'vision' ->> 'has_dog')::boolean = true
     )
   );

-- ─── Step 2: re-curate from the cleaner candidate pool ───────────────

WITH ranked AS (
  SELECT p.id, p.dog_park_fid,
         ROW_NUMBER() OVER (
           PARTITION BY p.dog_park_fid
           ORDER BY p.sort_order ASC NULLS LAST, p.id ASC
         ) AS rk
    FROM public.dog_park_photos p
   WHERE p.curated_at IS NULL
     AND p.hidden_at IS NULL
     AND COALESCE(p.source_meta -> 'vision' ->> 'quality_issue', 'none') = 'none'
     AND (
       (p.source_meta -> 'vision' ->> 'is_dog_park_relevant')::boolean = true
       OR (
         (p.source_meta -> 'vision' ->> 'is_dog_park_relevant') IS NULL
         AND (p.source_meta -> 'vision' ->> 'has_dog')::boolean = true
       )
     )
)
UPDATE public.dog_park_photos p
   SET curated_at    = now(),
       curated_by    = 'vision-auto',
       match_quality = 'medium'
  FROM ranked r
 WHERE p.id = r.id AND r.rk <= 3;

COMMIT;
