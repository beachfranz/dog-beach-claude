-- 20260522_beach_photos_source_add_md_dnr.sql
--
-- Extend beach_photos.source CHECK constraint to allow 'md_dnr' for the
-- new MD DNR loader (scripts/loaders/md_dnr.py, on the StateParksLoader
-- ABC).
--
-- Future state-parks loaders (NJ, MA, MI, etc.) will each add a new
-- source value the same way. Consider migrating to a lookup table if
-- this grows beyond ~20 sources.

BEGIN;

ALTER TABLE public.beach_photos
  DROP CONSTRAINT IF EXISTS beach_photos_source_check;

ALTER TABLE public.beach_photos
  ADD CONSTRAINT beach_photos_source_check
  CHECK (source = ANY (ARRAY[
    'mapillary'::text,
    'wikimedia'::text,
    'ccc'::text,
    'nps'::text,
    'cdpr'::text,
    'wsprc'::text,
    'oprd'::text,
    'md_dnr'::text,
    'unsplash'::text,
    'pexels'::text,
    'pixabay'::text,
    'flickr'::text,
    'manual'::text
  ]));

COMMIT;
