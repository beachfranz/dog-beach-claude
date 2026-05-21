-- 20260521_beach_photos_add_oprd_source.sql
--
-- Add 'oprd' to the beach_photos.source check constraint, enabling the
-- OPRD (Oregon Parks and Recreation Department) photo loader.
-- Per the per-state photo source discovery (project_per_state_photo_source_discovery_2026_05_19.md):
-- OPRD covers ~44 OR scoreable beaches via STAT/SPR PAD-US units
-- (limited today by sparse OR PAD-US coverage; will lift when the
-- OR ocean-shore proxy polygon is wired into beach_polygon_membership).
-- Loader: scripts/load_oprd_photos.py.

begin;

alter table public.beach_photos drop constraint if exists beach_photos_source_check;

alter table public.beach_photos add constraint beach_photos_source_check
  check (source = any (array[
    'mapillary'::text,
    'wikimedia'::text,
    'ccc'::text,
    'nps'::text,
    'cdpr'::text,
    'wsprc'::text,
    'oprd'::text,
    'unsplash'::text,
    'pexels'::text,
    'pixabay'::text,
    'flickr'::text,
    'manual'::text
  ]));

commit;
