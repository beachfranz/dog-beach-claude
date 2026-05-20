-- 20260519_beach_photos_add_wsprc_source.sql
--
-- Add 'wsprc' to the beach_photos.source check constraint, enabling the
-- WSPRC (Washington State Parks and Recreation Commission) photo loader.
-- Per the per-state photo source discovery (project_per_state_photo_source_discovery_2026_05_19.md):
-- WSPRC covers 104 WA scoreable beaches via STAT/SPR PAD-US units.
-- Loader: scripts/load_wsprc_photos.py.

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
    'unsplash'::text,
    'pexels'::text,
    'pixabay'::text,
    'flickr'::text,
    'manual'::text
  ]));

commit;
