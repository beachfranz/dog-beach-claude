-- 20260513_beach_photos_dimensions.sql
--
-- Add explicit width/height columns to beach_photos so we can:
--   1. Quantify landscape vs portrait distribution (Franz 2026-05-13)
--   2. Use aspect-ratio info in selection (e.g. prefer landscape for hero)
--   3. Pre-allocate frontend layout space (reduce CLS)
--
-- Per-source backfill scripts populate these columns (see
-- scripts/one_off/backfill_photo_dimensions_*.py). Future loaders should
-- capture width/height at ingest so we don't re-backfill.

begin;

alter table public.beach_photos
  add column if not exists width  integer,
  add column if not exists height integer;

-- Aspect-ratio expression index — filters to portrait photos efficiently.
-- (Equivalent: height > width.)
create index if not exists beach_photos_portrait_idx
  on public.beach_photos ((height > width))
  where width is not null and height is not null;

comment on column public.beach_photos.width
  is 'Image width in pixels. Populated by backfill scripts per-source. '
     'Null = dimensions unknown.';
comment on column public.beach_photos.height
  is 'Image height in pixels. Same provenance as width.';

commit;
