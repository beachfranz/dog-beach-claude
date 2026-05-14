-- 20260513_beach_photos_hidden.sql
--
-- Adds a "hidden" status to beach_photos — distinct from trash (which
-- writes a tombstone + becomes a model training signal).
--
-- Semantics:
--   Trash   = "bad photo, train against it"           → DELETE + tombstone
--   Hide    = "fine photo, just not chosen for this beach"
--                                                     → set hidden_at, no tombstone, no model signal
--
-- Hidden photos are excluded from the consumer-facing photo selection
-- (get_beach_photos_diverse) but are NOT deleted and are NOT used as
-- negative examples in model training (train_photo_model.py only reads
-- tombstones for negative labels).

begin;

alter table public.beach_photos
  add column if not exists hidden_at timestamptz,
  add column if not exists hidden_by text;

-- Partial index for the common query "active (non-hidden) photos for a beach"
create index if not exists beach_photos_active_idx
  on public.beach_photos (arena_group_id)
  where hidden_at is null;

comment on column public.beach_photos.hidden_at
  is 'Set when curator hides the photo from this beach without trashing it. '
     'Excluded from consumer-facing selection. Distinguished from trash '
     '(which writes a beach_photo_rejected tombstone). NULL = visible.';
comment on column public.beach_photos.hidden_by
  is 'Curator username who hid the photo.';

commit;
