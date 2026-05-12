-- Tombstone vision parity: add vision_tags JSONB column to
-- beach_photo_rejected so vision features can be present on BOTH the
-- kept (beach_photos.source_meta.vision) AND rejected (here) sides of
-- the photo-quality training data.
--
-- Without this, vision features can only be applied via the post-hoc
-- rule layer at inference time — the model can't learn from them
-- during training because the missingness on the rejected side would
-- perfectly separate classes (Polyzotis 2018 missingness leak).
--
-- See project_photo_vision_tagging.md for the full design rationale.

alter table public.beach_photo_rejected
  add column if not exists vision_tags jsonb;

comment on column public.beach_photo_rejected.vision_tags is
  'Vision tags from load_photo_vision_tags.py — same schema as '
  'beach_photos.source_meta.vision. Captured at trash time for new '
  'rejections; backfilled retroactively for legacy tombstones via '
  'rebuild_tombstone_image_url + vision tagging.';
