-- Register 'destateparks' as a photo source for the new DE state-parks loader.
--
-- Added 2026-05-23 LATE during DE virgin-state pipeline test (gap #16).
-- scripts/loaders/dnrec_de.py is the 5th StateParksLoader ABC subclass,
-- using a new WORDPRESS-SLUG pattern category (destateparks.com runs on
-- WordPress with per-park /park/<slug>/ profile pages).
--
-- Without this row, beach_photos.source='destateparks' INSERTs fail with
-- FK violation against public.photo_source_type.
--
-- Reversibility: DELETE FROM public.photo_source_type WHERE id='destateparks';

INSERT INTO public.photo_source_type
  (id, label, source_kind, default_license, attribution_template,
   loader_script, notes)
VALUES
  ('destateparks',
   'Delaware Division of Parks and Recreation (DNREC DPR)',
   'agency',
   'unknown',
   'Delaware Division of Parks and Recreation',
   'scripts/loaders/dnrec_de.py',
   'StateParksLoader ABC — WordPress-slug pattern (destateparks.com).')
ON CONFLICT (id) DO NOTHING;
