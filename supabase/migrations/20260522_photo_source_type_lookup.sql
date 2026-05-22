-- 20260522_photo_source_type_lookup.sql
--
-- CONSOLIDATION MOVE #1 from the 2026-05-22 clean-pipeline audit.
--
-- Replaces the beach_photos.source CHECK constraint (a hardcoded enum
-- requiring an ALTER per new loader) with a real lookup table that
-- carries metadata: attribution template, default license, loader
-- script path, source kind (agency/community/commercial/manual).
--
-- Before: every new state-parks loader required:
--   ALTER TABLE beach_photos DROP CONSTRAINT beach_photos_source_check;
--   ALTER TABLE beach_photos ADD CONSTRAINT beach_photos_source_check
--     CHECK (source IN ('mapillary',...,'md_dnr',...));
-- We just did this 4 hours ago for 'md_dnr'.
--
-- After: INSERT a single row into photo_source_type.
--
-- The source column on beach_photos stays as text (no rename) — the
-- enforcement moves from CHECK to FOREIGN KEY. All existing rows
-- continue to work.

BEGIN;

-- ─── 1. Create lookup table ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.photo_source_type (
  id                   text PRIMARY KEY,
  label                text NOT NULL,
  source_kind          text NOT NULL CHECK (source_kind IN
                            ('agency','community','commercial','osm','manual')),
  default_license      text,
  attribution_template text,    -- canonical attribution string (when known)
  loader_script        text,    -- path to the canonical loader script
  notes                text,
  added_at             timestamptz NOT NULL DEFAULT now()
);

GRANT SELECT ON public.photo_source_type
  TO anon, authenticated, service_role;

COMMENT ON TABLE public.photo_source_type IS
  'Registry of photo-source types. Replaces the beach_photos.source CHECK constraint with a real table carrying per-source metadata. Adding a new source = INSERT one row; no ALTER required.';

-- ─── 2. Seed all known source values (extant + reserved) ────────────

INSERT INTO public.photo_source_type
  (id, label, source_kind, default_license, attribution_template, loader_script, notes)
VALUES
  ('mapillary', 'Mapillary', 'community', 'CC-BY-SA',
   'Mapillary contributor',
   'scripts/load_mapillary_photos.py',
   'Street-level imagery. TESTED + REJECTED for quality (see mapillary-photo-quality-rejected pin). Loader exists but NOT in canonical pipeline.'),

  ('wikimedia', 'Wikimedia Commons', 'community', 'CC BY-SA 4.0',
   '<author> / CC BY-SA via Wikimedia Commons',
   'scripts/load_wikimedia_photos.py',
   'Community-curated. ~3x higher quality than Mapillary per 2026-05-09 swap. Canonical pipeline.'),

  ('ccc', 'California Coastal Commission', 'agency', 'state-govt',
   'California Coastal Commission',
   NULL,
   'CCC access-point photos; legacy loader.'),

  ('nps', 'National Park Service', 'agency', 'public_domain_us_gov',
   'National Park Service',
   'scripts/load_nps_photos.py',
   'US federal; public-domain.'),

  ('cdpr', 'California State Parks', 'agency', 'cdpr_state_parks',
   'California State Parks',
   'scripts/loaders/cdpr.py',
   'StateParksLoader ABC — DB-driven pattern.'),

  ('wsprc', 'Washington State Parks and Recreation Commission', 'agency', 'unknown',
   'Washington State Parks and Recreation Commission',
   'scripts/loaders/wsprc.py',
   'StateParksLoader ABC — Drupal-sitemap pattern.'),

  ('oprd', 'Oregon Parks and Recreation Department', 'agency', 'unknown',
   'Oregon Parks and Recreation Department',
   'scripts/loaders/oprd.py',
   'StateParksLoader ABC — ColdFusion CMS-embed pattern.'),

  ('md_dnr', 'Maryland Department of Natural Resources', 'agency', 'unknown',
   'Maryland Department of Natural Resources',
   'scripts/loaders/md_dnr.py',
   'StateParksLoader ABC — SharePoint-static pattern.'),

  ('flickr', 'Flickr', 'community', 'CC-BY-NC-SA-2.0',
   '© <author> / CC-BY-SA via Flickr',
   'scripts/load_flickr_photos.py',
   'Community-curated; per-photo license varies.'),

  ('unsplash', 'Unsplash', 'commercial', 'Unsplash-License',
   'Photo by <author> on Unsplash',
   NULL,
   'Commercial CC0-like; rarely used.'),

  ('pexels', 'Pexels', 'commercial', 'Pexels-License',
   'Photo by <author> on Pexels',
   NULL,
   'Commercial CC0-like; reserved, unused.'),

  ('pixabay', 'Pixabay', 'commercial', 'Pixabay-License',
   'Photo by <author> on Pixabay',
   NULL,
   'Commercial CC0-like; reserved, unused.'),

  ('manual', 'Manual / Curator', 'manual', NULL,
   '<curator-defined>',
   NULL,
   'Hand-uploaded; for one-off corrections.')
ON CONFLICT (id) DO NOTHING;

-- ─── 3. Verify all extant source values are seeded ──────────────────
--
-- If any beach_photos.source value is missing from the lookup, the FK
-- below would fail. Guard with an explicit check that returns the
-- offending values for triage.

DO $$
DECLARE
  missing text;
BEGIN
  SELECT string_agg(DISTINCT bp.source, ', ')
    INTO missing
    FROM public.beach_photos bp
    LEFT JOIN public.photo_source_type ps ON ps.id = bp.source
   WHERE ps.id IS NULL;
  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'photo_source_type missing rows for extant sources: %', missing;
  END IF;
END $$;

-- ─── 4. Replace CHECK with FOREIGN KEY ──────────────────────────────

ALTER TABLE public.beach_photos
  DROP CONSTRAINT IF EXISTS beach_photos_source_check;

ALTER TABLE public.beach_photos
  ADD CONSTRAINT beach_photos_source_fkey
  FOREIGN KEY (source) REFERENCES public.photo_source_type(id);

COMMIT;

-- ─── Adding a new source going forward ──────────────────────────────
--
--   INSERT INTO public.photo_source_type
--     (id, label, source_kind, default_license, attribution_template,
--      loader_script, notes)
--   VALUES
--     ('nj_state_parks', 'New Jersey State Parks', 'agency',
--      'unknown', 'New Jersey State Parks', 'scripts/loaders/nj.py',
--      'StateParksLoader ABC');
--
-- No ALTER, no CHECK rewrite. Loader can start using the source value
-- immediately because the FK is satisfied by the INSERT above.
