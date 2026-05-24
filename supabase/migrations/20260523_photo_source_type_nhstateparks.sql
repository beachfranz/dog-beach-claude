-- 20260523_photo_source_type_nhstateparks.sql
--
-- Missed during NH loader work (gap #46-era): scripts/loaders/nhsp.py
-- + state_park_urls.json entry + photo_source_type row are the three
-- pieces every new state-park loader needs. The first two shipped
-- 2026-05-23 LATE but the photo_source_type row never landed.
--
-- Symptom (caught by Dagster state_photo_galleries asset materialization
-- 2026-05-23 EVENING): NH loader found 240 photos, FK constraint
-- rejected all 240 INSERTs because beach_photos.source='nhstateparks'
-- had no parent in photo_source_type. action_state_photo_galleries'
-- silent-failure detector caught it: rc=0 from subprocess but stdout
-- showed [INSERT FAIL] × 61 markers + 240 found / 0 inserted.

BEGIN;

INSERT INTO public.photo_source_type
  (id, label, source_kind, default_license, attribution_template,
   loader_script, notes, added_at)
VALUES (
  'nhstateparks',
  'New Hampshire State Parks (NH Division of Parks and Recreation)',
  'agency',
  'fair-use',
  'New Hampshire State Parks',
  'scripts/loaders/nhsp.py',
  'Kentico-CMS pattern. Sitemap-driven park discovery + per-park '
  'image extraction. Added 2026-05-23 LATE alongside the NH state '
  'launch (gap #46 era). Cloudflare-clean (Vision-API-fetchable).',
  now()
)
ON CONFLICT (id) DO UPDATE SET
  label = excluded.label,
  source_kind = excluded.source_kind,
  default_license = excluded.default_license,
  attribution_template = excluded.attribution_template,
  loader_script = excluded.loader_script,
  notes = excluded.notes;

COMMIT;
