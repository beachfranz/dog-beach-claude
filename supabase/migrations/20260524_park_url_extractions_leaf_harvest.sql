-- 20260524_park_url_extractions_leaf_harvest.sql
--
-- Extend park_url_extractions.extraction_type CHECK to include
-- 'leaf_page_harvest', the new value written by
-- scripts/harvest_park_text.py for OR/WA state-park leaf URLs.
-- Fills the description text reservoir gap (Gap 2 per
-- project_deferred_description_text_reservoir.md) for states where
-- pad_us_units don't carry park_url.

BEGIN;

ALTER TABLE public.park_url_extractions
  DROP CONSTRAINT IF EXISTS park_url_extractions_extraction_type_check;

ALTER TABLE public.park_url_extractions
  ADD CONSTRAINT park_url_extractions_extraction_type_check
  CHECK (extraction_type IS NULL OR extraction_type = ANY (ARRAY[
    'cpad_source',
    'cpad_source_crawl',
    'derived_url_crawl',
    'leaf_page_harvest'
  ]));

COMMIT;
