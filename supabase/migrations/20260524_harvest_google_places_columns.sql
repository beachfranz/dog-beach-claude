-- 20260524_harvest_google_places_columns.sql
--
-- Add Google Places fields to operator_extracted_locations.
-- Google Places (searchText) becomes the primary resolver; Nominatim
-- + Overpass remain as free fallbacks.
--
-- google_place_id     stable Google identifier — useful for dedup +
--                     future API calls (Place Details, photos, reviews).
-- google_places_data  full Places API response as JSONB. Captures
--                     formattedAddress, types, websiteUri,
--                     regularOpeningHours, businessStatus, rating,
--                     userRatingCount, googleMapsUri, etc.
--
-- Extend geocode_status CHECK to allow 'google_places'.

BEGIN;

ALTER TABLE public.operator_extracted_locations
  ADD COLUMN IF NOT EXISTS google_place_id TEXT,
  ADD COLUMN IF NOT EXISTS google_places_data JSONB;

CREATE INDEX IF NOT EXISTS operator_extracted_locations_place_id_idx
  ON public.operator_extracted_locations (google_place_id)
  WHERE google_place_id IS NOT NULL;

-- Extend geocode_status CHECK
ALTER TABLE public.operator_extracted_locations
  DROP CONSTRAINT IF EXISTS operator_extracted_locations_geocode_status_check;

ALTER TABLE public.operator_extracted_locations
  ADD CONSTRAINT operator_extracted_locations_geocode_status_check
  CHECK (geocode_status IS NULL OR geocode_status IN
    ('google_places', 'success', 'no_address', 'no_match',
     'overpass_fallback', 'manual'));

COMMIT;
