-- Add 'cdpr' to beach_photos.source check constraint.
-- Source = California Department of Parks and Recreation gallery scrape
-- (parks.ca.gov park-page galleries). Sibling to 'nps' which is already
-- in the allowed set for the planned NPS Multimedia API loader.

alter table public.beach_photos
  drop constraint if exists beach_photos_source_check;

alter table public.beach_photos
  add constraint beach_photos_source_check
  check (source = any (array[
    'mapillary', 'wikimedia', 'ccc', 'nps', 'cdpr',
    'unsplash', 'pexels', 'pixabay', 'flickr', 'manual'
  ]));
