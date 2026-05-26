-- Register 'websearch' as a photo source type.
-- Used by load_websearch_photos.py for entity-aware web image discovery
-- (currently dog_park only — beaches have richer geo-tagged sources).

INSERT INTO public.photo_source_type
  (id,         label,                        source_kind, default_license,
   attribution_template, loader_script, notes)
VALUES
  ('websearch', 'Web image search (Tavily)', 'commercial', 'third-party',
   'via web image search',
   'scripts/load_websearch_photos.py',
   'Tavily image-search results. Links to third-party hosts (Yelp, Reddit, '
   || 'bringfido, news sites, etc.). Fair-use small-thumbnail display with '
   || 'link-back attribution. Not CC; do not redistribute outside the app.')
ON CONFLICT (id) DO NOTHING;
