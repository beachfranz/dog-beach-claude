"""Harvest pipeline — peer to codify, parameterized over content_type.

Codify produces RULES; harvest produces LOCATIONS. Both share substrate
(operator registry, HTTP fetcher, LLM extractor, dispatch queue) but
produce different output schemas for different downstream consumers.

See pin: harvest-vs-codify-distinction.

Pipeline stages (each is its own script):
  discover_listing_urls    operator + content_type -> listing URL(s)
                            via sibling-URL heuristic / web-search fallback
                            outputs: operator_location_sources rows
  extract_locations         listing URLs -> structured entity records
                            via LLM (content_type-specific prompt+schema)
                            outputs: operator_extracted_locations rows
  geocode                   addresses -> lat/lng via Nominatim + Overpass
                            updates operator_extracted_locations.lat/lng
  compare_to_inventory      extracted entities -> match against existing inventory
                            (osm_dog_parks for dog parks; arena for beaches; etc.)
                            updates inventory_match_* fields
  promote_gaps              gap entities -> push to canonical inventory
                            (dog_parks_gold / arena / etc.)

Content types live in content_types/ — each module declares:
  CONTENT_TYPE         the literal string (must match check constraint)
  EXTRACTION_PROMPT    the LLM system prompt for entity extraction
  ATTRIBUTES_SCHEMA    JSON Schema for the attributes JSONB
  SIBLING_URL_HINTS    list of likely sub-paths on operator sites
  INVENTORY_TABLE      where to compare against (e.g. 'osm_dog_parks')
  inventory_match_query(extracted_row, cursor) -> dict with match info
"""
