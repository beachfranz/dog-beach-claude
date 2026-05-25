-- 20260524_harvest_framework_schema.sql
--
-- Harvest framework — peer to codify. Codify produces RULES; harvest
-- produces LOCATIONS. Both are operator-driven; both share substrate
-- (operator registry, HTTP/LLM, dispatch queue, URL resolution); but
-- they produce different output schemas for different downstream
-- consumers.
--
-- See pin: [[harvest-vs-codify-distinction]]
--
-- Two tables, parameterized over content_type so the same framework
-- runs against dog parks, beaches, trails, etc.:
--
--   operator_location_sources
--     Registry: which operators publish which content-type listings
--     at which URLs. Per (operator, content_type, url). Drives the
--     scrape dispatch.
--
--   operator_extracted_locations
--     Structured per-location records from successful scrapes. The
--     attributes JSONB is content-type-specific (dog park: surface,
--     gates, small/large; beach: access, lifeguards, parking; etc.)
--     so a single table holds entities of any type.
--
-- Both tables are idempotent — re-running a scrape ON CONFLICT updates.
-- Sentinel rows (extraction_status='no_locations_found' or similar)
-- prevent re-scrape thrash on stubborn pages.

BEGIN;

-- ── operator_location_sources ────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.operator_location_sources (
  operator_id           INTEGER NOT NULL REFERENCES public.operator(id) ON DELETE CASCADE,
  content_type          TEXT NOT NULL
                          CHECK (content_type IN
                            ('dog_park', 'beach', 'trail', 'park', 'campground', 'dog_friendly_park')),
  source_url            TEXT NOT NULL,
  shape                 TEXT
                          CHECK (shape IS NULL OR shape IN
                            ('hub_subpages',         -- index + per-entity subpages
                             'single_page_inline',   -- all entities on one URL
                             'single_entity_page',   -- one entity per URL
                             'pdf',                  -- PDF directory
                             'js_rendered',          -- requires headless browser
                             'unknown')),
  discovery_method      TEXT
                          CHECK (discovery_method IS NULL OR discovery_method IN
                            ('manual',
                             'codify_policy_source',     -- reused from codify's known URLs
                             'sibling_url_heuristic',    -- derived from operator.web_url
                             'web_search_fallback',      -- found via web search
                             'site_index_crawl')),       -- crawled operator site index
  last_scraped_at       TIMESTAMPTZ,
  extraction_status     TEXT
                          CHECK (extraction_status IS NULL OR extraction_status IN
                            ('success', 'no_locations_found', 'fetch_error',
                             'parse_error', 'js_required', 'rate_limited')),
  locations_extracted_count INTEGER DEFAULT 0,
  qualified_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  notes                 TEXT,
  PRIMARY KEY (operator_id, content_type, source_url)
);

COMMENT ON TABLE public.operator_location_sources IS
  'Harvest registry: per (operator, content_type, url) which listing pages exist + their shape + last-scrape state. Parallel to policy_source for codify, but content-typed.';

CREATE INDEX IF NOT EXISTS operator_location_sources_content_type_idx
  ON public.operator_location_sources (content_type);
CREATE INDEX IF NOT EXISTS operator_location_sources_stale_idx
  ON public.operator_location_sources (last_scraped_at NULLS FIRST);

-- ── operator_extracted_locations ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.operator_extracted_locations (
  id                    BIGSERIAL PRIMARY KEY,
  operator_id           INTEGER NOT NULL REFERENCES public.operator(id) ON DELETE CASCADE,
  content_type          TEXT NOT NULL
                          CHECK (content_type IN
                            ('dog_park', 'beach', 'trail', 'park', 'campground', 'dog_friendly_park')),
  source_url            TEXT NOT NULL,
  -- Identity
  name                  TEXT NOT NULL,
  -- Address (universal across content types)
  address               TEXT,
  address_city          TEXT,
  address_state         CHAR(2),
  -- Geometry
  lat                   NUMERIC,
  lng                   NUMERIC,
  geocode_status        TEXT
                          CHECK (geocode_status IS NULL OR geocode_status IN
                            ('success', 'no_address', 'no_match', 'overpass_fallback', 'manual')),
  -- Content-type-specific attributes (schema varies per type; see content_types/*.py)
  attributes            JSONB,
  -- Subpage URL if the listing linked to a detail page
  subpage_url           TEXT,
  -- Inventory comparison output (populated by compare_to_inventory.py)
  inventory_match_status TEXT
                          CHECK (inventory_match_status IS NULL OR inventory_match_status IN
                            ('matched',         -- found in existing inventory
                             'weak_match',      -- found nearby but name/score weak
                             'gap',             -- not in existing inventory
                             'pending_compare', -- not yet compared
                             'no_geocode')),    -- can't compare without lat/lng
  inventory_match_id    BIGINT,                  -- FK to existing inventory entity (entity-type-specific table)
  inventory_match_score NUMERIC,                 -- match confidence 0-1
  inventory_match_components JSONB,              -- {trigram, distance_m, ...}
  -- Audit
  extraction_status     TEXT NOT NULL DEFAULT 'success'
                          CHECK (extraction_status IN ('success', 'no_locations_found', 'parse_error')),
  extracted_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  compared_at           TIMESTAMPTZ,
  UNIQUE (operator_id, content_type, name, source_url)
);

COMMENT ON TABLE public.operator_extracted_locations IS
  'Harvest output: structured per-entity records from operator-page scrapes. attributes JSONB is content-type-specific. inventory_match_* populated by compare-to-inventory step. Entities with inventory_match_status=gap are candidates for promotion to the canonical inventory.';

CREATE INDEX IF NOT EXISTS operator_extracted_locations_operator_idx
  ON public.operator_extracted_locations (operator_id, content_type);
CREATE INDEX IF NOT EXISTS operator_extracted_locations_content_type_status_idx
  ON public.operator_extracted_locations (content_type, inventory_match_status);
CREATE INDEX IF NOT EXISTS operator_extracted_locations_geocoded_idx
  ON public.operator_extracted_locations (lat, lng) WHERE lat IS NOT NULL;

COMMIT;
