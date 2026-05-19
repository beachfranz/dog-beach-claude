-- Phase (a) Part 1 — dog_parks_gold curated entity table + ETL from OSM.
--
-- Per Franz 2026-05-19 + docs/operate_pipeline_spec.md v3 + [[dog-park-phase3-v1-ready]].
--
-- Dog parks become first-class destinations parallel to beaches (find.html
-- toggle, detail page, scoring, fallback-from-beach). This migration creates
-- the curated `dog_parks_gold` table mirroring the beaches_gold pattern,
-- and seeds it from `osm_dog_parks` via a quality gate.
--
-- Quality gate (per Franz 2026-05-19):
--   - name present (drops 3,594 unnamed OSM features)
--   - not abandoned (excludes `disused:leisure=dog_park` — 3 rows)
--   - access != private (drops 499 HOA/restricted)
--   - geom present (always true for osm_dog_parks)
--
-- Expected post-gate counts:
--   Full US: ~2,463 rows
--   MVP+ (CA+OR+WA via PIP): ~582 rows (CA 351, WA 140, OR 91)
--
-- State derivation: spatial PIP against `public.states` (56 polygons).
-- Pass A inferred_operator_id + attribution_method are POPULATED IN A
-- LATER STEP via `scripts/dog_park_operator_attribution.py` (kept separate
-- so we don't bake LLM/PIP logic into the migration).

BEGIN;

-- ─── 1. Table ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.dog_parks_gold (
  -- Identity
  fid                       bigserial PRIMARY KEY,
  name                      text NOT NULL,                  -- quality-gated
  source_code               text NOT NULL,                  -- 'osm_v1' to start
  source_id                 text NOT NULL,                  -- 'osm/<osm_id>'
  osm_id                    bigint,                         -- raw OSM id (joinable to osm_dog_parks)
  osm_type                  text,                           -- node|way|relation

  -- Location
  geom                      geometry(Geometry, 4326),
  lat                       double precision,               -- centroid
  lon                       double precision,
  area_m2                   double precision,

  -- Administrative
  state                     text,                           -- 2-letter; derived via PIP at ETL
  county_fips               text,
  county_name               text,
  address_street            text,
  address_city              text,
  address_state             text,
  address_zip               text,
  address_county            text,

  -- Derived signal columns (from OSM tags at ETL)
  has_fence                 boolean,                        -- tags.fence=yes OR tags.barrier=fence
  has_drinking_water        boolean,                        -- tags.drinking_water present
  surface                   text,                           -- grass|dirt|concrete|...
  opening_hours_osm         text,                           -- raw OSM opening_hours string
  website                   text,                           -- tags.website or tags.url

  -- Inferred operator (populated by Pass A in scripts/dog_park_operator_attribution.py)
  inferred_operator_id      bigint REFERENCES public.operator(id) ON DELETE SET NULL,
  attribution_method        text,                           -- direct_tag|city_high|city_medium|county_*|spatial_*|non_municipal_defer
  attribution_confidence    text,                           -- high|medium|low

  -- Provenance
  tags                      jsonb,                          -- full OSM tag bag for future signals
  promoted_from             text NOT NULL DEFAULT 'osm_dog_parks_v1',
  promoted_at               timestamptz NOT NULL DEFAULT now(),

  -- Lifecycle
  is_active                 boolean NOT NULL DEFAULT true,
  inactive_reason           text,

  -- Curator overrides (sparse; future use)
  display_name_override     text,
  description               text,
  review_status             text,
  review_notes              text,

  -- Uniqueness
  CONSTRAINT dog_parks_gold_source_uidx UNIQUE (source_code, source_id)
);

COMMENT ON TABLE public.dog_parks_gold IS
  'Curated dog-park entities — parallel first-class destination type alongside beaches_gold. '
  'Promoted from osm_dog_parks via quality gate (named + not abandoned + not private + has geom). '
  'inferred_operator_id is populated by Pass A attribution script (scripts/dog_park_operator_attribution.py). '
  'Created 2026-05-19 per Phase 3 dog-park first-class scope.';

COMMENT ON COLUMN public.dog_parks_gold.source_id IS
  'Stable opaque id for the source. For OSM rows: ''osm/'' || osm_id (e.g., ''osm/123456789'').';

COMMENT ON COLUMN public.dog_parks_gold.inferred_operator_id IS
  'Pass A inferred operator (singular public.operator). NULL until Pass A runs. '
  'See attribution_method for derivation path. Only verified-operator rows (Phase 3 v1+) '
  'should be treated as authoritative for hours/rules.';

-- ─── 2. Indexes ──────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS dog_parks_gold_state_idx           ON public.dog_parks_gold (state);
CREATE INDEX IF NOT EXISTS dog_parks_gold_county_idx          ON public.dog_parks_gold (county_fips);
CREATE INDEX IF NOT EXISTS dog_parks_gold_geom_gist_idx       ON public.dog_parks_gold USING GIST (geom);
CREATE INDEX IF NOT EXISTS dog_parks_gold_name_trgm_idx       ON public.dog_parks_gold USING GIN (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS dog_parks_gold_active_idx          ON public.dog_parks_gold (is_active) WHERE is_active;
CREATE INDEX IF NOT EXISTS dog_parks_gold_inferred_op_idx     ON public.dog_parks_gold (inferred_operator_id) WHERE inferred_operator_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS dog_parks_gold_osm_id_idx          ON public.dog_parks_gold (osm_id) WHERE osm_id IS NOT NULL;

-- ─── 3. ETL from osm_dog_parks ───────────────────────────────────────
-- One-shot seed. Idempotent via the source UNIQUE constraint.

INSERT INTO public.dog_parks_gold (
  name, source_code, source_id, osm_id, osm_type,
  geom, lat, lon, area_m2,
  state,
  address_street, address_city, address_state, address_zip,
  has_fence, has_drinking_water, surface, opening_hours_osm, website,
  tags,
  promoted_from
)
SELECT
  d.name,
  'osm_v1'                                    AS source_code,
  'osm/' || d.osm_id::text                    AS source_id,
  d.osm_id,
  d.osm_type,
  d.geom,
  ST_Y(ST_Centroid(d.geom))                   AS lat,
  ST_X(ST_Centroid(d.geom))                   AS lon,
  d.area_m2,
  -- state via PIP; LATERAL pick of the containing state polygon
  (SELECT s.state_code
     FROM public.states s
     WHERE ST_Contains(s.geom, d.geom)
     LIMIT 1)                                 AS state,
  d.tags->>'addr:street'                      AS address_street,
  d.tags->>'addr:city'                        AS address_city,
  d.tags->>'addr:state'                       AS address_state,
  d.tags->>'addr:postcode'                    AS address_zip,
  (d.tags->>'fence' = 'yes' OR d.tags->>'barrier' = 'fence') AS has_fence,
  (d.tags ? 'drinking_water')                 AS has_drinking_water,
  d.tags->>'surface'                          AS surface,
  d.tags->>'opening_hours'                    AS opening_hours_osm,
  COALESCE(d.tags->>'website', d.tags->>'url') AS website,
  d.tags,
  'osm_dog_parks_v1'                          AS promoted_from
FROM public.osm_dog_parks d
WHERE d.name IS NOT NULL AND d.name <> ''
  AND NOT (d.tags ? 'disused:leisure')
  AND COALESCE(d.tags->>'access','') <> 'private'
  AND d.geom IS NOT NULL
ON CONFLICT (source_code, source_id) DO NOTHING;

-- ─── 4. Verification ─────────────────────────────────────────────────

SELECT 'total_dog_parks_gold' AS metric, count(*)::text AS val FROM public.dog_parks_gold
UNION ALL
SELECT 'mvp_plus_states', count(*)::text FROM public.dog_parks_gold WHERE state IN ('CA','OR','WA')
UNION ALL
SELECT 'state_breakdown_mvp_plus',
       string_agg(state || ':' || n::text, ', ' ORDER BY state)
  FROM (SELECT state, count(*) AS n FROM public.dog_parks_gold WHERE state IN ('CA','OR','WA') GROUP BY state) t
UNION ALL
SELECT 'null_state',  count(*)::text FROM public.dog_parks_gold WHERE state IS NULL
UNION ALL
SELECT 'has_fence',   count(*)::text FROM public.dog_parks_gold WHERE has_fence
UNION ALL
SELECT 'has_opening_hours_osm', count(*)::text FROM public.dog_parks_gold WHERE opening_hours_osm IS NOT NULL
UNION ALL
SELECT 'has_website', count(*)::text FROM public.dog_parks_gold WHERE website IS NOT NULL;

COMMIT;
