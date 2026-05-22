-- 20260522_canonical_operating_hours_schema.sql
--
-- CANONICAL OPERATING HOURS PER BEACH (task #118)
--
-- Schema-only migration. Adds 4 columns to beaches_gold to carry the
-- canonical "when is this beach open" model + provenance + freshness.
--
-- Per Franz 2026-05-22:
--   - Beach-level only (per-section access lives in zone_rules).
--   - "Inferred" source = sunrise-sunset fallback flagged as unverified;
--     UI hedges copy ("typically dawn-to-dusk") for these rows.
--
-- Subsequent migrations:
--   - populate_operating_hours(p_fid) function — chains operator >
--     osm > agency > inferred fallback. Skips manual_curator.
--   - operating_hours_refresh pipeline phase — runs the populator
--     per active beach in state. Between operator_merge and
--     hourly_status_refresh.
--
-- JSONB shape:
--   {
--     "default": "sunrise-sunset" | "06:00-22:00" | "24/7",
--     "seasonal": [
--       {"from": "MM-DD", "to": "MM-DD", "hours": "<spec>"}
--     ],
--     "notes": "<optional human-readable context>"
--   }
--
-- The `default` + `hours` strings follow OSM opening_hours conventions
-- where practical. Parsing is consumer-side (scoreHours() in
-- _shared/scoring.ts). Sunrise-sunset is computed from beach lat/lon +
-- date.

BEGIN;

ALTER TABLE public.beaches_gold
  ADD COLUMN IF NOT EXISTS operating_hours              jsonb,
  ADD COLUMN IF NOT EXISTS operating_hours_source       text,
  ADD COLUMN IF NOT EXISTS operating_hours_confidence   numeric,
  ADD COLUMN IF NOT EXISTS operating_hours_last_verified timestamptz;

ALTER TABLE public.beaches_gold
  DROP CONSTRAINT IF EXISTS beaches_gold_operating_hours_source_check;

ALTER TABLE public.beaches_gold
  ADD CONSTRAINT beaches_gold_operating_hours_source_check
  CHECK (operating_hours_source IS NULL
         OR operating_hours_source IN
            ('operator','osm','agency','inferred','manual_curator'));

-- Index on source so the consumer surface can filter "unverified"
-- (= inferred) beaches efficiently when rendering hedged copy.
CREATE INDEX IF NOT EXISTS beaches_gold_operating_hours_source_idx
  ON public.beaches_gold (operating_hours_source);

COMMENT ON COLUMN public.beaches_gold.operating_hours IS
  'Canonical operating hours, beach-level. JSONB: {default: "<spec>", seasonal: [{from: "MM-DD", to: "MM-DD", hours: "<spec>"}], notes: "..."}. Spec strings accept OSM opening_hours conventions (sunrise-sunset, HH:MM-HH:MM, 24/7). Per-section access (e.g., boardwalk vs sand summer carve-out) lives in zone_rules, not here.';

COMMENT ON COLUMN public.beaches_gold.operating_hours_source IS
  'Provenance: manual_curator (highest, never overwritten) > operator (operator_dogs_policy.time_windows) > osm (osm_features.opening_hours) > agency (scraped from policy pages) > inferred (sunrise-sunset fallback, ~80% of beaches). UI hedges copy when source=inferred ("typically dawn-to-dusk").';

COMMENT ON COLUMN public.beaches_gold.operating_hours_confidence IS
  'Default ranges: manual=1.00, operator=0.85, agency=0.80, osm=0.70, inferred=0.30.';

COMMENT ON COLUMN public.beaches_gold.operating_hours_last_verified IS
  'Set on every populate_operating_hours() run (idempotent). Drives staleness checks for steady-state revalidation.';

COMMIT;
