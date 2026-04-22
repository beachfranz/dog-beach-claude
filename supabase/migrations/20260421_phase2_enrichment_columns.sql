-- Phase 2 operational-data columns for beaches_staging_new.
--
-- These columns hold the user-facing operational data we will enrich from
-- multiple sources: CCC access points (free amenities), per-jurisdiction
-- scraping (city/state/county parks pages), and AI-assisted research as a
-- fallback.
--
-- Each enrichment source records itself in dogs_policy_source /
-- enrichment_source with a URL where possible, so monthly refresh can
-- re-validate and stale entries are traceable.

alter table public.beaches_staging_new
  -- Dog policy (priority 1)
  add column if not exists dogs_allowed              text
    check (dogs_allowed is null or dogs_allowed in ('yes', 'no', 'seasonal', 'unknown')),
  add column if not exists dogs_leash_required       boolean,
  add column if not exists dogs_off_leash_area       text,
  add column if not exists dogs_time_restrictions    text,
  add column if not exists dogs_season_restrictions  text,
  add column if not exists dogs_policy_source        text,
  add column if not exists dogs_policy_source_url    text,
  add column if not exists dogs_policy_notes         text,
  add column if not exists dogs_policy_updated_at    timestamptz,

  -- Parking
  add column if not exists has_parking               boolean,
  add column if not exists parking_type              text,
  add column if not exists parking_notes             text,

  -- Hours
  add column if not exists hours_text                text,
  add column if not exists hours_notes               text,

  -- Amenities
  add column if not exists has_restrooms             boolean,
  add column if not exists has_showers               boolean,
  add column if not exists has_lifeguards            boolean,
  add column if not exists has_picnic_area           boolean,
  add column if not exists has_food                  boolean,
  add column if not exists has_drinking_water        boolean,
  add column if not exists has_fire_pits             boolean,
  add column if not exists has_disabled_access       boolean,

  -- Enrichment metadata
  add column if not exists enrichment_source         text,
  add column if not exists enrichment_updated_at     timestamptz,
  add column if not exists enrichment_confidence     text
    check (enrichment_confidence is null or enrichment_confidence in ('high', 'low'));

-- Index on enrichment_updated_at for stale-row queries during monthly refresh
create index if not exists idx_beaches_staging_new_enrichment_updated_at
  on public.beaches_staging_new (enrichment_updated_at);
