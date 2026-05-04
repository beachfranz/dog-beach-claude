-- 20260503_harmony_phase8_2_beach_amenities.sql
--
-- Slice 2.1 of "harmony phase 8 — curator on canonical tables"
-- (project_curator_on_canonical_tables.md).
--
-- Adds public.beach_amenities — the canonical home for the amenity
-- booleans + parking + hours that today live on locations_stage. PK +
-- FK on arena_group_id to beaches_gold.fid (matches beach_dog_policy
-- shape exactly).
--
-- After this lands:
--   * Slice 2.2 backfills from latest successful park_url_extractions
--     (source='auto_promoted_from_production')
--   * Slice 2.3 ships admin-update-beach-amenities edge function
--     (source='manual_curator' on writes, overrides auto-promoted)

begin;

create table if not exists public.beach_amenities (
  arena_group_id      bigint primary key,

  -- Boolean amenity flags (NULL = unknown, TRUE/FALSE = known)
  has_restrooms       boolean,
  has_showers         boolean,
  has_lifeguards      boolean,
  has_drinking_water  boolean,
  has_disabled_access boolean,
  has_food            boolean,
  has_fire_pits       boolean,
  has_picnic_area     boolean,

  -- Parking + hours (text — parking_type uses 'lot','street','metered',
  -- 'mixed','none' vocabulary preserved from locations_stage; no CHECK
  -- to keep the curator flexible)
  parking_type        text,
  parking_notes       text,
  hours_text          text,

  -- Provenance
  source              text not null,
  curated_at          timestamptz not null default now(),
  notes               text
);

create index if not exists beach_amenities_source_idx
  on public.beach_amenities (source);

-- RLS: anon can SELECT (consumer surface may want to read amenities for
-- detail.html later); writes go through edge functions only with the
-- service role key. Mirrors the beaches_gold / beach_dog_policy pattern.
alter table public.beach_amenities enable row level security;

drop policy if exists "anon read beach_amenities" on public.beach_amenities;
create policy "anon read beach_amenities"
  on public.beach_amenities
  for select
  using (true);

comment on table public.beach_amenities is
  'Canonical amenity overlay for beaches_gold. Curator writes via admin-update-beach-amenities (source=manual_curator); backfilled from park_url_extractions (source=auto_promoted_from_production). FK to beaches_gold.fid via arena_group_id. See project_curator_on_canonical_tables.md.';

commit;
