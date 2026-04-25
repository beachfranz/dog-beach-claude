-- us_beach_points_staging — new catalog ingest staging table (2026-04-24)
-- Replaces beaches_staging_new in the refactored catalog ingest pipeline.
-- The old table is left in place as historical until the new pipeline is
-- fully cut over.
--
-- Design captured in memory project_staging_schema_v2:
--   - 50 columns covering identity, location (raw + spatial-truth),
--     governance, access, dogs, hours, parking, amenities, NOAA, misc.
--   - Provenance offloaded to side table beach_enrichment_provenance.
--   - 4 enum-like CHECK constraints lock down free-text drift.
--   - Geographic columns split: address_* (input from source) vs
--     spatial-truth (state_code, county_*, place_*) from PostGIS joins.
--
-- Convention: geom is geography(Point, 4326). All distances use
-- ::geography casts. See project_crs_convention memory.

-- ── 1. Main staging table ────────────────────────────────────────────────
create table if not exists public.us_beach_points_staging (
  -- Identity
  fid              int          primary key,
  display_name     text         not null,
  is_active        boolean      not null default true,
  timezone         text,                                         -- IANA tz, e.g. 'America/Los_Angeles'

  -- Location (raw — what the source CSV / external feed said)
  raw_address      text,
  address_street   text,
  address_city     text,
  address_county   text,
  address_state    text,
  address_zip      text,

  -- Spatial truth (derived from PostGIS joins — authoritative)
  latitude         double precision,
  longitude        double precision,
  geom             geography(Point, 4326),
  state_code       text,                                         -- 2-letter, from states polygon
  county_name      text,
  county_fips      text,                                         -- 5-digit
  place_name       text,                                         -- TIGER Places, null if not in any incorporated/CDP
  place_fips       text,                                         -- TIGER PLACEFIPS
  place_type       text,                                         -- C1/C5/U1/U2/etc.

  -- Governance (operator — owner stays in CPAD, joined when needed)
  governing_body_type text check (governing_body_type in
    ('city','county','state','federal','tribal','private','unknown')),
  governing_body_name text,

  -- Access (general public access right — distinct from dog policy)
  access_status text check (access_status in
    ('public','private','restricted','unknown')),

  -- Dogs (rules)
  dogs_allowed text check (dogs_allowed in
    ('yes','no','seasonal','restricted','unknown')),
  dogs_leash_required text check (dogs_leash_required in
    ('required','off_leash_ok','mixed','unknown')),
  -- Hours when dogs are RESTRICTED — array of {start: HH:MM, end: HH:MM}
  -- Empty/null = no daily time restrictions.
  dogs_restricted_hours jsonb,
  -- Seasonal overrides — array of {from: MM-DD, to: MM-DD,
  -- restricted_hours: [{start, end}], notes: text}.
  -- Non-empty rows replace dogs_restricted_hours during the date range.
  dogs_seasonal_rules   jsonb,
  -- Free-form zone description: "north of tower 5", "entire beach", "nowhere"
  dogs_zone_description text,

  -- Hours (structured + text escape hatch)
  open_time   time,
  close_time  time,
  hours_text  text,                                                -- "dawn to dusk" / "24 hours" / "6am–10pm"

  -- Parking
  has_parking   boolean,                                           -- nullable = unknown
  parking_type  text check (parking_type is null or parking_type in
    ('lot','street','metered','mixed','none')),
  parking_notes text,

  -- Amenities (all nullable booleans → tri-state in admin UI)
  has_restrooms       boolean,
  has_showers         boolean,
  has_drinking_water  boolean,
  has_lifeguards      boolean,
  has_disabled_access boolean,
  has_food            boolean,
  has_fire_pits       boolean,
  has_picnic_area     boolean,

  -- NOAA tide station (deterministic spatial KNN — distance is the truth)
  noaa_station_id          text,
  noaa_station_name        text,
  noaa_station_distance_m  integer,

  -- Misc user-facing
  description text,
  website     text,

  -- Pipeline metadata (row-level "freshness" — not data provenance)
  pipeline_last_run_at timestamptz,
  pipeline_version     text,

  -- Audit
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Indexes
create index if not exists ubps_geom_idx        on public.us_beach_points_staging using gist (geom);
create index if not exists ubps_state_idx       on public.us_beach_points_staging (state_code);
create index if not exists ubps_county_fips_idx on public.us_beach_points_staging (county_fips);
create index if not exists ubps_place_fips_idx  on public.us_beach_points_staging (place_fips);
create index if not exists ubps_active_idx      on public.us_beach_points_staging (is_active) where is_active = true;

-- updated_at touch trigger
create or replace function public.tg_touch_updated_at() returns trigger as $$
begin
  new.updated_at := now();
  return new;
end;
$$ language plpgsql;

drop trigger if exists ubps_touch_updated_at on public.us_beach_points_staging;
create trigger ubps_touch_updated_at
  before update on public.us_beach_points_staging
  for each row execute function public.tg_touch_updated_at();

-- Documentation
comment on table  public.us_beach_points_staging is
  'New catalog ingest staging table (2026-04-24 design). Replaces beaches_staging_new. 50 columns covering beach identity, raw + canonical location, governance, access, dogs policy, hours, parking, amenities, NOAA. Provenance lives in beach_enrichment_provenance. See memory project_staging_schema_v2.';
comment on column public.us_beach_points_staging.geom is
  'Authoritative spatial location. EPSG:4326. Cast to ::geography for meter math.';
comment on column public.us_beach_points_staging.state_code is
  'Spatial-truth (from states polygon join). May disagree with address_state — pipeline flags mismatches.';
comment on column public.us_beach_points_staging.governing_body_type is
  'Operator type (CPAD MNG_AG_LEV-style classification). Source precedence in beach_enrichment_provenance: manual > cpad > tiger_places > llm.';
comment on column public.us_beach_points_staging.access_status is
  'General public access right. Distinct from dog policy. Source precedence: manual > plz > cpad > ccc > llm.';
comment on column public.us_beach_points_staging.dogs_restricted_hours is
  'jsonb array of {start: HH:MM, end: HH:MM}. Hours when dogs are RESTRICTED (frame matches scoring engine isProhibited). Null/empty = no daily restriction.';
comment on column public.us_beach_points_staging.dogs_seasonal_rules is
  'jsonb array of {from: MM-DD, to: MM-DD, restricted_hours: [...], notes: text}. Overrides dogs_restricted_hours for date ranges (e.g. Aliso Beach Jun 15–Sep 10).';
comment on column public.us_beach_points_staging.pipeline_last_run_at is
  'When the pipeline last touched this row, regardless of whether anything changed. Used to find stale rows for re-enrichment.';
comment on column public.us_beach_points_staging.pipeline_version is
  'Schema/pipeline version that produced this row. Bump when the contract changes.';

-- ── 2. Side table — per-(fid, field_group) provenance ────────────────────
create table if not exists public.beach_enrichment_provenance (
  fid         int  not null references public.us_beach_points_staging(fid) on delete cascade,
  field_group text not null check (field_group in
    ('governance','access','dogs','practical')),
  source      text check (source in
    ('manual','plz','cpad','tiger_places','ccc','llm','web_scrape')),
  source_url  text,
  confidence  numeric(3,2) check (confidence is null or (confidence >= 0 and confidence <= 1)),
  updated_at  timestamptz not null default now(),
  notes       text,
  primary key (fid, field_group)
);

create index if not exists bep_field_group_idx on public.beach_enrichment_provenance(field_group);
create index if not exists bep_source_idx      on public.beach_enrichment_provenance(source);

drop trigger if exists bep_touch_updated_at on public.beach_enrichment_provenance;
create trigger bep_touch_updated_at
  before update on public.beach_enrichment_provenance
  for each row execute function public.tg_touch_updated_at();

comment on table public.beach_enrichment_provenance is
  'Per-(beach, field_group) provenance for us_beach_points_staging. 4 field_groups: governance, access, dogs, practical. NOAA omitted (deterministic). History supported later by adding a version int + dropping PK uniqueness.';
comment on column public.beach_enrichment_provenance.source is
  'Where this group of fields came from. plz = private_land_zones bbox override (form of manual). web_scrape = generic page scrape outside the LLM extraction pipeline.';
comment on column public.beach_enrichment_provenance.confidence is
  '0.00–1.00. Manual = 1.00. CPAD = 0.80–0.95 (by match quality). TIGER Places = 0.65–0.75. LLM = 0.40–0.80 (by page clarity).';
