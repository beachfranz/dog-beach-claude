-- Master CA operators catalog. One row per real-world operating entity:
-- city, county, state agency, federal agency, tribe, special district.
--
-- Replaces the 10-row hand-curated park_operators / state_park_operators
-- compatibility shims. Bootstrap-populated from CPAD mng_agncy +
-- jurisdictions (TIGER places) + counties (TIGER counties) + small
-- federal/tribal seed lists in a follow-up migration.
--
-- Geometry is borrowed by FK from jurisdictions / counties — never
-- duplicated. Operators with no jurisdictional polygon (NPS units, BLM
-- regions, tribal lands) get geometry attached separately when needed.

create table if not exists public.operators (
  id              bigserial primary key,

  -- Identity
  slug            text not null unique,            -- 'ca-state-parks', 'city-of-newport-beach'
  canonical_name  text not null,                   -- 'California Department of Parks and Recreation'
  short_name      text,                            -- 'California State Parks'
  aliases         text[] not null default '{}',    -- every variant observed in raw data

  -- Authority hierarchy
  level           text not null check (level in (
                    'federal','state','tribal','county','city',
                    'special-district','private','unknown')),
  subtype         text,                            -- nps/usfs/blm/state-parks/cdfw/port/water/etc.

  -- Geographic authority — borrowed by FK
  jurisdiction_id integer references public.jurisdictions(id),
  county_geoid    text    references public.counties(geoid),
  state_code      text    not null default 'CA',

  -- Source-system join keys
  cpad_agncy_name      text,                       -- exact CPAD mng_agncy string
  cpad_agncy_level     text,                       -- verbatim mng_ag_lev
  osm_operator_strings text[] not null default '{}',
  fips_state           text,
  fips_place           text,
  fips_county          text,

  -- Contact (sparse, fill on discovery)
  website         text,
  phone           text,
  email           text,
  permits_url     text,
  dog_policy_url  text,

  -- Denormalized counts (refreshed on demand)
  cpad_unit_count   integer not null default 0,
  ccc_point_count   integer not null default 0,
  osm_feature_count integer not null default 0,
  usbeach_count     integer not null default 0,

  -- Provenance
  origin_source   text not null check (origin_source in (
                    'cpad','tiger_places','tiger_counties',
                    'seed_federal','seed_tribal','manual')),
  notes           text,
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now()
);

create index if not exists operators_slug_idx          on public.operators(slug);
create index if not exists operators_level_idx         on public.operators(level);
create index if not exists operators_level_subtype_idx on public.operators(level, subtype);
create index if not exists operators_jurisdiction_idx  on public.operators(jurisdiction_id);
create index if not exists operators_county_idx        on public.operators(county_geoid);
create index if not exists operators_cpad_idx          on public.operators(cpad_agncy_name);
create index if not exists operators_aliases_gin       on public.operators using gin (aliases);
create index if not exists operators_osm_strings_gin   on public.operators using gin (osm_operator_strings);

comment on table public.operators is
  'Master CA operators catalog. Beach source tables (CCC, UBP, OSM, locations_stage) reference this via operator_id FK. Bootstrap-populated from CPAD + TIGER places + TIGER counties + federal/tribal seeds.';

comment on column public.operators.slug is
  'Stable human-readable key. Generated deterministically from canonical_name (lowercase, hyphenated, abbreviated for common terms). Used as the public identifier in URLs and audit trails.';

comment on column public.operators.aliases is
  'All name variants observed in CPAD agncy_name/mng_agncy, OSM operator tags, and free-text fields. Powers fuzzy match-back from raw data.';

comment on column public.operators.cpad_agncy_name is
  'Exact CPAD mng_agncy string. Primary join key when matching CPAD polygons to operators.';
