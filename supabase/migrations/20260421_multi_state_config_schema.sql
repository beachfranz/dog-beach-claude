-- Multi-state pipeline configuration tables. Phase 1 — schema only, no data
-- seeded here (that's Phase 2). None of the existing v2 edge functions read
-- from these tables yet; creating them is purely additive and non-breaking.
--
-- Goal: make the pipeline config-driven so adding a new state (Oregon,
-- Washington, Hawaii, ...) becomes a matter of inserting rows rather than
-- writing new edge functions.

begin;

-- ── pipeline_sources ───────────────────────────────────────────────────────
-- GIS endpoints the pipeline polls, per state or national. Each source_key
-- corresponds to a role in the pipeline ("federal_polygon",
-- "state_park_polygon", "coastal_access_points", etc.). Multiple rows for
-- the same (source_key, state) are allowed — they're tried in priority order.

create table if not exists public.pipeline_sources (
  id              bigserial primary key,
  source_key      text not null,
  state_code      text,                 -- NULL = national source
  kind            text not null check (kind in ('polygon','point','polyline','rest_json')),
  url             text not null,
  query_defaults  jsonb default '{}'::jsonb,
  field_map       jsonb default '{}'::jsonb,
  priority        integer default 100,
  active          boolean default true,
  notes           text,
  created_at      timestamptz default now()
);

create index if not exists idx_pipeline_sources_lookup
  on public.pipeline_sources (source_key, state_code, active, priority);

-- ── state_config ───────────────────────────────────────────────────────────
-- Per-state behavior switches. coastal_default_tier drives whether a coastal
-- beach that escaped all polygon matches should default to state (Oregon
-- Beach Bill model) or county (California model).

create table if not exists public.state_config (
  state_code                 text primary key,
  state_name                 text not null,
  enabled                    boolean default true,
  coastal_default_tier       text not null default 'county'
    check (coastal_default_tier in ('state','county')),
  coastal_default_body       text,                -- e.g. 'Oregon Parks and Recreation Department (Ocean Shore)'
  has_coastal_access_source  boolean default false,
  research_context_notes     text,                -- legal / cultural context inserted into Claude prompts
  created_at                 timestamptz default now()
);

-- ── park_operators ─────────────────────────────────────────────────────────
-- Generalizes the CA-specific state_park_operators table. Maps state-owned
-- park units that are operationally run by a city or county under lease.

create table if not exists public.park_operators (
  state_code             text not null,
  park_name              text not null,
  operator_jurisdiction  text not null check (operator_jurisdiction in ('governing city','governing county')),
  operator_body          text not null,
  notes                  text,
  created_at             timestamptz default now(),
  primary key (state_code, park_name)
);

-- ── private_land_zones ─────────────────────────────────────────────────────
-- Generalizes the Del Monte Forest bbox. Private land where beaches in the
-- zone should be marked invalid (not public).

create table if not exists public.private_land_zones (
  id          bigserial primary key,
  state_code  text not null,
  name        text not null,
  min_lat     double precision not null,
  max_lat     double precision not null,
  min_lon     double precision not null,
  max_lon     double precision not null,
  reason      text,
  active      boolean default true,
  created_at  timestamptz default now()
);

create index if not exists idx_private_land_zones_state
  on public.private_land_zones (state_code, active);

-- ── sma_code_mappings ──────────────────────────────────────────────────────
-- BLM Surface Management Agency code lookup. CA and national layers use the
-- same code domain; this table gives us the mapping without hardcoding it in
-- edge-function code.

create table if not exists public.sma_code_mappings (
  sma_id       integer primary key,
  agency_name  text not null,
  agency_type  text not null check (agency_type in (
    'federal','state','local','private','tribal','undetermined','other'
  )),
  is_public    boolean not null default true
);

-- ── research_prompts ───────────────────────────────────────────────────────
-- Per-state, per-tier Claude research prompt templates. The Claude call
-- concatenates system_context with the schema hint and the Tavily results.
-- Having these per-state lets us say "Oregon state parks default to dogs-
-- on-leash" vs "California state parks typically prohibit dogs on beaches".

create table if not exists public.research_prompts (
  id                  bigserial primary key,
  state_code          text not null,
  tier                text not null check (tier in ('federal','state','city','county')),
  system_context      text not null,
  output_schema_hint  text,
  active              boolean default true,
  created_at          timestamptz default now(),
  unique (state_code, tier)
);

commit;
