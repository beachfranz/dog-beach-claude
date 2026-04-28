-- Per-CPAD-unit dog policy extracted from CPAD-provided URLs.
-- Independent of CCC. One row per CPAD unit_id whose dog policy we
-- have extracted from its park_url (preferred) or agncy_web (fallback).
--
-- This table is the canonical "what does the operator's own page say
-- about dogs at THIS unit" — replaces nothing; sits alongside
-- park_url_extractions (per-fid, beach-grained) and
-- operator_dogs_policy (per-agency, agency-grained).

create table if not exists public.cpad_unit_dogs_policy (
  cpad_unit_id           integer primary key,
  unit_name              text,
  agency_name            text,

  url_used               text not null,
  url_kind               text not null check (url_kind in ('park_url','agncy_web')),

  dogs_allowed           text check (dogs_allowed in ('yes','no','restricted','unknown')),
  default_rule           text check (default_rule in ('yes','no','restricted','unknown')),
  leash_required         boolean,

  exceptions             jsonb,    -- [{beach_name, rule, source_quote}]
  time_windows           jsonb,    -- [{description, start_hour, end_hour}]
  seasonal_rules         jsonb,    -- [{description, start_date, end_date}]

  source_quote           text,
  ordinance_ref          text,

  extraction_model       text,     -- LLM used (e.g., claude-haiku-4-5-20251001)
  extraction_confidence  numeric,
  scraped_at             timestamptz not null default now()
);

-- Note: no FK to cpad_units(unit_id) because cpad_units' PK is objectid;
-- unit_id is unique-in-practice but not constrained. The PK above
-- prevents duplicates within this table.

create index if not exists cpad_unit_dogs_policy_agency_idx
  on public.cpad_unit_dogs_policy (agency_name);
create index if not exists cpad_unit_dogs_policy_url_kind_idx
  on public.cpad_unit_dogs_policy (url_kind);
create index if not exists cpad_unit_dogs_policy_default_idx
  on public.cpad_unit_dogs_policy (default_rule);
