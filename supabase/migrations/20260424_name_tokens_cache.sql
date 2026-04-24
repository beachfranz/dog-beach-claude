-- Materialize name_tokens on CPAD + CCC as stored generated columns, add
-- GIN indexes for fast array-overlap joins (2026-04-24).
--
-- Motivation: list_orphan_geocode_flags was ~9s because the query evaluated
-- public.shared_name_tokens() per row across thousands of CPAD polygons.
-- A stored generated column + GIN index lets Postgres narrow candidates
-- via index scan before calling any function.

-- ── CPAD ──────────────────────────────────────────────────────────────────
alter table public.cpad_units
  add column if not exists name_tokens_cache text[]
  generated always as (public.name_tokens(unit_name)) stored;

create index if not exists cpad_units_name_tokens_gin
  on public.cpad_units using gin (name_tokens_cache);

-- ── CCC ───────────────────────────────────────────────────────────────────
alter table public.ccc_access_points
  add column if not exists name_tokens_cache text[]
  generated always as (public.name_tokens(name)) stored;

create index if not exists ccc_access_points_name_tokens_gin
  on public.ccc_access_points using gin (name_tokens_cache);

-- ── CSP (used later for name-match against state parks) ──────────────────
alter table public.csp_parks
  add column if not exists name_tokens_cache text[]
  generated always as (public.name_tokens(unit_name)) stored;

create index if not exists csp_parks_name_tokens_gin
  on public.csp_parks using gin (name_tokens_cache);
