-- Genericise cpad_unit_dog_extractions into a multi-question table.
-- Each row is one answer to one question (response_scope) about one
-- CPAD unit URL. Today response_scope='dogs_allowed' is the only one
-- populated; future scopes might be 'parking_available', 'fees',
-- 'hours', 'restrooms', etc.
--
-- Changes:
--   1. Rename table:   cpad_unit_dog_extractions  → cpad_unit_responses
--   2. Add column:     response_scope text not null
--   3. Rename columns: dogs_allowed/reason/confidence → response_*
--                     has_dog_keywords → has_keywords
--   4. Drop the CHECK on the answer enum (different scopes will use
--      different vocabularies; constraint at the script layer)
--   5. Widen unique key to include response_scope
--   6. Recreate the "current best answer" view, parameterised by scope.

alter table public.cpad_unit_dog_extractions
  rename to cpad_unit_responses;

-- Existing rows are all dogs_allowed; backfill before adding the not-null.
alter table public.cpad_unit_responses
  add column if not exists response_scope text;
update public.cpad_unit_responses
  set response_scope = 'dogs_allowed'
  where response_scope is null;
alter table public.cpad_unit_responses
  alter column response_scope set not null;

-- Generic column names
alter table public.cpad_unit_responses
  rename column dogs_allowed    to response_value;
alter table public.cpad_unit_responses
  rename column dogs_reason     to response_reason;
alter table public.cpad_unit_responses
  rename column dogs_confidence to response_confidence;
alter table public.cpad_unit_responses
  rename column has_dog_keywords to has_keywords;

-- Drop the dogs-specific value CHECK; vocabulary is per-scope and
-- enforced at the populator layer.
alter table public.cpad_unit_responses
  drop constraint if exists cpad_unit_dog_extractions_dogs_allowed_check;

-- Widen the uniqueness key — one answer per (unit, url, scope, scrape).
alter table public.cpad_unit_responses
  drop constraint if exists cpad_unit_dog_extractions_cpad_unit_id_source_url_scraped_a_key;
create unique index if not exists cpad_unit_responses_unique
  on public.cpad_unit_responses (cpad_unit_id, source_url, response_scope, scraped_at);

-- Re-point the supporting indexes to the new table name (Postgres
-- already moved them silently during the rename, but make the names
-- match the new table).
alter index if exists cpad_unit_dog_extractions_unit_idx
  rename to cpad_unit_responses_unit_idx;
alter index if exists cpad_unit_dog_extractions_status_idx
  rename to cpad_unit_responses_status_idx;
alter index if exists cpad_unit_dog_extractions_url_idx
  rename to cpad_unit_responses_url_idx;
create index if not exists cpad_unit_responses_scope_idx
  on public.cpad_unit_responses (response_scope);

-- Replace the view with one keyed by scope.
drop view if exists public.cpad_unit_dog_current;
create or replace view public.cpad_unit_response_current as
  select distinct on (cpad_unit_id, response_scope)
    cpad_unit_id, response_scope, source_url,
    response_value, response_reason, response_confidence,
    extracted_at
  from public.cpad_unit_responses
  where fetch_status = 'success' and response_value is not null
  order by cpad_unit_id, response_scope,
           response_confidence desc nulls last, extracted_at desc;
