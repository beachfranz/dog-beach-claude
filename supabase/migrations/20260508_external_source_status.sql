-- 20260508_external_source_status.sql
--
-- Tracks per-(source, state) freshness for external geographic data
-- pre-loaded by the bulk loaders (pad_us, osm_landing, osm_amenities).
-- Lets us answer "when did X last load Y" without scanning the data
-- tables, and feeds a future quarterly staleness cron.

begin;

create table if not exists public.external_source_status (
  source          text         not null,
  state           text         not null,
  last_loaded_at  timestamptz  not null default now(),
  row_count       integer,
  status          text         not null check (status in ('ok','failed','skipped')),
  notes           text,
  primary key (source, state)
);

create index if not exists external_source_status_loaded_idx
  on public.external_source_status (last_loaded_at);

comment on table public.external_source_status is
  'Per-(source, state) freshness for bulk-loaded external geo data.
   Written by the bulk_load_*.py loaders. Read by quarterly staleness checks.';

commit;
