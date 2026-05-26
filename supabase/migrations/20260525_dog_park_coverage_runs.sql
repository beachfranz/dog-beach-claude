-- 20260525_dog_park_coverage_runs.sql
--
-- Per-op run metrics for the dog_park_pipeline (Dagster wrapping).
-- One row per (state, op, run). Enables per-state observability and
-- "skip if recent successful run" freshness guards in Dagster.

begin;

create table if not exists public.dog_park_coverage_runs (
  id          bigserial primary key,
  state       text   not null,
  op          text   not null,         -- preflight_check / pip_address_city_backfill / ...
  started_at  timestamptz not null,
  ended_at    timestamptz not null,
  metrics     jsonb,
  created_at  timestamptz not null default now()
);

create index if not exists dpcr_state_op_idx       on public.dog_park_coverage_runs (state, op, started_at desc);
create index if not exists dpcr_started_at_idx     on public.dog_park_coverage_runs (started_at desc);

comment on table public.dog_park_coverage_runs is
  'Per-op run metrics for dog_park_pipeline (Dagster wrapping). One row per (state, op, run). Used for freshness guards + observability.';

alter table public.dog_park_coverage_runs enable row level security;
create policy "dpcr_service_role_all" on public.dog_park_coverage_runs
  for all using (auth.role() = 'service_role');

commit;
