-- 20260506_pipeline_runs.sql
--
-- Audit table for the homegrown orchestrator at scripts/orchestrator/.
-- Every pipeline invocation (manual, scheduled, sensor-fired, called by
-- another script) writes a row here. The admin/pipelines.html page reads
-- it to show "is this pipeline fresh? did it succeed last time?".
--
-- Why a homegrown orchestrator: Dagster requires Python 3.13 (we're on
-- 3.14), and for ~10 scripts run by one person the install overhead +
-- learning curve outweighed the benefits. ~150 lines of Python + this
-- table + an HTML page covers ~80% of Dagster's value.

begin;

create table if not exists public.pipeline_runs (
  id              bigserial primary key,
  pipeline_name   text not null,
  trigger_kind    text default 'manual' not null
                  check (trigger_kind in ('manual','schedule','sensor','script_call')),
  started_at      timestamptz default now() not null,
  finished_at     timestamptz,
  status          text default 'running' not null
                  check (status in ('running','success','failed','skipped','timeout')),
  exit_code       int,
  stdout_tail     text,
  stderr_tail     text,
  notes           text,
  duration_seconds numeric generated always as (
    extract(epoch from (finished_at - started_at))
  ) stored
);

create index if not exists pipeline_runs_name_started
  on public.pipeline_runs (pipeline_name, started_at desc);
create index if not exists pipeline_runs_status_started
  on public.pipeline_runs (status, started_at desc);

-- View: latest row per pipeline, for the admin status page
drop view if exists public.pipeline_runs_latest;
create view public.pipeline_runs_latest as
select distinct on (pipeline_name)
       pipeline_name,
       started_at      as last_started_at,
       finished_at     as last_finished_at,
       status          as last_status,
       exit_code       as last_exit_code,
       duration_seconds as last_duration_seconds,
       trigger_kind    as last_trigger_kind,
       (select count(*) from public.pipeline_runs r2
         where r2.pipeline_name = pipeline_runs.pipeline_name) as total_runs,
       (select count(*) from public.pipeline_runs r2
         where r2.pipeline_name = pipeline_runs.pipeline_name
           and r2.status = 'success') as success_runs,
       (select count(*) from public.pipeline_runs r2
         where r2.pipeline_name = pipeline_runs.pipeline_name
           and r2.status = 'failed') as failed_runs
  from public.pipeline_runs
 order by pipeline_name, started_at desc;

-- RLS: anon can SELECT (admin status page reads this); writes via service role.
alter table public.pipeline_runs enable row level security;

drop policy if exists pipeline_runs_anon_read on public.pipeline_runs;
create policy pipeline_runs_anon_read
  on public.pipeline_runs for select
  to anon, authenticated
  using (true);

grant select on public.pipeline_runs_latest to anon, authenticated;

commit;
