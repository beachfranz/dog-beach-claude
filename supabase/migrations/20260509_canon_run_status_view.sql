-- 20260509_canon_run_status_view.sql
--
-- Exposes orchestrator phase status (run_state_pipeline.py writes to
-- public.pipeline_phase_status) as a per-state×phase view filtered to
-- the latest run_id per state. Used by admin/pipeline-monitor.html to
-- show canon progress for each launched state.

begin;

-- Latest status per (state, phase) across ALL runs. This is the right
-- semantic because --phase-from creates partial runs that only record
-- rows for the phases that ran — taking just the latest run_id would
-- hide all the previously-completed phases. Surfacing latest-per-phase
-- gives a true "where is this state at?" view.
create or replace view public.canon_run_status as
select distinct on (state_code, phase)
       state_code,
       run_id,
       phase,
       status,
       started_at,
       finished_at,
       rows_affected,
       criterion_met,
       criterion_text,
       error_message,
       extract(epoch from coalesce(finished_at, now()) - started_at)::int
         as duration_s
  from public.pipeline_phase_status
 order by state_code, phase, started_at desc;

grant select on public.canon_run_status to anon, authenticated, service_role;

comment on view public.canon_run_status is
  'One row per (state, phase) for the latest orchestrator run per state. '
  'Backs admin/pipeline-monitor.html canon section.';

commit;
