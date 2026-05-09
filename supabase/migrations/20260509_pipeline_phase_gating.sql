-- 20260509_pipeline_phase_gating.sql
--
-- Canon (Franz, 2026-05-09): every process in a phase must complete
-- successfully before the next phase starts. No more best-effort
-- silent-skip behavior.
--
-- This migration adds:
--   1. public.pipeline_phase_status — per-(state, run_id, phase) tracker
--   2. public.run_pipeline_phase(p_state, p_run_id, p_phase, p_action_sql, p_criterion_sql)
--      — generic phase runner that captures errors, validates a criterion,
--        records status, and RAISES on failure
--   3. Phase-specific criterion helpers as needed (added incrementally as
--      we rewrite each phase to use the gate)
--
-- The migration does NOT yet rewrite run_pipeline_for_state to use the
-- gate — that's a follow-on per-phase refactor. This ships the
-- infrastructure so subsequent phase rewrites have a target to plug into.

begin;

-- ── Tracker table ──────────────────────────────────────────────────
create table if not exists public.pipeline_phase_status (
  run_id          bigint  not null,
  state_code      text    not null,
  phase           text    not null,
  status          text    not null check (status in ('in_progress','ok','failed','skipped')),
  started_at      timestamptz not null default now(),
  finished_at     timestamptz,
  rows_affected   integer,
  criterion_met   boolean,
  criterion_text  text,
  error_message   text,
  notes           jsonb,
  primary key (run_id, state_code, phase)
);

create index if not exists pipeline_phase_status_state_phase_idx
  on public.pipeline_phase_status (state_code, phase, status);

create index if not exists pipeline_phase_status_run_idx
  on public.pipeline_phase_status (run_id, started_at);

comment on table public.pipeline_phase_status is
  'Per-(state, run_id, phase) status ledger. Every phase must record an ok row before the next phase fires. Resumable: re-run with same run_id picks up at first non-ok phase; new run_id starts over.';


-- ── Phase runner ───────────────────────────────────────────────────
-- Records 'in_progress', executes the action SQL, validates the criterion,
-- records 'ok' or 'failed', and RAISES on failure so the caller halts.
-- The action SQL must be a single SELECT or PERFORM-able expression that
-- returns an integer rows_affected. The criterion SQL must return a single
-- boolean (true = pass, false = fail).
create or replace function public.run_pipeline_phase(
  p_run_id        bigint,
  p_state_code    text,
  p_phase         text,
  p_action_sql    text,
  p_criterion_sql text default null,
  p_criterion_text text default null
)
returns table(rows_affected int, status text, error_msg text)
language plpgsql
security definer
as $$
declare
  v_rows int := 0;
  v_pass boolean := true;
  v_err  text := null;
begin
  insert into public.pipeline_phase_status (run_id, state_code, phase, status, started_at)
  values (p_run_id, p_state_code, p_phase, 'in_progress', now())
  on conflict (run_id, state_code, phase) do update
    set status = 'in_progress', started_at = now(), finished_at = null,
        rows_affected = null, criterion_met = null, error_message = null;

  begin
    execute p_action_sql into v_rows;
  exception when others then
    v_err := format('action error: %s', sqlerrm);
    update public.pipeline_phase_status
       set status = 'failed', finished_at = now(), error_message = v_err
     where run_id = p_run_id and state_code = p_state_code and phase = p_phase;
    raise exception 'phase % failed for state %: %', p_phase, p_state_code, v_err;
  end;

  if p_criterion_sql is not null then
    begin
      execute p_criterion_sql into v_pass;
    exception when others then
      v_err := format('criterion error: %s', sqlerrm);
      v_pass := false;
    end;
  end if;

  if v_pass then
    update public.pipeline_phase_status
       set status = 'ok', finished_at = now(), rows_affected = v_rows,
           criterion_met = true, criterion_text = p_criterion_text
     where run_id = p_run_id and state_code = p_state_code and phase = p_phase;
    return query select v_rows, 'ok'::text, null::text;
  else
    v_err := coalesce(v_err, format('criterion failed: %s', coalesce(p_criterion_text, '(no text)')));
    update public.pipeline_phase_status
       set status = 'failed', finished_at = now(), rows_affected = v_rows,
           criterion_met = false, criterion_text = p_criterion_text, error_message = v_err
     where run_id = p_run_id and state_code = p_state_code and phase = p_phase;
    raise exception 'phase % criterion failed for state %: % (rows_affected=%)',
      p_phase, p_state_code, p_criterion_text, v_rows;
  end if;
end $$;


-- ── Resumability helper ────────────────────────────────────────────
-- Returns true if the given (state, phase) is already 'ok' for any run_id
-- in the last p_within_hours. Lets the orchestrator skip already-done
-- phases on re-run (unless force=true).
create or replace function public.pipeline_phase_already_ok(
  p_state_code   text,
  p_phase        text,
  p_within_hours int default 24
) returns boolean
language sql
stable
as $$
  select exists(
    select 1 from public.pipeline_phase_status
     where state_code = p_state_code and phase = p_phase and status = 'ok'
       and finished_at > now() - make_interval(hours => p_within_hours)
  )
$$;


-- ── Run-id sequence ────────────────────────────────────────────────
create sequence if not exists public.pipeline_run_id_seq;

create or replace function public.next_pipeline_run_id() returns bigint
language sql as $$ select nextval('public.pipeline_run_id_seq') $$;

grant execute on function public.run_pipeline_phase(bigint, text, text, text, text, text)
  to anon, authenticated, service_role;
grant execute on function public.pipeline_phase_already_ok(text, text, int)
  to anon, authenticated, service_role;
grant execute on function public.next_pipeline_run_id()
  to anon, authenticated, service_role;
grant select on public.pipeline_phase_status to anon, authenticated, service_role;

commit;
