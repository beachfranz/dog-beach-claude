-- 20260513_gold_set_curation_queue.sql
--
-- Queue + sign-off + gate function for the gold-set curator phase.
--
-- Phase 26.5 of the launch pipeline:
--   1. After Phase 26 (operator extractions), sample_gold_set_candidates(state)
--      writes ~25 beaches × 7 fields with diversity stratification to the queue.
--   2. Curator works through queue via admin/gold-set-curator-v3.html?queue=STATE,
--      writing truth_value to beach_policy_gold_set + flipping queue.status.
--   3. Dagster gold_set_review_gate asset polls gold_set_curation_complete_for_state(state);
--      passes when queue empty or explicit signoff. Run tag gold_set_bypass=true
--      skips the gate (engineering use only — logged to gold_set_bypass_audit).
--
-- See project_gold_set_curator_queue.md for full design.

begin;

-- ────────────────────────────────────────────────────────────────────────
-- 1. The queue table — one row per (state, gold_fid, field_name) candidate
-- ────────────────────────────────────────────────────────────────────────

create table if not exists public.gold_set_curation_queue (
  id                       bigserial primary key,
  state                    text not null,
  gold_fid                 bigint not null
                             references public.beaches_gold(fid) on delete cascade,
  field_name               text not null,
  sampled_at               timestamptz not null default now(),
  sampled_reason           text,   -- 'low_conf' / 'high_conf' / 'federal_op' / 'city_op' / 'state_op' / 'county_op' / 'mixed_outcome' / 'no_dogs' / 'seasonal'
  current_extraction_value jsonb,
  current_source           text,   -- 'cpad_unit' / 'operator' / 'park_url' / 'state_dogs_policy_v1' / 'unified_v1' / etc.
  current_source_url       text,
  current_confidence       numeric,
  status                   text not null default 'pending'
                             check (status in ('pending','reviewed','skipped')),
  reviewed_at              timestamptz,
  reviewed_by              text,
  unique (state, gold_fid, field_name)
);

create index if not exists gold_set_curation_queue_state_status_idx
  on public.gold_set_curation_queue (state, status);

create index if not exists gold_set_curation_queue_fid_idx
  on public.gold_set_curation_queue (gold_fid);

comment on table public.gold_set_curation_queue is
  'Phase 26.5 — auto-sampled candidates awaiting human curator review. See project_gold_set_curator_queue.md.';

-- ────────────────────────────────────────────────────────────────────────
-- 2. Sign-off table — explicit "this state is curated enough for production"
-- ────────────────────────────────────────────────────────────────────────

create table if not exists public.gold_set_signoff (
  id           bigserial primary key,
  state        text not null unique,
  approved     boolean not null default false,
  approved_at  timestamptz,
  approved_by  text,
  rows_in_set  int,
  notes        text
);

comment on table public.gold_set_signoff is
  'Per-state explicit sign-off for the gold-set curator phase. Set approved=true to unblock pipeline regardless of pending queue rows.';

-- ────────────────────────────────────────────────────────────────────────
-- 3. Bypass audit — logs every run that skipped the gate
-- ────────────────────────────────────────────────────────────────────────

create table if not exists public.gold_set_bypass_audit (
  id            bigserial primary key,
  state         text not null,
  dagster_run_id text,
  bypassed_at   timestamptz not null default now(),
  reason        text,
  caller        text   -- 'dagster_tag' / 'cli_flag' / 'env_var'
);

create index if not exists gold_set_bypass_audit_state_idx
  on public.gold_set_bypass_audit (state, bypassed_at desc);

comment on table public.gold_set_bypass_audit is
  'Records every state_launch_job run that skipped the gold-set gate via gold_set_bypass=true tag. Engineering-only — production launches should never appear here.';

-- ────────────────────────────────────────────────────────────────────────
-- 4. Gate function — returns true when state is ready to proceed past 26.5
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.gold_set_curation_complete_for_state(p_state text)
returns boolean
language plpgsql
stable
set search_path to 'public', 'pg_catalog'
as $function$
declare
  v_signoff       boolean;
  v_queue_total   int;
  v_queue_pending int;
begin
  -- Explicit sign-off short-circuits
  select approved into v_signoff
    from public.gold_set_signoff
   where state = p_state;
  if v_signoff is true then return true; end if;

  -- Queue must exist (sampler ran for this state)
  select count(*) into v_queue_total
    from public.gold_set_curation_queue
   where state = p_state;
  if v_queue_total = 0 then return false; end if;

  -- All queue rows must be terminal (reviewed or skipped)
  select count(*) into v_queue_pending
    from public.gold_set_curation_queue
   where state = p_state and status = 'pending';
  return v_queue_pending = 0;
end $function$;

grant execute on function public.gold_set_curation_complete_for_state(text)
  to anon, authenticated, service_role;

-- ────────────────────────────────────────────────────────────────────────
-- 5. Bypass-audit helper — Dagster asset calls this when tag is set
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.gold_set_log_bypass(
  p_state text, p_run_id text, p_reason text, p_caller text default 'dagster_tag'
) returns void
language sql
security definer
set search_path to 'public', 'pg_catalog'
as $function$
  insert into public.gold_set_bypass_audit (state, dagster_run_id, reason, caller)
  values (p_state, p_run_id, p_reason, p_caller);
$function$;

grant execute on function public.gold_set_log_bypass(text, text, text, text)
  to anon, authenticated, service_role;

commit;
