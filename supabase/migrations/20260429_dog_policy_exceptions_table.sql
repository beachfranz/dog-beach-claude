-- Promote dog_policy_exceptions from a derivation view to a real
-- first-class table. Each exception becomes a row with proper PK,
-- foreign keys to its parent (operator or cpad_unit), and a UNIQUE
-- constraint on (source_kind, source_id, beach_name).
--
-- Keeps the parent jsonb columns (operator_dogs_policy.exceptions and
-- cpad_unit_dogs_policy.exceptions) — for now they remain canonical
-- for the cascade. A trigger keeps the new table in lockstep so
-- queries against dog_policy_exceptions always reflect current state.
--
-- Phase 1 (this migration):
--   - drop the existing view
--   - create the table + sync triggers
--   - backfill from jsonb
--   - cascade SQL unchanged (still reads jsonb)
--
-- Phase 2 (later, separate migration):
--   - rewrite compute_dogs_verdict_core + compute_dogs_verdict_ccc_friendly
--     to read from this table directly
--   - drop the jsonb columns
--   - drop the sync triggers (no longer needed)

drop view if exists public.dog_policy_exceptions;

create table public.dog_policy_exceptions (
  id            bigserial primary key,
  source_kind   text not null check (source_kind in ('operator', 'cpad_unit')),
  source_id     bigint not null,
  parent_name   text,
  rule          text,
  beach_name    text,
  source_quote  text,
  source_url    text,
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);

create unique index dog_policy_exceptions_unique
  on public.dog_policy_exceptions (source_kind, source_id, beach_name);
create index dog_policy_exceptions_source_id
  on public.dog_policy_exceptions (source_kind, source_id);
create index dog_policy_exceptions_beach_name
  on public.dog_policy_exceptions (lower(beach_name));

comment on table public.dog_policy_exceptions is
  'First-class beach exception list. Each row pins one beach to a specific rule (off_leash / allowed / no / restricted) under a parent operator or CPAD unit. Source-of-truth pending Phase 2; for now the parent jsonb columns are canonical and this table is kept in sync via trigger.';
comment on column public.dog_policy_exceptions.source_kind is
  '''operator'' = pinned in operator_dogs_policy.exceptions[]; ''cpad_unit'' = pinned in cpad_unit_dogs_policy.exceptions[]';
comment on column public.dog_policy_exceptions.rule is
  'off_leash / allowed / yes / restricted = dogs allowed (with conditions); prohibited / no = dogs not allowed';

-- Backfill from existing jsonb arrays
insert into public.dog_policy_exceptions
       (source_kind, source_id, parent_name, rule, beach_name, source_quote, source_url)
select 'operator',
       odp.operator_id,
       coalesce(o.short_name, o.canonical_name),
       e->>'rule',
       e->>'beach_name',
       e->>'source_quote',
       e->>'source_url'
  from public.operator_dogs_policy odp
  left join public.operators o on o.id = odp.operator_id
  cross join lateral jsonb_array_elements(coalesce(odp.exceptions, '[]'::jsonb)) e
 where e->>'beach_name' is not null
on conflict (source_kind, source_id, beach_name) do nothing;

insert into public.dog_policy_exceptions
       (source_kind, source_id, parent_name, rule, beach_name, source_quote, source_url)
select 'cpad_unit',
       cup.cpad_unit_id,
       cup.unit_name,
       e->>'rule',
       e->>'beach_name',
       e->>'source_quote',
       e->>'source_url'
  from public.cpad_unit_dogs_policy cup
  cross join lateral jsonb_array_elements(coalesce(cup.exceptions, '[]'::jsonb)) e
 where e->>'beach_name' is not null
on conflict (source_kind, source_id, beach_name) do nothing;


-- Sync trigger: when the parent jsonb column changes, rebuild the
-- corresponding rows in dog_policy_exceptions. Pure delete-then-insert
-- of just the relevant parent's rows — small footprint per UPDATE.

create or replace function public._sync_dog_policy_exceptions_from_operator()
returns trigger language plpgsql as $$
begin
  delete from public.dog_policy_exceptions
   where source_kind = 'operator' and source_id = NEW.operator_id;

  insert into public.dog_policy_exceptions
         (source_kind, source_id, parent_name, rule, beach_name, source_quote, source_url)
  select 'operator',
         NEW.operator_id,
         coalesce(o.short_name, o.canonical_name),
         e->>'rule',
         e->>'beach_name',
         e->>'source_quote',
         e->>'source_url'
    from jsonb_array_elements(coalesce(NEW.exceptions, '[]'::jsonb)) e
    left join public.operators o on o.id = NEW.operator_id
   where e->>'beach_name' is not null
  on conflict (source_kind, source_id, beach_name) do nothing;

  return NEW;
end;
$$;

create or replace function public._sync_dog_policy_exceptions_from_cpad()
returns trigger language plpgsql as $$
begin
  delete from public.dog_policy_exceptions
   where source_kind = 'cpad_unit' and source_id = NEW.cpad_unit_id;

  insert into public.dog_policy_exceptions
         (source_kind, source_id, parent_name, rule, beach_name, source_quote, source_url)
  select 'cpad_unit',
         NEW.cpad_unit_id,
         NEW.unit_name,
         e->>'rule',
         e->>'beach_name',
         e->>'source_quote',
         e->>'source_url'
    from jsonb_array_elements(coalesce(NEW.exceptions, '[]'::jsonb)) e
   where e->>'beach_name' is not null
  on conflict (source_kind, source_id, beach_name) do nothing;

  return NEW;
end;
$$;

drop trigger if exists trg_sync_dog_policy_exceptions_op on public.operator_dogs_policy;
create trigger trg_sync_dog_policy_exceptions_op
  after insert or update of exceptions on public.operator_dogs_policy
  for each row execute function public._sync_dog_policy_exceptions_from_operator();

drop trigger if exists trg_sync_dog_policy_exceptions_cpad on public.cpad_unit_dogs_policy;
create trigger trg_sync_dog_policy_exceptions_cpad
  after insert or update of exceptions on public.cpad_unit_dogs_policy
  for each row execute function public._sync_dog_policy_exceptions_from_cpad();
