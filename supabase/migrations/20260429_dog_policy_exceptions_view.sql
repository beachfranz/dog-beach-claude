-- Flatten operator_dogs_policy.exceptions[] and
-- cpad_unit_dogs_policy.exceptions[] jsonb arrays into a single
-- queryable view, one row per exception.
--
-- Source-of-truth remains the jsonb columns on the parent tables —
-- this is purely a read view. Cascade and UPDATE migrations still
-- write to the jsonb columns directly; this view auto-reflects.
--
-- Schema:
--   source_kind     'operator' | 'cpad_unit'
--   source_id       operator_id or cpad_unit_id
--   parent_name     operator's display name or cpad unit_name
--   rule            from exception object: 'off_leash' | 'allowed' | 'no' | etc.
--   beach_name      the beach the exception applies to
--   source_quote    the source-text quote justifying the exception
--   source_url      url where the source quote was found (may be null)
--
-- Counts as of 2026-04-29: 209 operator exceptions across 111 operators,
-- 76 cpad_unit exceptions across 226 cpad units.

create or replace view public.dog_policy_exceptions as
  select
    'operator'                 as source_kind,
    odp.operator_id            as source_id,
    coalesce(o.short_name, o.canonical_name) as parent_name,
    e->>'rule'                 as rule,
    e->>'beach_name'           as beach_name,
    e->>'source_quote'         as source_quote,
    e->>'source_url'           as source_url
  from public.operator_dogs_policy odp
  left join public.operators o on o.id = odp.operator_id
  cross join lateral jsonb_array_elements(coalesce(odp.exceptions, '[]'::jsonb)) e

  union all

  select
    'cpad_unit'                as source_kind,
    cup.cpad_unit_id           as source_id,
    cup.unit_name              as parent_name,
    e->>'rule'                 as rule,
    e->>'beach_name'           as beach_name,
    e->>'source_quote'         as source_quote,
    e->>'source_url'           as source_url
  from public.cpad_unit_dogs_policy cup
  cross join lateral jsonb_array_elements(coalesce(cup.exceptions, '[]'::jsonb)) e;

comment on view public.dog_policy_exceptions is
  'Flattened view of operator + cpad-unit exception lists. One row per exception, source_kind distinguishes the two sources.';
