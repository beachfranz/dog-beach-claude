-- Operator-level dog policy table. Populated by an LLM extractor that
-- reads the operator's actual public policy page (a URL, not training-
-- data memory). Three independent passes write disjoint columns:
--   Pass A (Haiku):  policy_found, default_rule, applies_to_all, leash_required
--   Pass B (Sonnet): time_windows, seasonal_closures, spatial_zones
--   Pass C (Sonnet): exceptions, ordinance_reference, summary
-- Per-pass timestamps + confidences let us re-run individual passes.

alter table public.operators
  add column if not exists dog_policy_url text;

create table if not exists public.operator_dogs_policy (
  operator_id        bigint primary key references public.operators(id),

  -- Pass A: Headline (Haiku)
  policy_found       boolean,
  default_rule       text check (default_rule in ('yes','no','restricted')),
  applies_to_all     boolean,
  leash_required     boolean,
  pass_a_confidence  numeric,
  pass_a_quotes      jsonb,
  pass_a_at          timestamptz,

  -- Pass B: Restrictions (Sonnet)
  time_windows       jsonb,
  seasonal_closures  jsonb,
  spatial_zones      jsonb,
  pass_b_confidence  numeric,
  pass_b_quotes      jsonb,
  pass_b_at          timestamptz,

  -- Pass C: Exceptions + meta (Sonnet)
  exceptions         jsonb,
  ordinance_reference text,
  summary            text,
  pass_c_confidence  numeric,
  pass_c_quotes      jsonb,
  pass_c_at          timestamptz,

  -- Provenance
  source_url         text not null,
  verified_by        text not null check (verified_by in ('llm','manual','admin_review')),
  notes              text,
  created_at         timestamptz default now(),
  updated_at         timestamptz default now()
);

create index if not exists operator_dogs_policy_default_rule_idx
  on public.operator_dogs_policy(default_rule);

grant select on public.operator_dogs_policy to anon, authenticated;
alter table public.operator_dogs_policy disable row level security;
