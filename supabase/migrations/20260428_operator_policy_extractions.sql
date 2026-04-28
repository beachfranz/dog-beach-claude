-- Evidence layer for operator dog policy extractions. Many extractions
-- per operator (one per source URL). The canonical operator_dogs_policy
-- row is the deterministic merge of all extraction rows.

create table if not exists public.operator_policy_extractions (
  id                 bigserial primary key,
  operator_id        bigint not null references public.operators(id),

  -- Source provenance
  source_kind        text not null check (source_kind in ('direct_url','site_search','manual')),
  source_url         text not null,
  source_query       text,                 -- for site_search: the Tavily query used
  fetch_status       text not null,        -- 'ok' | 'http_403' | 'timeout' | 'fetch_error'
  page_chars         integer,

  -- Pass A (Headline)
  pass_a_policy_found     boolean,
  pass_a_default_rule     text check (pass_a_default_rule in ('yes','no','restricted')),
  pass_a_applies_to_all   boolean,
  pass_a_leash_required   boolean,
  pass_a_quotes           jsonb,
  pass_a_confidence       numeric,
  pass_a_status           text,            -- 'ok' | 'parse_error' | 'skipped'

  -- Pass B (Restrictions)
  pass_b_time_windows     jsonb,
  pass_b_seasonal_closures jsonb,
  pass_b_spatial_zones    jsonb,
  pass_b_quotes           jsonb,
  pass_b_confidence       numeric,
  pass_b_status           text,

  -- Pass C (Exceptions + meta)
  pass_c_exceptions       jsonb,
  pass_c_ordinance        text,
  pass_c_summary          text,
  pass_c_quotes           jsonb,
  pass_c_confidence       numeric,
  pass_c_status           text,

  -- Cost / metadata
  total_input_tokens      integer,
  total_output_tokens     integer,
  extracted_at            timestamptz default now(),

  unique (operator_id, source_kind, source_url)
);

create index if not exists operator_policy_extractions_op_idx
  on public.operator_policy_extractions(operator_id);
create index if not exists operator_policy_extractions_kind_idx
  on public.operator_policy_extractions(source_kind);

grant select on public.operator_policy_extractions to anon, authenticated;
alter table public.operator_policy_extractions disable row level security;

comment on table public.operator_policy_extractions is
  'Evidence rows for operator dog policy extraction. One row per (operator, source_kind, source_url). The canonical operator_dogs_policy row is the deterministic merge of all rows here for a given operator.';
