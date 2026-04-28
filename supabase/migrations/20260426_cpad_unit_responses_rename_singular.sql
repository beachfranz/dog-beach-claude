-- Rename to singular: cpad_unit_responses → cpad_unit_response
alter table public.cpad_unit_responses rename to cpad_unit_response;

alter index if exists cpad_unit_responses_unit_idx
  rename to cpad_unit_response_unit_idx;
alter index if exists cpad_unit_responses_status_idx
  rename to cpad_unit_response_status_idx;
alter index if exists cpad_unit_responses_url_idx
  rename to cpad_unit_response_url_idx;
alter index if exists cpad_unit_responses_scope_idx
  rename to cpad_unit_response_scope_idx;
alter index if exists cpad_unit_responses_unique
  rename to cpad_unit_response_unique;

-- Recreate the view against the renamed table.
drop view if exists public.cpad_unit_response_current;
create or replace view public.cpad_unit_response_current as
  select distinct on (cpad_unit_id, response_scope)
    cpad_unit_id, response_scope, source_url,
    response_value, response_reason, response_confidence,
    extracted_at
  from public.cpad_unit_response
  where fetch_status = 'success' and response_value is not null
  order by cpad_unit_id, response_scope,
           response_confidence desc nulls last, extracted_at desc;
