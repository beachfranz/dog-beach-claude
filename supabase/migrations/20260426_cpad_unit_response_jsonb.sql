-- Add response_value_jsonb to cpad_unit_response so scopes that need
-- richer-than-text answers (leash_policy with zones + time windows;
-- future: parking zones, fee structure) can carry structured data
-- alongside the human-readable response_value text.

alter table public.cpad_unit_response
  add column if not exists response_value_jsonb jsonb;

create index if not exists cpad_unit_response_jsonb_idx
  on public.cpad_unit_response using gin (response_value_jsonb);
