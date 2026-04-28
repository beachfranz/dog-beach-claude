-- Allow fetch_status='manual' on cpad_unit_response so admin overrides
-- (agency-wide policy assertions, hand-corrected entries) can co-exist
-- with web-extracted rows. The cpad_unit_response_current view picks
-- the highest-confidence row, so a manual row at confidence=1.0
-- supersedes anything else for that (unit, scope).

alter table public.cpad_unit_response
  drop constraint if exists cpad_unit_response_fetch_status_check;
alter table public.cpad_unit_response
  drop constraint if exists cpad_unit_dog_extractions_fetch_status_check;

alter table public.cpad_unit_response
  add constraint cpad_unit_response_fetch_status_check
  check (fetch_status in
    ('success','fetch_failed','no_keywords','llm_error','no_data','manual'));
