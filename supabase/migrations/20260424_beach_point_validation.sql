-- Validation flagging on canonical beach-point inventory. Lets the
-- enrichment pipeline skip records that fail one or more data-quality
-- checks, while keeping the failure reasons as first-class data for
-- admin review and auditability.
--
-- Pattern (proposed 2026-04-24):
--   validation_status ∈ {'valid','invalid'} — filter column for pipeline queries
--   validation_flags  — JSONB array; each entry describes a failed check
--   validated_at      — last time any validation pass touched this row
--
-- Flag object shape:
--   {
--     "check":       "state_boundary",             -- machine-readable check ID
--     "expected":    "California",                  -- what we expected
--     "details":     "point is 42km offshore ...", -- human-readable
--     "process":     "script::function",            -- traceability
--     "detected_at": "2026-04-24T18:45:12Z"
--   }
--
-- Multiple flags accumulate (JSONB array append). Removing a flag is
-- an admin action — no automatic clearing.

alter table public.us_beach_points
  add column if not exists validation_status text        not null default 'valid',
  add column if not exists validation_flags  jsonb       not null default '[]'::jsonb,
  add column if not exists validated_at      timestamptz;

alter table public.us_beach_points
  drop constraint if exists us_beach_points_validation_status_chk;
alter table public.us_beach_points
  add  constraint us_beach_points_validation_status_chk
  check (validation_status in ('valid', 'invalid'));

create index if not exists us_beach_points_validation_idx
  on public.us_beach_points (validation_status);

-- Helper: append a flag to a single beach point. Idempotent against
-- the (fid, check) pair — re-flagging the same check replaces the old
-- entry so the flag reflects the most recent validation run.
create or replace function public.flag_beach_point(
  p_fid      int,
  p_check    text,
  p_expected text,
  p_details  text,
  p_process  text
) returns void
language sql
security definer
as $$
  update public.us_beach_points set
    validation_status = 'invalid',
    validation_flags  = (
      select coalesce(jsonb_agg(f), '[]'::jsonb)
      from jsonb_array_elements(validation_flags) as f
      where (f->>'check') is distinct from p_check
    ) || jsonb_build_array(jsonb_build_object(
      'check',       p_check,
      'expected',    p_expected,
      'details',     p_details,
      'process',     p_process,
      'detected_at', now()
    )),
    validated_at = now()
  where fid = p_fid;
$$;

revoke all on function public.flag_beach_point(int, text, text, text, text) from public, anon, authenticated;
grant  execute on function public.flag_beach_point(int, text, text, text, text) to service_role;
