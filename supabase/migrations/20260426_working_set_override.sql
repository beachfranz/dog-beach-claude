-- Add a tri-state manual override on top of the generated is_working_set
-- column. Lets admins force-include (e.g., Fort Funston, whose CCC name
-- lacks "beach") or force-exclude points that the auto rule mis-classifies.
--
--   is_working_set_override = true   -> always in
--   is_working_set_override = false  -> always out
--   is_working_set_override = null   -> use auto rule
--
-- PostgreSQL doesn't support ALTER on a stored generated expression,
-- so we drop + recreate. Partial index goes with it; recreated below.

drop index if exists ccc_access_points_working_set_idx;
alter table public.ccc_access_points drop column if exists is_working_set;

alter table public.ccc_access_points
  add column if not exists is_working_set_override boolean;

alter table public.ccc_access_points
  add column is_working_set boolean
    generated always as (
      coalesce(
        is_working_set_override,
        (   sandy_beach    = 'Yes'
        and open_to_public = 'Yes'
        and lower(name) like '%beach%'
        and dogs_verdict   = 'yes'
        and (archived is null or archived <> 'Yes'))
      )
    ) stored;

create index if not exists ccc_access_points_working_set_idx
  on public.ccc_access_points (is_working_set)
  where is_working_set = true;
