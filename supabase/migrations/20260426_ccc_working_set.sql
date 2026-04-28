-- The Working Set: a CCC sandy public beach with "beach" in the name
-- and a dogs_verdict of 'yes'. Generated column auto-recomputes on
-- every row change (incl. dogs_verdict updates from compute_dogs_verdict),
-- so no separate refresh function is needed.
--
-- Loader safety: same as dogs_verdict columns — load_ccc_batch's
-- explicit-SET upsert doesn't touch this column. Re-evaluation happens
-- automatically when the SET'd columns (sandy_beach, open_to_public,
-- name, archived) change.

alter table public.ccc_access_points
  add column if not exists is_working_set boolean
    generated always as (
          sandy_beach    = 'Yes'
      and open_to_public = 'Yes'
      and lower(name) like '%beach%'
      and dogs_verdict   = 'yes'
      and (archived is null or archived <> 'Yes')
    ) stored;

create index if not exists ccc_access_points_working_set_idx
  on public.ccc_access_points (is_working_set)
  where is_working_set = true;
