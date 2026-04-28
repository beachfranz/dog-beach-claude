-- Three derived columns on ccc_access_points holding the weighted-
-- vote consensus across CCC-native, CPAD, and CCC-LLM signals.
--
-- Loader safety: admin-load-ccc / load_ccc_batch use INSERT ON CONFLICT
-- DO UPDATE SET <explicit column list>; these three columns are not in
-- that SET list, so periodic CCC reloads preserve them.

alter table public.ccc_access_points
  add column if not exists dogs_verdict             text,
  add column if not exists dogs_verdict_confidence  numeric,
  add column if not exists dogs_verdict_meta        jsonb;

alter table public.ccc_access_points
  drop constraint if exists ccc_access_points_dogs_verdict_check;
alter table public.ccc_access_points
  add  constraint ccc_access_points_dogs_verdict_check
    check (dogs_verdict is null or dogs_verdict in ('yes','no'));

create index if not exists ccc_access_points_dogs_verdict_idx
  on public.ccc_access_points (dogs_verdict);
