-- Path 3b-3: scoreable gate on beaches_gold.
--
-- daily-beach-refresh is about to swap from iterating public.beaches
-- to iterating beaches_gold. Without a gate, that fan-out goes from
-- 15 to 763. We don't want to score every beach in the catalog —
-- only those Franz has flagged worth scoring.
--
-- This column is the gate. Defaults false. Backfilled to true for
-- every beach currently in public.beaches as is_active (= the 15
-- beaches the nightly job already scores). seed_arena_beach.py
-- --score sets it true going forward.

begin;

alter table public.beaches_gold
  add column if not exists is_scoreable boolean not null default false;

create index if not exists beaches_gold_is_scoreable_idx
  on public.beaches_gold (is_scoreable) where is_scoreable = true;

comment on column public.beaches_gold.is_scoreable is
  'True → daily-beach-refresh covers this beach. Defaults false; set by seed_arena_beach.py --score or by manual curation. Without this gate, fan-out would explode to 763 beaches.';

-- Backfill: every beach currently scored (i.e., has a public.beaches
-- row with is_active=true and arena_group_id set) gets is_scoreable=true.
update public.beaches_gold g
   set is_scoreable = true
  from public.beaches b
 where b.arena_group_id = g.fid
   and b.is_active = true;

commit;

-- Verify (informational):
-- SELECT count(*) FROM public.beaches_gold WHERE is_scoreable = true;
-- expect: 15 (14 originals + Mission Beach + Fiesta Island, minus 1 inactive sunset)
