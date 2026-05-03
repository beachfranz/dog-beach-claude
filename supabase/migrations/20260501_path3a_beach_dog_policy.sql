-- Path 3a step 6: create beach_dog_policy overlay table.
--
-- Per path-3 decision 2=b: beaches_gold stays identity-only; curated
-- dog policy lives in this overlay, keyed on arena_group_id.
--
-- Source data: 7 hand-curated columns from public.beaches. Copy into
-- the overlay for the 8 active CA beaches that have arena_group_id.
-- (The 5 OR beaches don't have curated dog policy yet — left out.)
--
-- This data is duplicated during 3a (lives in both public.beaches and
-- beach_dog_policy). 3b drops the public.beaches columns. Edge functions
-- in step 7 read from this overlay.

begin;

create table if not exists public.beach_dog_policy (
  arena_group_id        bigint primary key references public.beaches_gold(fid) on delete restrict,
  dogs_allowed          text,
  leash_policy          text,
  off_leash_flag        boolean,
  dogs_prohibited_start text,
  dogs_prohibited_end   text,
  dogs_allowed_areas    text,
  access_rule           text,
  source                text not null default 'public.beaches',
  curated_at            timestamptz not null default now(),
  notes                 text
);

create index if not exists beach_dog_policy_dogs_allowed_idx
  on public.beach_dog_policy (dogs_allowed);

comment on table public.beach_dog_policy is
  'Hand-curated dog-access policy per beach, keyed on arena_group_id. Overlay on beaches_gold (identity-only). Currently sourced from public.beaches; future curation will write here directly.';

-- Backfill from public.beaches for arena-linked rows
insert into public.beach_dog_policy
  (arena_group_id, dogs_allowed, leash_policy, off_leash_flag,
   dogs_prohibited_start, dogs_prohibited_end, dogs_allowed_areas,
   access_rule, source, notes)
select b.arena_group_id, b.dogs_allowed, b.leash_policy, b.off_leash_flag,
       b.dogs_prohibited_start, b.dogs_prohibited_end, b.dogs_allowed_areas,
       b.access_rule,
       'public.beaches', 'backfill from public.beaches 2026-05-01'
  from public.beaches b
 where b.arena_group_id is not null
   and (b.dogs_allowed is not null
        or b.leash_policy is not null
        or b.off_leash_flag is not null
        or b.dogs_prohibited_start is not null
        or b.dogs_prohibited_end is not null
        or b.dogs_allowed_areas is not null
        or b.access_rule is not null)
on conflict (arena_group_id) do nothing;

commit;
