-- 20260510_inactive_clears_scoreable.sql
--
-- Two things:
--
-- 1. Backfill: clear is_scoreable on 366 stale beaches_gold rows where
--    is_active=false but is_scoreable=true. Pre-trigger-fix legacy
--    state from dedup soft-deletes that didn't propagate to
--    is_scoreable. (See 20260509_recompute_scoreable_all_states_tier_based.sql
--    for the trigger fix that made go-forward state correct; this
--    migration handles the pre-fix stragglers.)
--
-- 2. Defensive trigger: on UPDATE that flips is_active to false, set
--    is_scoreable=false in the same row. Holds the invariant
--    "is_scoreable=true requires is_active=true" going forward without
--    relying on a downstream trigger to catch the transition.

begin;

-- 1. Backfill stragglers
update public.beaches_gold
   set is_scoreable = false
 where not is_active and is_scoreable;

-- 2. Defensive trigger
create or replace function public.tg_inactive_clears_scoreable()
returns trigger
language plpgsql
as $function$
begin
  -- Fires on INSERT/UPDATE; clears is_scoreable when is_active becomes false.
  -- Doesn't touch is_scoreable when is_active is true — that's owned by the
  -- existing _recompute_is_scoreable_for_fid pathway driven by beach_dog_policy.
  if NEW.is_active is false then
    NEW.is_scoreable := false;
  end if;
  return NEW;
end $function$;

drop trigger if exists tg_inactive_clears_scoreable on public.beaches_gold;

create trigger tg_inactive_clears_scoreable
  before insert or update on public.beaches_gold
  for each row
  execute function public.tg_inactive_clears_scoreable();

comment on function public.tg_inactive_clears_scoreable() is
  'Invariant guard: when a beaches_gold row is inactive, is_scoreable '
  'must be false. Catches the case where dedup soft-deletes a row '
  'without going through the beach_dog_policy-driven recompute path.';

commit;
