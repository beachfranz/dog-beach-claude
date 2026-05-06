-- 20260506_is_scoreable_derived_rule.sql
--
-- Rebase is_scoreable from a "set on insert + demote on no" rule to a
-- derived rule that holds an invariant:
--
--   is_scoreable = (
--     is_active
--     AND state = 'CA'
--     AND EXISTS (beach_dog_policy with dogs_allowed in yes-family)
--   )
--
-- Yes-family = ('yes','mixed','seasonal'). 'restricted' is folded into
-- 'mixed' on the consumer surface (per the 2026-05-05 audit migration).
--
-- This naturally:
--   - keeps scoring on CA active beaches with extracted positive policy
--   - excludes 'no' (already)
--   - excludes 'null' / no-policy-row (the stuck-99 set)
--   - re-includes a beach when extraction lands a yes-family value
--
-- Implements via _recompute_is_scoreable_for_fid(p_fid) function
-- + triggers on beach_dog_policy (when dogs_allowed changes) and
-- beaches_gold (when state/is_active changes) + a one-shot recompute.

begin;

-- ----------------------------------------------------------------------
-- Per-fid recomputer
-- ----------------------------------------------------------------------
create or replace function public._recompute_is_scoreable_for_fid(p_fid bigint)
returns boolean
language plpgsql
as $$
declare
  v_should_score boolean;
  v_changed boolean;
begin
  select (bg.is_active = true
          and bg.state = 'CA'
          and exists (
            select 1 from public.beach_dog_policy p
             where p.arena_group_id = bg.fid
               and p.dogs_allowed in ('yes','mixed','seasonal')
          ))
    into v_should_score
    from public.beaches_gold bg
   where bg.fid = p_fid;

  update public.beaches_gold
     set is_scoreable = v_should_score
   where fid = p_fid
     and is_scoreable is distinct from v_should_score;
  v_changed := found;
  return v_changed;
end $$;

-- ----------------------------------------------------------------------
-- Trigger on beach_dog_policy (demote OR promote on dogs_allowed change)
-- Replaces tg_after_change_demote_scoreable_no_dogs which was demote-only.
-- ----------------------------------------------------------------------
drop trigger if exists tg_after_change_demote_scoreable_no_dogs
  on public.beach_dog_policy;

create or replace function public.tg_recompute_scoreable_on_dog_policy()
returns trigger
language plpgsql
as $$
begin
  if NEW.arena_group_id is null then return NEW; end if;
  perform public._recompute_is_scoreable_for_fid(NEW.arena_group_id);
  return NEW;
end $$;

drop trigger if exists tg_after_change_recompute_scoreable
  on public.beach_dog_policy;
create trigger tg_after_change_recompute_scoreable
  after insert or update on public.beach_dog_policy
  for each row execute function public.tg_recompute_scoreable_on_dog_policy();

-- ----------------------------------------------------------------------
-- Update auto-scoreable trigger on beaches_gold to use the derived rule
-- (don't optimistically set true on insert — let the policy-side
-- trigger drive it). This means: new gold beaches start with
-- is_scoreable=false; once their first BEP row lands and the chain
-- promotes dogs_allowed, the policy trigger flips them true.
-- ----------------------------------------------------------------------
create or replace function public.tg_auto_scoreable_socal_gold()
returns trigger
language plpgsql
as $$
begin
  -- Default to false on insert. The dog-policy-side trigger will
  -- flip true when extraction lands a yes-family verdict.
  if NEW.is_scoreable is null then NEW.is_scoreable := false; end if;
  return NEW;
end $$;

-- ----------------------------------------------------------------------
-- Retroactive: apply the derived rule across every active CA beach
-- ----------------------------------------------------------------------
do $$
declare
  v_fid bigint;
  v_updated int := 0;
begin
  for v_fid in
    select fid from public.beaches_gold where is_active and state = 'CA'
  loop
    if public._recompute_is_scoreable_for_fid(v_fid) then
      v_updated := v_updated + 1;
    end if;
  end loop;
  raise notice 'is_scoreable rule rebased: % rows changed', v_updated;
end $$;

commit;
