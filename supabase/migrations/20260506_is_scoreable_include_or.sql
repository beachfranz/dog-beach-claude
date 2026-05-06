-- 20260506_is_scoreable_include_or.sql
--
-- Patch: today's _recompute_is_scoreable_for_fid hard-coded state='CA'
-- and excluded the 5 OR arena seeds from the scoreable set. Those seeds
-- were intentional per CLAUDE.md; reverting them was an unintended
-- side effect.
--
-- Fix: state in ('CA','OR'). Add more states here when scoring scope
-- expands.

begin;

create or replace function public._recompute_is_scoreable_for_fid(p_fid bigint)
returns boolean
language plpgsql
as $$
declare
  v_should_score boolean;
  v_changed boolean;
begin
  select (bg.is_active = true
          and bg.state in ('CA','OR')
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

-- Retroactive: re-run the recomputer for active OR beaches so the seeds land correctly
do $$
declare
  v_fid bigint;
  v_updated int := 0;
begin
  for v_fid in
    select fid from public.beaches_gold where is_active and state = 'OR'
  loop
    if public._recompute_is_scoreable_for_fid(v_fid) then
      v_updated := v_updated + 1;
    end if;
  end loop;
  raise notice 'OR-seed recompute: % rows changed', v_updated;
end $$;

commit;
