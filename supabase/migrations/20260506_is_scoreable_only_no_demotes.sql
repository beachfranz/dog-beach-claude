-- 20260506_is_scoreable_only_no_demotes.sql
--
-- Refine the is_scoreable rule per Franz: only an explicit
-- dogs_allowed='no' should block scoring. Beaches with null /
-- yes-family / no policy row at all should still flow through
-- phases 01-05 (extraction, policy, zone_rules) AND get scored.
--
-- The earlier rule (yes-family-only) was over-strict: it demoted
-- ~73 beaches that have evidence but no extracted dogs_allowed yet,
-- and never-extracted beaches that we still want to know about.
--
-- New rule:
--   is_scoreable = (
--     is_active
--     AND state in ('CA','OR')
--     AND NOT EXISTS (beach_dog_policy with dogs_allowed='no')
--   )

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
          and not exists (
            select 1 from public.beach_dog_policy p
             where p.arena_group_id = bg.fid
               and p.dogs_allowed = 'no'
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

-- BEFORE INSERT trigger: now defaults to true (since the demote
-- only fires on explicit 'no'). Goes back to the original behavior.
create or replace function public.tg_auto_scoreable_socal_gold()
returns trigger
language plpgsql
as $$
begin
  if NEW.is_active is true
     and NEW.state in ('CA','OR')
     and (NEW.is_scoreable is null or NEW.is_scoreable = false)
  then
    NEW.is_scoreable := true;
  end if;
  return NEW;
end $$;

-- Retroactive: re-evaluate every active CA/OR beach. Promotes the
-- ~73 that were demoted under the over-strict yes-family rule.
do $$
declare
  v_fid bigint;
  v_promoted int := 0;
begin
  for v_fid in
    select fid from public.beaches_gold
     where is_active and state in ('CA','OR')
  loop
    if public._recompute_is_scoreable_for_fid(v_fid) then
      v_promoted := v_promoted + 1;
    end if;
  end loop;
  raise notice 'is_scoreable rule loosened: % rows changed', v_promoted;
end $$;

commit;
