-- 20260506_no_dogs_no_scoring.sql
--
-- Don't score beaches where dogs aren't allowed. Saves the daily
-- scoring run from spending compute + storage on ~100 SoCal beaches
-- where dogs_allowed='no'. Yes-family stays scoreable: 'yes',
-- 'mixed' (areas allowed/prohibited within one beach), 'seasonal'
-- (depends on date). Those are still product-relevant.
--
-- Rule semantics: DEMOTE-ONLY.
--   dogs_allowed='no'      → is_scoreable=false (demote)
--   dogs_allowed in yes-fam → no action (don't auto-promote, since
--                              geography filters still apply for
--                              OR/NorCal beaches outside scoring scope)
--   dogs_allowed is null    → no action (we don't know yet)
--
-- This is asymmetric on purpose: promotion to is_scoreable=true
-- happens only via tg_before_insert_auto_scoreable_socal (geography
-- gate) or curator action.

begin;

create or replace function public.tg_demote_scoreable_when_no_dogs()
returns trigger
language plpgsql
as $$
begin
  if NEW.dogs_allowed = 'no' then
    update public.beaches_gold
       set is_scoreable = false
     where fid = NEW.arena_group_id
       and is_scoreable = true;
  end if;
  return NEW;
end $$;

drop trigger if exists tg_after_change_demote_scoreable_no_dogs
  on public.beach_dog_policy;

create trigger tg_after_change_demote_scoreable_no_dogs
  after insert or update on public.beach_dog_policy
  for each row
  when (NEW.dogs_allowed = 'no' and NEW.arena_group_id is not null)
  execute function public.tg_demote_scoreable_when_no_dogs();

-- Retroactive: demote the 106 currently-scoreable beaches with dogs_allowed='no'
update public.beaches_gold bg
   set is_scoreable = false
  from public.beach_dog_policy p
 where p.arena_group_id = bg.fid
   and p.dogs_allowed = 'no'
   and bg.is_scoreable = true;

commit;
