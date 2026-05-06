-- 20260506_auto_scoreable_socal_gold.sql
--
-- BEFORE INSERT trigger on beaches_gold: if a new row is inserted for
-- an active SoCal beach (LA/Orange/SD/Santa Barbara/Ventura), auto-flip
-- is_scoreable=true. Closes one manual step in the arena → gold →
-- scoring chain that today required `bulk_promote_socal.py` to fire.
--
-- Conservative scope:
--   - CA only
--   - 5 SoCal counties (where daily-beach-refresh already runs against
--     the existing 304-beach scoreable population)
--   - Only flips when caller didn't explicitly set is_scoreable=true
--   - Doesn't touch existing rows (no UPDATE side)
--
-- For OR beaches, NorCal, and any new state coverage: manual curator
-- decision still applies. This trigger covers the 80% case.

begin;

create or replace function public.tg_auto_scoreable_socal_gold()
returns trigger
language plpgsql
as $$
begin
  if NEW.is_active is true
     and NEW.state = 'CA'
     and NEW.county_name in ('Los Angeles','Orange','San Diego','Santa Barbara','Ventura')
     and (NEW.is_scoreable is null or NEW.is_scoreable = false)
  then
    NEW.is_scoreable := true;
  end if;
  return NEW;
end $$;

drop trigger if exists tg_before_insert_auto_scoreable_socal
  on public.beaches_gold;

create trigger tg_before_insert_auto_scoreable_socal
  before insert on public.beaches_gold
  for each row execute function public.tg_auto_scoreable_socal_gold();

commit;
