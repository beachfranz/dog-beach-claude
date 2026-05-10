-- 20260509_zone_rules_refire_on_dogs_change.sql
--
-- Fixes the trigger-ordering bug that left 499 beaches with NULL
-- zone_rules across MA/DE/CA/RI (Franz, 2026-05-09):
--
-- Chain that exposed the bug:
--   1. promote_to_gold inserts row → tg_after_insert_gold_promote_chain
--      runs _promote_zone_rules_for_fid → _zr_inject_sand_from_policy
--      reads beach_dog_policy.dogs_allowed → NULL (BEP not extracted
--      yet) → returns early → zone_rules stays NULL.
--   2. Later phases (operator_llm_extract, operator_merge, bep_refire)
--      populate BEP and call promote_canonical_dogs_to_beach_dog_policy,
--      which writes dogs_allowed/has_on_leash/has_off_leash on
--      beach_dog_policy.
--   3. The existing tg_after_change_promote_zone_rules trigger ONLY
--      fires for source='text_repass_v1' BEP rows that already carry a
--      zone_rules JSON — so the dogs-allowed transition above doesn't
--      cascade back into a zone_rules re-fire. zone_rules stays NULL.
--
-- This migration adds tg_after_change_dogs_refire_zone_rules: when any
-- of the dogs* columns on beach_dog_policy change from NULL → not-null
-- (or when the dogs decision changes), re-fire
-- _promote_zone_rules_for_fid for that fid. The fid then picks up the
-- sand baseline from _zr_inject_sand_from_policy.
--
-- Idempotent: _promote_zone_rules_for_fid is a no-op when zone_rules
-- doesn't change.

begin;

create or replace function public.tg_after_change_dogs_refire_zone_rules()
returns trigger
language plpgsql
as $function$
begin
  if NEW.arena_group_id is null then return NEW; end if;
  -- Only fire when dogs evidence newly arrives or changes — avoids
  -- spurious work on every UPDATE of unrelated columns.
  if TG_OP = 'INSERT'
     or NEW.dogs_allowed   is distinct from OLD.dogs_allowed
     or NEW.has_off_leash  is distinct from OLD.has_off_leash
     or NEW.has_on_leash   is distinct from OLD.has_on_leash then
    perform public._promote_zone_rules_for_fid(NEW.arena_group_id);
  end if;
  return NEW;
end $function$;

drop trigger if exists tg_after_change_dogs_refire_zone_rules
  on public.beach_dog_policy;

create trigger tg_after_change_dogs_refire_zone_rules
  after insert or update on public.beach_dog_policy
  for each row
  execute function public.tg_after_change_dogs_refire_zone_rules();

comment on function public.tg_after_change_dogs_refire_zone_rules() is
  'Re-fires _promote_zone_rules_for_fid when beach_dog_policy dogs* '
  'columns change. Fixes the case where the sand baseline was missed '
  'because dogs_allowed was NULL at the time the gold-insert trigger '
  'first ran.';

commit;
