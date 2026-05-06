-- 20260506_cpad_unit_restricted_to_mixed.sql
--
-- Yesterday's 20260505_dogs_allowed_restricted_to_mixed.sql migrated the
-- vocab `restricted` -> `mixed` at the BEP / consensus / consumer
-- (beach_dog_policy) layers but did NOT touch the source policy tables
-- (cpad_unit_dogs_policy / operator_dogs_policy). Today's audit of the
-- phase-01 bucket showed 14 beaches sit inside CPAD units whose
-- dogs_allowed was still `restricted` -- a value the rest of the
-- pipeline now treats as legacy/invalid.
--
-- This migration brings cpad_unit_dogs_policy in line with the new
-- vocab. (operator_dogs_policy will be done in a follow-up if its
-- restricted rows show up in another audit.)

begin;

alter table public.cpad_unit_dogs_policy
  drop constraint if exists cpad_unit_dogs_policy_dogs_allowed_check,
  drop constraint if exists cpad_unit_dogs_policy_default_rule_check;

update public.cpad_unit_dogs_policy
   set dogs_allowed = 'mixed'
 where dogs_allowed = 'restricted';

update public.cpad_unit_dogs_policy
   set default_rule = 'mixed'
 where default_rule = 'restricted';

alter table public.cpad_unit_dogs_policy
  add constraint cpad_unit_dogs_policy_dogs_allowed_check
    check (dogs_allowed = any (array['yes','no','mixed','seasonal','unknown'])),
  add constraint cpad_unit_dogs_policy_default_rule_check
    check (default_rule = any (array['yes','no','mixed','seasonal','unknown']));

commit;
