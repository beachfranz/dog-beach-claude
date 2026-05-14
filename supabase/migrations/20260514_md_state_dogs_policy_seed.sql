-- 20260514_md_state_dogs_policy_seed.sql
--
-- Seeds Maryland state_dogs_policy row from the Maryland DNR pets page:
-- https://dnr.maryland.gov/publiclands/pages/pets.aspx
--
-- Policy summary:
--   * Pets must be leashed in Maryland State Parks (default rule).
--   * Pets may be off-leash under voice control in DESIGNATED swimming areas
--     or while hunting (with appropriate permit) — so off-leash exists at
--     specific designated zones, but not statewide.
--   * Pets prohibited inside park buildings and on playgrounds.
--   * Each park may set additional rules.
--
-- We code this as dogs_allowed=yes, default_rule=mixed (statewide leash
-- requirement + designated off-leash zones), has_on_leash=true,
-- has_off_leash=true (in designated areas).
--
-- Ordinance reference: COMAR 08.01.07 (Maryland Park Service regulations
-- on pet behavior). Page also cites Maryland DNR Public Lands policy.

begin;

delete from public.state_dogs_policy where state_code = 'MD';

insert into public.state_dogs_policy
  (state_code, state_name, dogs_allowed, default_rule,
   has_on_leash, has_off_leash,
   source_quote, source_url, ordinance_ref,
   scope_notes, confidence, scraped_at)
values
  ('MD', 'Maryland', 'yes', 'mixed',
   true, true,
   'Pets must be leashed. Pets may be off-leash and under voice control while swimming in designated areas or hunting (with the appropriate permit).',
   'https://dnr.maryland.gov/publiclands/pages/pets.aspx',
   'COMAR 08.01.07',
   'Maryland DNR Public Lands pet policy: leashed by default statewide; designated swimming areas + permitted hunting allow off-leash under voice control. Pets prohibited from park buildings + playgrounds. County/municipal beaches handled by lower-level operator policy.',
   0.40,
   now());

commit;
