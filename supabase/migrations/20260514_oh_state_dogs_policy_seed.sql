-- 20260514_oh_state_dogs_policy_seed.sql
--
-- Seeds Ohio state_dogs_policy from Ohio Administrative Code 1501:31-23-04
-- (Pets in state parks), the codified pet rule under Ohio DNR authority.
--
-- Policy summary (from OAC 1501:31-23-04):
--   * All pets must be on a leash not greater than six feet in length and
--     under the control of an attendant.
--   * Pets prohibited on designated swimming beaches and within 10 feet
--     of swimming areas, except at parks that have designated dog-beach
--     zones (e.g., Geneva State Park).
--   * Pets prohibited inside park buildings.
--   * Each state park may publish additional pet rules.
--
-- We code this as dogs_allowed=yes, default_rule=mixed (leash by default,
-- designated dog beaches exist at a handful of parks), has_on_leash=true,
-- has_off_leash=false (designated dog-beach areas are park-level operator
-- overrides, not statewide default).
--
-- Ordinance reference: OAC 1501:31-23-04. Citation:
-- https://codes.ohio.gov/ohio-administrative-code/rule-1501:31-23-04
-- (Curator: replace with a friendlier ODNR landing page once verified —
-- ODNR's pets-in-state-parks portal page is currently rejecting our
-- curl, so this seed uses the codified rule directly.)

begin;

delete from public.state_dogs_policy where state_code = 'OH';

insert into public.state_dogs_policy
  (state_code, state_name, dogs_allowed, default_rule,
   has_on_leash, has_off_leash,
   source_quote, source_url, ordinance_ref,
   scope_notes, confidence, scraped_at)
values
  ('OH', 'Ohio', 'yes', 'mixed',
   true, false,
   'All pets shall be on a leash not greater than six feet in length and under the control of an attendant. Pets shall be prohibited from designated swimming areas.',
   'https://codes.ohio.gov/ohio-administrative-code/rule-1501:31-23-04',
   'OAC 1501:31-23-04',
   'Ohio DNR state parks: 6-ft leash required statewide; pets prohibited on designated swimming beaches and within 10 feet of swim areas. A few parks (e.g., Geneva State Park) operate designated dog-beach zones — those surface as operator-level overrides, not statewide default. Lake Erie county/municipal beaches set their own rules.',
   0.40,
   now());

commit;
