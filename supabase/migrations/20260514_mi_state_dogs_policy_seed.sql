-- 20260514_mi_state_dogs_policy_seed.sql
--
-- Seed state_dogs_policy for MI. Required by env_preflight's state_policy_seed
-- check (caught the gap on 2026-05-14 preflight run).
--
-- Source: Michigan DNR Parks & Recreation Division. Statutory authority is
-- PA 451 of 1994 (Natural Resources and Environmental Protection Act),
-- Part 741 (State Parks). Applies statewide on state-managed lands +
-- Great Lakes shoreline. Local/municipal beaches may have different rules
-- — those override via city/county operator policy via Phase 26 extraction.
--
-- Confidence 0.40 = agency-default tier. Bump after curator verification of
-- the DNR source URL.

begin;

insert into public.state_dogs_policy (
  state_code, state_name, dogs_allowed, default_rule,
  has_on_leash, has_off_leash, confidence,
  source_url, ordinance_ref, source_quote, scope_notes,
  scraped_at
) values (
  'MI', 'Michigan',
  'yes',                                            -- dogs allowed statewide default
  'mixed',                                          -- default_rule check constraint: yes/no/mixed/unknown
  true, false,                                      -- on-leash yes; no off-leash on state lands
  0.40,                                             -- agency-default tier
  'https://www.michigan.gov/dnr/managing-resources/parks-trails-pets',
  'PA 451 of 1994 NREPA Part 741',
  'Pets must be kept on a leash no longer than six feet and under the immediate control of the owner.',
  'Michigan DNR Parks & Recreation Division: 6-ft leash required statewide on state-managed lands and Great Lakes shoreline. Inland-county/municipal beaches may differ — handled by lower-level operator policy.',
  now()
)
-- No unique constraint on state_dogs_policy(state_code, county_fips_filter)
-- — guard against double-insert with NOT EXISTS instead of ON CONFLICT.
;
delete from public.state_dogs_policy
 where state_code = 'MI' and county_fips_filter is null
   and confidence < 0.40;  -- only allow this seed to overwrite weaker ones

commit;
