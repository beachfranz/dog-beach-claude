-- 20260514_md_state_seed_fix_off_leash.sql
--
-- Fix MD state_dogs_policy seed: has_off_leash was set true based on
-- DNR's "off-leash under voice control in designated swimming areas
-- or hunting" language, but that's a PARK-LEVEL override, not the
-- STATEWIDE default. The state default is on-leash everywhere.
--
-- Observed downstream impact: MD launch produced leash_policy=off_leash
-- for 120 of 136 scoreable beaches, which is wrong. Beach-level reality
-- is on_leash for the vast majority; designated off-leash zones surface
-- as operator-level rows.
--
-- Aligns the encoding with MI's seed, which uses has_off_leash=false
-- for the same designated-zone-exists-but-not-default situation.

begin;

update public.state_dogs_policy
   set has_off_leash = false,
       scope_notes = 'Maryland DNR Public Lands pet policy: leashed by default statewide. Designated swimming areas + permitted hunting allow off-leash under voice control — these are operator-level overrides, NOT statewide default. Pets prohibited from park buildings + playgrounds. County/municipal beaches handled by lower-level operator policy.',
       scraped_at = now()
 where state_code = 'MD';

commit;
