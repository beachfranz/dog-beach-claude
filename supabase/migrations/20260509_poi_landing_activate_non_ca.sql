-- 20260509_poi_landing_activate_non_ca.sql
--
-- Issue #23: poi_landing rows for any state other than CA were
-- marked is_active=false with inactive_reason='not_ca_county'. The
-- soft-delete was a one-time CA-scope filter that's now stale —
-- the project canon now covers 27 priority-1 states, not just CA.
--
-- Symptom: DE canon's promote_poi_landing_to_arena() promoted 0 of
-- 49 DE POIs because all were is_active=false. Same pattern would
-- affect MA (685), FL (708), HI (538), NY (469), NJ (269), MD (137),
-- TX (149), and every other Atlantic/Gulf/Great-Lakes state launch.
--
-- Fix: flip is_active=true for all priority-1 state POIs where the
-- only inactive reason is the CA-county filter.

begin;

with flipped as (
  update public.poi_landing
     set is_active = true,
         inactive_reason = null
   where state in (
     'CA','OR','WA','RI','TX','FL','NY','NJ','MA','MD','VA','NC','SC',
     'GA','AL','MS','LA','ME','NH','CT','DE','HI','AK','MI','IL','MN','OH'
   )
     and is_active = false
     and inactive_reason = 'not_ca_county'
  returning state
)
select state, count(*) flipped from flipped group by 1 order by count(*) desc;

commit;
