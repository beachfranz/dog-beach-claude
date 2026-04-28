-- Backfill operators.website from CPAD's agncy_web column. CPAD ships
-- a per-unit operator website URL on each polygon; collapsing by
-- mng_agncy gives operator-scope URLs that previous Tavily/curation
-- work has already validated.
--
-- Coverage: 1,076 of 1,237 operators (87%). The remaining 161 are
-- federal/state seed entries + small private/HOA operators that CPAD
-- doesn't track at the agncy_web level. Manual or Tavily-discovery
-- fills the long tail.
--
-- This URL is the OPERATOR HOMEPAGE. The dog-specific policy page
-- (dog_policy_url) is downstream — discovered via site-search on
-- operators.website + cached for re-use.

update public.operators op
set website = sub.agncy_web
from (
  select cu.mng_agncy, max(cu.agncy_web) as agncy_web
  from public.cpad_units cu
  where cu.agncy_web is not null and cu.agncy_web <> ''
  group by cu.mng_agncy
) sub
where op.cpad_agncy_name = sub.mng_agncy
  and (op.website is null or op.website = '');
