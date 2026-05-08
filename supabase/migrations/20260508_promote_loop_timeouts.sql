-- 20260508_promote_loop_timeouts.sql
--
-- promote_to_gold loops over each fid calling 10 populate_from_X +
-- 5 _resolve_X functions inline. WA's first promote pass has ~500 fids
-- (~158 OSM + ~388 POI) so the loop totals 7,500+ inner calls. Each
-- populator's default 8s statement_timeout is fine per-call, but
-- aggregate exceeds the outer function's timeout. Bump them all to 600s.
--
-- Real fix: batch the populators to take fids[] instead of one fid.
-- Tonight's bandaid: just give the loop more headroom.

begin;

alter function public.populate_from_pad_us_gold(bigint)            set statement_timeout = '600s';
alter function public.populate_from_cpad_gold(bigint)              set statement_timeout = '600s';
alter function public.populate_from_park_operators_gold(bigint)    set statement_timeout = '600s';
alter function public.populate_from_research_gold(bigint)          set statement_timeout = '600s';
alter function public.populate_from_park_url_gold(bigint)          set statement_timeout = '600s';
alter function public.populate_from_park_url_governance_gold(bigint) set statement_timeout = '600s';
alter function public.populate_from_unified_v1_gold(bigint)        set statement_timeout = '600s';
alter function public.populate_from_city_dog_policy_gold(bigint)   set statement_timeout = '600s';
alter function public.populate_from_county_dog_policy_gold(bigint) set statement_timeout = '600s';
alter function public.populate_polygon_containment_gold(bigint)    set statement_timeout = '600s';

alter function public.promote_to_gold(bigint[], boolean, boolean)  set statement_timeout = '900s';
alter function public.run_pipeline_for_state(text, bigint[])       set statement_timeout = '900s';

commit;
