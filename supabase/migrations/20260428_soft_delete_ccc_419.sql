-- Soft-delete CCC objectid 419 — "San Francisco Maritime National
-- Historical Park Visitor Center". Surfaced during the Pass 7+8
-- regression spot-check: it had verdict 'no' under the ccc-using
-- cascade (only ccc_native='No' was carrying it), then flipped to
-- 'yes' under Pass 7 because NPS operator_default (restricted=yes)
-- became the only signal. The right fix isn't to re-add CCC — it's
-- to remove this row from the universe entirely. A visitor center is
-- not a beach.
--
-- admin_inactive=true is the standard soft-delete; the recompute
-- walker (recompute_all_dogs_verdicts_by_origin) filters on
-- admin_inactive=false, so this row will not regenerate.

begin;

update public.ccc_access_points
   set admin_inactive = true
 where objectid = 419;

delete from public.beach_verdicts where origin_key = 'ccc/419';

commit;
