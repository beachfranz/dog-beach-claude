-- 20260506_drop_bad_verdict_trigger.sql
--
-- Reverts a bad assumption from earlier today.
-- Migration 20260506_verdict_polygon_triggers.sql added
-- tg_after_change_recompute_verdict on beach_dog_policy that called
-- compute_dogs_verdict_by_origin(NEW.arena_group_id::bigint).
--
-- Reality: compute_dogs_verdict_by_origin takes p_origin_key text,
-- not a fid. So the trigger crashed every time beach_dog_policy was
-- updated -- which is exactly when the dogs-chain trigger writes to
-- it after extraction. Net effect: the 132-fid text-repass run reported
-- $0.71 spent and 0 BEP rows landed because each insert blew up in
-- the trigger cascade.
--
-- Fix: drop the trigger, restore the nightly cron that was working
-- fine. Per CLAUDE.md the verdict cascade is parity/reference only,
-- so daily lag is acceptable.

begin;

drop trigger if exists tg_after_change_recompute_verdict on public.beach_dog_policy;
drop function if exists public.tg_recompute_dog_verdict_for_fid();

-- Restore the nightly cron job (it was unscheduled by the prior migration
-- in favor of the now-removed trigger).
do $cron$
begin
  if not exists (select 1 from cron.job where jobname = 'verdict_cascade_nightly') then
    perform cron.schedule(
      'verdict_cascade_nightly',
      '0 3 * * *',
      $$ select public.recompute_all_dogs_verdicts_by_origin(); $$
    );
  end if;
end $cron$;

commit;
