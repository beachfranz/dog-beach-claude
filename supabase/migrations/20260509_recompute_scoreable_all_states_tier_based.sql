-- 20260509_recompute_scoreable_all_states_tier_based.sql
--
-- Two bugs in _recompute_is_scoreable_for_fid:
--
-- 1. Hardcoded state allowlist `state in ('CA','OR')`. After launching
--    DE, MA, RI we have 892 MA tier-2 + 4 DE tier-2 beaches stuck
--    not_scoreable because the trigger always returned false for
--    non-CA/OR states. Removing the gate.
--
-- 2. Logic divergence with public.align_is_scoreable_to_tier. The align
--    function (canonical scoring scope = Tier 1 + 2) uses
--    public.beach_location_tier(); the trigger used a different
--    "any-dog-access" rule. These can disagree (e.g. tier-3 beaches
--    that have on_leash access end up scoreable per trigger but
--    not per align).
--
-- Reconciles both: trigger now uses the same tier check as align, with
-- no state gate. Re-fires across every active beach in every state to
-- backfill.
--
-- Symmetric to 20260509_zone_rules_refire_on_dogs_change — both close
-- the trigger-ordering gap that left beaches in stale post-promote
-- state when dogs evidence arrived after the gold-insert chain.

begin;

create or replace function public._recompute_is_scoreable_for_fid(p_fid bigint)
returns boolean
language plpgsql
as $function$
declare
  v_should_score boolean;
  v_changed boolean;
begin
  select (bg.is_active = true
          and exists (
            select 1 from public.beach_dog_policy p
             where p.arena_group_id = bg.fid
               and public.beach_location_tier(
                     p.dogs_allowed, p.has_off_leash, p.has_on_leash,
                     p.dogs_prohibited_start::text)
                   in ('1_off-leash','2_on-leash')
          ))
    into v_should_score
    from public.beaches_gold bg
   where bg.fid = p_fid;

  update public.beaches_gold
     set is_scoreable = v_should_score
   where fid = p_fid
     and is_scoreable is distinct from v_should_score;
  v_changed := found;
  return v_changed;
end
$function$;

comment on function public._recompute_is_scoreable_for_fid(bigint) is
  'Per-fid scoreable recompute. Aligns with align_is_scoreable_to_tier '
  '(scoring scope = Tier 1 + 2 per public.beach_location_tier). '
  'Triggered by tg_after_change_recompute_scoreable on beach_dog_policy '
  'INSERT/UPDATE.';

-- Backfill: re-fire for every active beach across all launched states.
-- Idempotent — only updates when is_scoreable actually changes.
do $$
declare
  fid_iter bigint;
  v_total int := 0;
  v_changed int := 0;
begin
  for fid_iter in
    select fid from public.beaches_gold where is_active
  loop
    v_total := v_total + 1;
    if public._recompute_is_scoreable_for_fid(fid_iter) then
      v_changed := v_changed + 1;
    end if;
  end loop;
  raise notice 'Recomputed % beaches; % flipped is_scoreable.',
    v_total, v_changed;
end $$;

commit;
