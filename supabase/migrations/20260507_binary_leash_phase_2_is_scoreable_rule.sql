-- 20260507_binary_leash_phase_2_is_scoreable_rule.sql
--
-- Update _recompute_is_scoreable_for_fid to use the binary leash schema.
--
-- The legacy rule demoted is_scoreable when dogs_allowed='no'. That made
-- sense when dogs_allowed was the only signal. But under the binary
-- schema, beaches like Doheny SB end up dogs_allowed='no' (the swim
-- beach prohibition wins consensus) while has_on_leash=T (the
-- campground/day-use carve-out is the actual dog access).
--
-- Demoting those incorrectly hides them from the consumer surface
-- entirely. Right rule: demote ONLY when truly no dog access exists,
-- i.e. has_on_leash=F AND has_off_leash=F. Falls back to the legacy
-- dogs_allowed='no' check if the binaries are NULL (beaches not yet
-- migrated through Phase 1B consensus).

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
          and bg.state in ('CA','OR')
          and not exists (
            select 1 from public.beach_dog_policy p
             where p.arena_group_id = bg.fid
               and (
                 -- Binary path: demote only when both leash modes
                 -- explicitly say no dog access exists.
                 (p.has_on_leash is false and p.has_off_leash is false)
                 -- Legacy fallback: when binaries are unset, fall back
                 -- to the categorical dogs_allowed='no' check.
                 or (p.has_on_leash is null and p.has_off_leash is null
                     and p.dogs_allowed = 'no')
               )
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

-- Recompute across the dataset to apply the new rule. Beaches with
-- dogs_allowed='no' but has_on_leash=true (campground carve-outs, etc)
-- will flip back to scoreable.
do $$
declare
  fid_iter bigint;
begin
  for fid_iter in select fid from public.beaches_gold where state in ('CA','OR') and is_active loop
    perform public._recompute_is_scoreable_for_fid(fid_iter);
  end loop;
end $$;

commit;
